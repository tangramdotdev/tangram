use {
	super::{
		Index, Key, KeyKind, ObjectField, ObjectMetadataField, ObjectPropagateUpdate,
		ObjectPropagateUpdateFields, ObjectStoredField, ProcessField, ProcessMetadataField,
		ProcessPropagateUpdate, ProcessPropagateUpdateFields, ProcessStoredField, PropagateUpdate,
		Update,
	},
	crate::ProcessObjectKind,
	foundationdb as fdb,
	foundationdb_tuple::{self as fdbt, Subspace},
	futures::{future, try_join},
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_util::varint,
};

impl Index {
	pub async fn updates_finished(&self, transaction_id: u64) -> tg::Result<bool> {
		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create the transaction"))?;

		let mut bytes = [0u8; 10];
		bytes[..8].copy_from_slice(&transaction_id.to_be_bytes());
		bytes[8..].copy_from_slice(&0xFFFFu16.to_be_bytes());
		let versionstamp = fdbt::Versionstamp::complete(bytes, 0);

		let key_kind = KeyKind::UpdateVersion.to_i32().unwrap();
		let futures = (0..self.partition_total).map(|partition| {
			let begin = Self::pack(&self.subspace, &(key_kind, partition));
			let end = Self::pack(&self.subspace, &(key_kind, partition, versionstamp.clone()));
			let range = fdb::RangeOption {
				begin: fdb::KeySelector::first_greater_or_equal(begin),
				end: fdb::KeySelector::first_greater_or_equal(end),
				limit: Some(1),
				mode: fdb::options::StreamingMode::WantAll,
				..Default::default()
			};
			txn.get_range(&range, 1, false)
		});
		let results = future::try_join_all(futures)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if updates are finished"))?;
		let finished = results.iter().all(|entries| entries.is_empty());

		Ok(finished)
	}

	pub async fn update_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<usize> {
		let partition_total = self.partition_total;
		let result = self
			.database
			.run(|txn, _| {
				let subspace = self.subspace.clone();
				async move {
					Self::update_batch_inner(
						&txn,
						&subspace,
						batch_size,
						partition_start,
						partition_count,
						partition_total,
					)
					.await
					.map_err(|source| fdb::FdbBindingError::CustomError(source.into()))
				}
			})
			.await;

		let count = match result {
			Ok(count) => count,
			Err(fdb::FdbBindingError::NonRetryableFdbError(error))
				if error.code() == 2101 && batch_size > 1 =>
			{
				let half = batch_size / 2;
				let first =
					Box::pin(self.update_batch(half, partition_start, partition_count)).await?;
				let second =
					Box::pin(self.update_batch(half, partition_start, partition_count)).await?;
				first + second
			},
			Err(error) => {
				return Err(tg::error!(!error, "failed to process update batch"));
			},
		};

		Ok(count)
	}

	async fn update_batch_inner(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
		partition_total: u64,
	) -> tg::Result<usize> {
		let mut entries = Vec::new();

		let key_kind = KeyKind::UpdateVersion.to_i32().unwrap();
		let partition_end = partition_start.saturating_add(partition_count);
		for partition in partition_start..partition_end {
			let remaining = batch_size.saturating_sub(entries.len());
			if remaining == 0 {
				break;
			}
			let begin = Self::pack(subspace, &(key_kind, partition));
			let end = Self::pack(subspace, &(key_kind, partition + 1));
			let range = fdb::RangeOption {
				begin: fdb::KeySelector::first_greater_or_equal(begin),
				end: fdb::KeySelector::first_greater_or_equal(end),
				limit: Some(remaining),
				mode: fdb::options::StreamingMode::WantAll,
				..Default::default()
			};
			let partition_entries = txn
				.get_range(&range, 1, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get update version range"))?;
			for entry in partition_entries {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::UpdateVersion {
					partition,
					version,
					id,
				} = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				entries.push((partition, version, id));
			}
		}

		let mut count = 0;
		for (partition, version, id) in entries {
			let key = Self::pack(subspace, &Key::Update { id: id.clone() });
			let value = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get update key"))?;

			let Some(update) = value else {
				let key = Self::pack(
					subspace,
					&Key::UpdateVersion {
						partition,
						version: version.clone(),
						id: id.clone(),
					},
				);
				txn.clear(&key);
				count += 1;
				continue;
			};

			let update = Update::deserialize(&update)?;

			let fields = match (&id, &update) {
				(tg::Either::Left(id), Update::Propagate(PropagateUpdate::Object(update))) => {
					let fields = ObjectPropagateUpdateFields::from_bits_truncate(update.fields);
					let fields = Self::update_object(txn, subspace, id, fields).await?;
					tg::Either::Left(fields)
				},
				(tg::Either::Left(id), _) => {
					let fields = ObjectPropagateUpdateFields::ALL;
					let fields = Self::update_object(txn, subspace, id, fields).await?;
					tg::Either::Left(fields)
				},
				(tg::Either::Right(id), Update::Propagate(PropagateUpdate::Process(update))) => {
					let fields = ProcessPropagateUpdateFields::from_bits_truncate(update.fields);
					let fields = Self::update_process(txn, subspace, id, fields).await?;
					tg::Either::Right(fields)
				},
				(tg::Either::Right(id), _) => {
					let fields = ProcessPropagateUpdateFields::ALL;
					let fields = Self::update_process(txn, subspace, id, fields).await?;
					tg::Either::Right(fields)
				},
			};

			if match update {
				Update::Put => true,
				Update::Propagate(_) => false,
			} {
				match &id {
					tg::Either::Left(id) => {
						let fields = ObjectPropagateUpdateFields::ALL;
						Self::enqueue_object_parents(
							txn,
							subspace,
							id,
							fields,
							&version,
							partition_total,
						)
						.await?;
					},
					tg::Either::Right(id) => {
						let fields = ProcessPropagateUpdateFields::ALL;
						Self::enqueue_process_parents(
							txn,
							subspace,
							id,
							fields,
							&version,
							partition_total,
						)
						.await?;
					},
				}
			} else {
				match (&id, &fields) {
					(tg::Either::Left(id), tg::Either::Left(fields)) if !fields.is_empty() => {
						Self::enqueue_object_parents(
							txn,
							subspace,
							id,
							*fields,
							&version,
							partition_total,
						)
						.await?;
					},
					(tg::Either::Right(id), tg::Either::Right(fields)) if !fields.is_empty() => {
						Self::enqueue_process_parents(
							txn,
							subspace,
							id,
							*fields,
							&version,
							partition_total,
						)
						.await?;
					},
					_ => {},
				}
			}

			let key = Self::pack(subspace, &Key::Update { id: id.clone() });
			txn.clear(&key);
			let key = Self::pack(
				subspace,
				&Key::UpdateVersion {
					partition,
					version: version.clone(),
					id: id.clone(),
				},
			);
			txn.clear(&key);

			count += 1;
		}

		Ok(count)
	}

	async fn update_object(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		fields: ObjectPropagateUpdateFields,
	) -> tg::Result<ObjectPropagateUpdateFields> {
		let children = Self::get_object_children_with_transaction(txn, subspace, id).await?;

		let stored_subtree_future = async {
			let mut updated = ObjectPropagateUpdateFields::empty();
			if fields.contains(ObjectPropagateUpdateFields::STORED_SUBTREE) {
				let field = ObjectStoredField::Subtree;
				let current = Self::update_get_object_field_bool(txn, subspace, id, field).await?;
				if current != Some(true) {
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_object_field_bool(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.all(|child| child.as_ref().is_some_and(|value| *value));
					if value {
						Self::update_set_object_field_bool(txn, subspace, id, field, true);
						updated |= ObjectPropagateUpdateFields::STORED_SUBTREE;
					}
				}
			}
			Ok::<_, tg::Error>(updated)
		};

		let metadata_subtree_count_future = async {
			let mut updated = ObjectPropagateUpdateFields::empty();
			if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_COUNT) {
				let field = ObjectMetadataField::SubtreeCount;
				let current = Self::update_get_object_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_object_field_u64(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values.iter().copied().sum::<Option<u64>>();
					if let Some(value) = value {
						let value = 1 + value;
						let field = ObjectMetadataField::SubtreeCount;
						Self::update_set_object_field_u64(txn, subspace, id, field, value);
						updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_COUNT;
					}
				}
			}
			Ok::<_, tg::Error>(updated)
		};

		let metadata_subtree_depth_future = async {
			let mut updated = ObjectPropagateUpdateFields::empty();
			if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_DEPTH) {
				let field = ObjectMetadataField::SubtreeDepth;
				let current = Self::update_get_object_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_object_field_u64(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
					if let Some(value) = value {
						let value = 1 + value;
						let field = ObjectMetadataField::SubtreeDepth;
						Self::update_set_object_field_u64(txn, subspace, id, field, value);
						updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_DEPTH;
					}
				}
			}
			Ok::<_, tg::Error>(updated)
		};

		let metadata_subtree_size_future = async {
			let mut updated = ObjectPropagateUpdateFields::empty();
			if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SIZE) {
				let field = ObjectMetadataField::SubtreeSize;
				let current = Self::update_get_object_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_object_field_u64(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values.iter().copied().sum::<Option<u64>>();
					if let Some(value) = value {
						let field = ObjectMetadataField::NodeSize;
						let node_size = Self::update_get_object_field_u64(txn, subspace, id, field)
							.await?
							.unwrap_or(0);
						let value = node_size + value;
						let field = ObjectMetadataField::SubtreeSize;
						Self::update_set_object_field_u64(txn, subspace, id, field, value);
						updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_SIZE;
					}
				}
			}
			Ok::<_, tg::Error>(updated)
		};

		let metadata_subtree_solvable_future = async {
			let mut updated = ObjectPropagateUpdateFields::empty();
			if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVABLE) {
				let field = ObjectMetadataField::SubtreeSolvable;
				let current = Self::update_get_object_field_bool(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ObjectMetadataField::NodeSolvable;
					let node_solvable =
						Self::update_get_object_field_bool(txn, subspace, id, field).await?;
					let field = ObjectMetadataField::SubtreeSolvable;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_object_field_bool(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.try_fold(node_solvable.unwrap_or(false), |output, value| {
							value.map(|value| output || value)
						});
					if let Some(value) = value {
						let field = ObjectMetadataField::SubtreeSolvable;
						Self::update_set_object_field_bool(txn, subspace, id, field, value);
						updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVABLE;
					}
				}
			}
			Ok::<_, tg::Error>(updated)
		};

		let metadata_subtree_solved_future = async {
			let mut updated = ObjectPropagateUpdateFields::empty();
			if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVED) {
				let field = ObjectMetadataField::SubtreeSolved;
				let current = Self::update_get_object_field_bool(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ObjectMetadataField::NodeSolved;
					let node_solved =
						Self::update_get_object_field_bool(txn, subspace, id, field).await?;
					let field = ObjectMetadataField::SubtreeSolved;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_object_field_bool(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.try_fold(node_solved.unwrap_or(true), |output, value| {
							value.map(|value| output && value)
						});
					if let Some(value) = value {
						let field = ObjectMetadataField::SubtreeSolved;
						Self::update_set_object_field_bool(txn, subspace, id, field, value);
						updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVED;
					}
				}
			}
			Ok::<_, tg::Error>(updated)
		};

		let (
			stored_subtree,
			metadata_subtree_count,
			metadata_subtree_depth,
			metadata_subtree_size,
			metadata_subtree_solvable,
			metadata_subtree_solved,
		) = try_join!(
			stored_subtree_future,
			metadata_subtree_count_future,
			metadata_subtree_depth_future,
			metadata_subtree_size_future,
			metadata_subtree_solvable_future,
			metadata_subtree_solved_future,
		)?;

		let updated = ObjectPropagateUpdateFields::empty()
			| stored_subtree
			| metadata_subtree_count
			| metadata_subtree_depth
			| metadata_subtree_size
			| metadata_subtree_solvable
			| metadata_subtree_solved;

		Ok(updated)
	}

	#[allow(clippy::too_many_lines)]
	async fn update_process(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		fields: ProcessPropagateUpdateFields,
	) -> tg::Result<ProcessPropagateUpdateFields> {
		let children = Self::get_process_children_with_transaction(txn, subspace, id).await?;

		let objects = Self::get_process_objects_with_transaction(txn, subspace, id).await?;
		let mut command_object: Option<tg::object::Id> = None;
		let mut error_objects: Vec<tg::object::Id> = Vec::new();
		let mut log_object: Option<Option<tg::object::Id>> = None;
		let mut output_objects: Vec<tg::object::Id> = Vec::new();
		for (id, kind) in objects {
			match kind {
				ProcessObjectKind::Command => {
					command_object = Some(id);
				},
				ProcessObjectKind::Error => {
					error_objects.push(id);
				},
				ProcessObjectKind::Log => {
					log_object = Some(Some(id));
				},
				ProcessObjectKind::Output => {
					output_objects.push(id);
				},
			}
		}

		let command_count_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_COUNT) {
				let field = ProcessMetadataField::NodeCommandCount;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none()
					&& let Some(object) = &command_object
				{
					let field = ObjectMetadataField::SubtreeCount;
					let value =
						Self::update_get_object_field_u64(txn, subspace, object, field).await?;
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeCommandCount;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_COUNT;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_COUNT) {
				let field = ProcessMetadataField::SubtreeCommandCount;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeCommandCount;
					let node_value =
						Self::update_get_process_field_u64(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeCommandCount;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_u64(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output + value))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeCommandCount;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_COUNT;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let command_depth_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_DEPTH) {
				let field = ProcessMetadataField::NodeCommandDepth;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none()
					&& let Some(object) = &command_object
				{
					let field = ObjectMetadataField::SubtreeDepth;
					let value =
						Self::update_get_object_field_u64(txn, subspace, object, field).await?;
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeCommandDepth;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_DEPTH;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_DEPTH) {
				let field = ProcessMetadataField::SubtreeCommandDepth;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeCommandDepth;
					let node_value =
						Self::update_get_process_field_u64(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeCommandDepth;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_u64(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output.max(value)))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeCommandDepth;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_DEPTH;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let command_size_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SIZE) {
				let field = ProcessMetadataField::NodeCommandSize;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none()
					&& let Some(object) = &command_object
				{
					let field = ObjectMetadataField::SubtreeSize;
					let value =
						Self::update_get_object_field_u64(txn, subspace, object, field).await?;
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeCommandSize;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SIZE;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SIZE) {
				let field = ProcessMetadataField::SubtreeCommandSize;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeCommandSize;
					let node_value =
						Self::update_get_process_field_u64(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeCommandSize;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_u64(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output + value))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeCommandSize;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SIZE;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let command_solvable_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVABLE) {
				let field = ProcessMetadataField::NodeCommandSolvable;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current.is_none()
					&& let Some(object) = &command_object
				{
					let field = ObjectMetadataField::SubtreeSolvable;
					let value =
						Self::update_get_object_field_bool(txn, subspace, object, field).await?;
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeCommandSolvable;
						Self::update_set_process_field_bool(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVABLE;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SOLVABLE) {
				let field = ProcessMetadataField::SubtreeCommandSolvable;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeCommandSolvable;
					let node_value =
						Self::update_get_process_field_bool(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeCommandSolvable;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_bool(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output || value))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeCommandSolvable;
						Self::update_set_process_field_bool(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SOLVABLE;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let command_solved_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVED) {
				let field = ProcessMetadataField::NodeCommandSolved;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current.is_none()
					&& let Some(object) = &command_object
				{
					let field = ObjectMetadataField::SubtreeSolved;
					let value =
						Self::update_get_object_field_bool(txn, subspace, object, field).await?;
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeCommandSolved;
						Self::update_set_process_field_bool(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVED;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SOLVED) {
				let field = ProcessMetadataField::SubtreeCommandSolved;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeCommandSolved;
					let node_value =
						Self::update_get_process_field_bool(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeCommandSolved;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_bool(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output && value))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeCommandSolved;
						Self::update_set_process_field_bool(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SOLVED;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let command_stored_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::STORED_NODE_COMMAND) {
				let field = ProcessStoredField::NodeCommand;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current != Some(true)
					&& let Some(object) = &command_object
				{
					let field = ObjectStoredField::Subtree;
					let value =
						Self::update_get_object_field_bool(txn, subspace, object, field).await?;
					if value == Some(true) {
						let field = ProcessStoredField::NodeCommand;
						Self::update_set_process_field_bool(txn, subspace, id, field, true);
						updated |= ProcessPropagateUpdateFields::STORED_NODE_COMMAND;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::STORED_SUBTREE_COMMAND) {
				let field = ProcessStoredField::SubtreeCommand;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current != Some(true) {
					let field = ProcessStoredField::NodeCommand;
					let node_command =
						Self::update_get_process_field_bool(txn, subspace, id, field).await?;
					if node_command == Some(true) {
						let field = ProcessStoredField::SubtreeCommand;
						let child_values = future::try_join_all(children.iter().map(|child| {
							Self::update_get_process_field_bool(txn, subspace, child, field)
						}))
						.await?;
						let value = child_values
							.iter()
							.all(|child| child.as_ref().is_some_and(|value| *value));
						if value {
							let field = ProcessStoredField::SubtreeCommand;
							Self::update_set_process_field_bool(txn, subspace, id, field, true);
							updated |= ProcessPropagateUpdateFields::STORED_SUBTREE_COMMAND;
						}
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let error_count_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_COUNT) {
				let field = ProcessMetadataField::NodeErrorCount;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ObjectMetadataField::SubtreeCount;
					let child_values = future::try_join_all(error_objects.iter().map(|object| {
						Self::update_get_object_field_u64(txn, subspace, object, field)
					}))
					.await?;
					let value = child_values.iter().copied().sum::<Option<u64>>();
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeErrorCount;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_ERROR_COUNT;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_COUNT) {
				let field = ProcessMetadataField::SubtreeErrorCount;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeErrorCount;
					let node_value =
						Self::update_get_process_field_u64(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeErrorCount;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_u64(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output + value))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeErrorCount;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_COUNT;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let error_depth_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_DEPTH) {
				let field = ProcessMetadataField::NodeErrorDepth;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ObjectMetadataField::SubtreeDepth;
					let child_values = future::try_join_all(error_objects.iter().map(|object| {
						Self::update_get_object_field_u64(txn, subspace, object, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeErrorDepth;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_ERROR_DEPTH;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_DEPTH) {
				let field = ProcessMetadataField::SubtreeErrorDepth;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeErrorDepth;
					let node_value =
						Self::update_get_process_field_u64(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeErrorDepth;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_u64(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output.max(value)))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeErrorDepth;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_DEPTH;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let error_size_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SIZE) {
				let field = ProcessMetadataField::NodeErrorSize;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ObjectMetadataField::SubtreeSize;
					let child_values = future::try_join_all(error_objects.iter().map(|object| {
						Self::update_get_object_field_u64(txn, subspace, object, field)
					}))
					.await?;
					let value = child_values.iter().copied().sum::<Option<u64>>();
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeErrorSize;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SIZE;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SIZE) {
				let field = ProcessMetadataField::SubtreeErrorSize;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeErrorSize;
					let node_value =
						Self::update_get_process_field_u64(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeErrorSize;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_u64(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output + value))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeErrorSize;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SIZE;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let error_solvable_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVABLE) {
				let field = ProcessMetadataField::NodeErrorSolvable;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ObjectMetadataField::SubtreeSolvable;
					let child_values = future::try_join_all(error_objects.iter().map(|object| {
						Self::update_get_object_field_bool(txn, subspace, object, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.try_fold(false, |output, value| value.map(|value| output || value));
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeErrorSolvable;
						Self::update_set_process_field_bool(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVABLE;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVABLE) {
				let field = ProcessMetadataField::SubtreeErrorSolvable;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeErrorSolvable;
					let node_value =
						Self::update_get_process_field_bool(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeErrorSolvable;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_bool(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output || value))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeErrorSolvable;
						Self::update_set_process_field_bool(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVABLE;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let error_solved_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVED) {
				let field = ProcessMetadataField::NodeErrorSolved;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ObjectMetadataField::SubtreeSolved;
					let child_values = future::try_join_all(error_objects.iter().map(|object| {
						Self::update_get_object_field_bool(txn, subspace, object, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.try_fold(true, |output, value| value.map(|value| output && value));
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeErrorSolved;
						Self::update_set_process_field_bool(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVED;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVED) {
				let field = ProcessMetadataField::SubtreeErrorSolved;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeErrorSolved;
					let node_value =
						Self::update_get_process_field_bool(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeErrorSolved;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_bool(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output && value))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeErrorSolved;
						Self::update_set_process_field_bool(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVED;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let error_stored_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::STORED_NODE_ERROR) {
				let field = ProcessStoredField::NodeError;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current != Some(true) {
					let field = ObjectStoredField::Subtree;
					let child_values = future::try_join_all(error_objects.iter().map(|object| {
						Self::update_get_object_field_bool(txn, subspace, object, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.all(|child| child.as_ref().is_some_and(|value| *value));
					if value {
						let field = ProcessStoredField::NodeError;
						Self::update_set_process_field_bool(txn, subspace, id, field, true);
						updated |= ProcessPropagateUpdateFields::STORED_NODE_ERROR;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::STORED_SUBTREE_ERROR) {
				let field = ProcessStoredField::SubtreeError;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current != Some(true) {
					let field = ProcessStoredField::NodeError;
					let node_error =
						Self::update_get_process_field_bool(txn, subspace, id, field).await?;
					if node_error == Some(true) {
						let field = ProcessStoredField::SubtreeError;
						let child_values = future::try_join_all(children.iter().map(|child| {
							Self::update_get_process_field_bool(txn, subspace, child, field)
						}))
						.await?;
						let value = child_values
							.iter()
							.all(|child| child.as_ref().is_some_and(|value| *value));
						if value {
							let field = ProcessStoredField::SubtreeError;
							Self::update_set_process_field_bool(txn, subspace, id, field, true);
							updated |= ProcessPropagateUpdateFields::STORED_SUBTREE_ERROR;
						}
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let log_count_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_COUNT) {
				let field = ProcessMetadataField::NodeLogCount;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					if let Some(Some(object)) = &log_object {
						let field = ObjectMetadataField::SubtreeCount;
						let value =
							Self::update_get_object_field_u64(txn, subspace, object, field).await?;
						if let Some(value) = value {
							let field = ProcessMetadataField::NodeLogCount;
							Self::update_set_process_field_u64(txn, subspace, id, field, value);
							updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_COUNT;
						}
					} else if log_object.is_none() {
						let field = ProcessMetadataField::NodeLogCount;
						Self::update_set_process_field_u64(txn, subspace, id, field, 0);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_COUNT;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_COUNT) {
				let field = ProcessMetadataField::SubtreeLogCount;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeLogCount;
					let node_value =
						Self::update_get_process_field_u64(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeLogCount;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_u64(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output + value))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeLogCount;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_COUNT;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let log_depth_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_DEPTH) {
				let field = ProcessMetadataField::NodeLogDepth;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					if let Some(Some(object)) = &log_object {
						let field = ObjectMetadataField::SubtreeDepth;
						let value =
							Self::update_get_object_field_u64(txn, subspace, object, field).await?;
						if let Some(value) = value {
							let field = ProcessMetadataField::NodeLogDepth;
							Self::update_set_process_field_u64(txn, subspace, id, field, value);
							updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_DEPTH;
						}
					} else if log_object.is_none() {
						let field = ProcessMetadataField::NodeLogDepth;
						Self::update_set_process_field_u64(txn, subspace, id, field, 0);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_DEPTH;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_DEPTH) {
				let field = ProcessMetadataField::SubtreeLogDepth;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeLogDepth;
					let node_value =
						Self::update_get_process_field_u64(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeLogDepth;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_u64(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output.max(value)))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeLogDepth;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_DEPTH;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let log_size_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SIZE) {
				let field = ProcessMetadataField::NodeLogSize;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					if let Some(Some(object)) = &log_object {
						let field = ObjectMetadataField::SubtreeSize;
						let value =
							Self::update_get_object_field_u64(txn, subspace, object, field).await?;
						if let Some(value) = value {
							let field = ProcessMetadataField::NodeLogSize;
							Self::update_set_process_field_u64(txn, subspace, id, field, value);
							updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SIZE;
						}
					} else if log_object.is_none() {
						let field = ProcessMetadataField::NodeLogSize;
						Self::update_set_process_field_u64(txn, subspace, id, field, 0);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SIZE;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SIZE) {
				let field = ProcessMetadataField::SubtreeLogSize;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeLogSize;
					let node_value =
						Self::update_get_process_field_u64(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeLogSize;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_u64(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output + value))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeLogSize;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SIZE;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let log_solvable_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVABLE) {
				let field = ProcessMetadataField::NodeLogSolvable;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current.is_none() {
					if let Some(Some(object)) = &log_object {
						let field = ObjectMetadataField::SubtreeSolvable;
						let value =
							Self::update_get_object_field_bool(txn, subspace, object, field)
								.await?;
						if let Some(value) = value {
							let field = ProcessMetadataField::NodeLogSolvable;
							Self::update_set_process_field_bool(txn, subspace, id, field, value);
							updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVABLE;
						}
					} else if log_object.is_none() {
						let field = ProcessMetadataField::NodeLogSolvable;
						Self::update_set_process_field_bool(txn, subspace, id, field, false);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVABLE;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVABLE) {
				let field = ProcessMetadataField::SubtreeLogSolvable;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeLogSolvable;
					let node_value =
						Self::update_get_process_field_bool(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeLogSolvable;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_bool(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output || value))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeLogSolvable;
						Self::update_set_process_field_bool(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVABLE;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let log_solved_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVED) {
				let field = ProcessMetadataField::NodeLogSolved;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current.is_none() {
					if let Some(Some(object)) = &log_object {
						let field = ObjectMetadataField::SubtreeSolved;
						let value =
							Self::update_get_object_field_bool(txn, subspace, object, field)
								.await?;
						if let Some(value) = value {
							let field = ProcessMetadataField::NodeLogSolved;
							Self::update_set_process_field_bool(txn, subspace, id, field, value);
							updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVED;
						}
					} else if log_object.is_none() {
						let field = ProcessMetadataField::NodeLogSolved;
						Self::update_set_process_field_bool(txn, subspace, id, field, true);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVED;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVED) {
				let field = ProcessMetadataField::SubtreeLogSolved;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeLogSolved;
					let node_value =
						Self::update_get_process_field_bool(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeLogSolved;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_bool(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output && value))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeLogSolved;
						Self::update_set_process_field_bool(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVED;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let log_stored_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::STORED_NODE_LOG) {
				let field = ProcessStoredField::NodeLog;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current != Some(true) {
					if let Some(Some(object)) = &log_object {
						let field = ObjectStoredField::Subtree;
						let value =
							Self::update_get_object_field_bool(txn, subspace, object, field)
								.await?;
						if value == Some(true) {
							let field = ProcessStoredField::NodeLog;
							Self::update_set_process_field_bool(txn, subspace, id, field, true);
							updated |= ProcessPropagateUpdateFields::STORED_NODE_LOG;
						}
					} else if log_object.is_none() {
						let field = ProcessStoredField::NodeLog;
						Self::update_set_process_field_bool(txn, subspace, id, field, true);
						updated |= ProcessPropagateUpdateFields::STORED_NODE_LOG;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::STORED_SUBTREE_LOG) {
				let field = ProcessStoredField::SubtreeLog;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current != Some(true) {
					let field = ProcessStoredField::NodeLog;
					let node_log =
						Self::update_get_process_field_bool(txn, subspace, id, field).await?;
					if node_log == Some(true) {
						let field = ProcessStoredField::SubtreeLog;
						let child_values = future::try_join_all(children.iter().map(|child| {
							Self::update_get_process_field_bool(txn, subspace, child, field)
						}))
						.await?;
						let value = child_values
							.iter()
							.all(|child| child.as_ref().is_some_and(|value| *value));
						if value {
							let field = ProcessStoredField::SubtreeLog;
							Self::update_set_process_field_bool(txn, subspace, id, field, true);
							updated |= ProcessPropagateUpdateFields::STORED_SUBTREE_LOG;
						}
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let output_count_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_COUNT) {
				let field = ProcessMetadataField::NodeOutputCount;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ObjectMetadataField::SubtreeCount;
					let child_values = future::try_join_all(output_objects.iter().map(|object| {
						Self::update_get_object_field_u64(txn, subspace, object, field)
					}))
					.await?;
					let value = child_values.iter().copied().sum::<Option<u64>>();
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeOutputCount;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_COUNT;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_COUNT) {
				let field = ProcessMetadataField::SubtreeOutputCount;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeOutputCount;
					let node_value =
						Self::update_get_process_field_u64(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeOutputCount;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_u64(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output + value))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeOutputCount;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_COUNT;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let output_depth_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_DEPTH) {
				let field = ProcessMetadataField::NodeOutputDepth;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ObjectMetadataField::SubtreeDepth;
					let child_values = future::try_join_all(output_objects.iter().map(|object| {
						Self::update_get_object_field_u64(txn, subspace, object, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeOutputDepth;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_DEPTH;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_DEPTH) {
				let field = ProcessMetadataField::SubtreeOutputDepth;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeOutputDepth;
					let node_value =
						Self::update_get_process_field_u64(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeOutputDepth;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_u64(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output.max(value)))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeOutputDepth;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_DEPTH;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let output_size_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SIZE) {
				let field = ProcessMetadataField::NodeOutputSize;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ObjectMetadataField::SubtreeSize;
					let child_values = future::try_join_all(output_objects.iter().map(|object| {
						Self::update_get_object_field_u64(txn, subspace, object, field)
					}))
					.await?;
					let value = child_values.iter().copied().sum::<Option<u64>>();
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeOutputSize;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SIZE;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SIZE) {
				let field = ProcessMetadataField::SubtreeOutputSize;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeOutputSize;
					let node_value =
						Self::update_get_process_field_u64(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeOutputSize;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_u64(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output + value))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeOutputSize;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SIZE;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let output_solvable_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVABLE) {
				let field = ProcessMetadataField::NodeOutputSolvable;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ObjectMetadataField::SubtreeSolvable;
					let child_values = future::try_join_all(output_objects.iter().map(|object| {
						Self::update_get_object_field_bool(txn, subspace, object, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.try_fold(false, |output, value| value.map(|value| output || value));
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeOutputSolvable;
						Self::update_set_process_field_bool(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVABLE;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVABLE) {
				let field = ProcessMetadataField::SubtreeOutputSolvable;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeOutputSolvable;
					let node_value =
						Self::update_get_process_field_bool(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeOutputSolvable;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_bool(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output || value))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeOutputSolvable;
						Self::update_set_process_field_bool(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVABLE;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let output_solved_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVED) {
				let field = ProcessMetadataField::NodeOutputSolved;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ObjectMetadataField::SubtreeSolved;
					let child_values = future::try_join_all(output_objects.iter().map(|object| {
						Self::update_get_object_field_bool(txn, subspace, object, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.try_fold(true, |output, value| value.map(|value| output && value));
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeOutputSolved;
						Self::update_set_process_field_bool(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVED;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVED) {
				let field = ProcessMetadataField::SubtreeOutputSolved;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeOutputSolved;
					let node_value =
						Self::update_get_process_field_bool(txn, subspace, id, field).await?;
					let field = ProcessMetadataField::SubtreeOutputSolved;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_bool(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.copied()
						.fold(node_value, |output, value| {
							output.and_then(|output| value.map(|value| output && value))
						});
					if let Some(value) = value {
						let field = ProcessMetadataField::SubtreeOutputSolved;
						Self::update_set_process_field_bool(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVED;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let output_stored_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::STORED_NODE_OUTPUT) {
				let field = ProcessStoredField::NodeOutput;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current != Some(true) {
					let field = ObjectStoredField::Subtree;
					let child_values = future::try_join_all(output_objects.iter().map(|object| {
						Self::update_get_object_field_bool(txn, subspace, object, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.all(|child| child.as_ref().is_some_and(|value| *value));
					if value {
						let field = ProcessStoredField::NodeOutput;
						Self::update_set_process_field_bool(txn, subspace, id, field, true);
						updated |= ProcessPropagateUpdateFields::STORED_NODE_OUTPUT;
					}
				}
			}

			if fields.contains(ProcessPropagateUpdateFields::STORED_SUBTREE_OUTPUT) {
				let field = ProcessStoredField::SubtreeOutput;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current != Some(true) {
					let field = ProcessStoredField::NodeOutput;
					let node_output =
						Self::update_get_process_field_bool(txn, subspace, id, field).await?;
					if node_output == Some(true) {
						let field = ProcessStoredField::SubtreeOutput;
						let child_values = future::try_join_all(children.iter().map(|child| {
							Self::update_get_process_field_bool(txn, subspace, child, field)
						}))
						.await?;
						let value = child_values
							.iter()
							.all(|child| child.as_ref().is_some_and(|value| *value));
						if value {
							let field = ProcessStoredField::SubtreeOutput;
							Self::update_set_process_field_bool(txn, subspace, id, field, true);
							updated |= ProcessPropagateUpdateFields::STORED_SUBTREE_OUTPUT;
						}
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let subtree_count_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_COUNT) {
				let field = ProcessMetadataField::SubtreeCount;
				let current = Self::update_get_process_field_u64(txn, subspace, id, field).await?;
				if current.is_none() {
					let field = ProcessMetadataField::SubtreeCount;
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_u64(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values.iter().copied().sum::<Option<u64>>();
					if let Some(value) = value {
						let value = 1 + value;
						let field = ProcessMetadataField::SubtreeCount;
						Self::update_set_process_field_u64(txn, subspace, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COUNT;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let subtree_stored_future = async {
			let mut updated = ProcessPropagateUpdateFields::empty();

			if fields.contains(ProcessPropagateUpdateFields::STORED_SUBTREE) {
				let field = ProcessStoredField::Subtree;
				let current = Self::update_get_process_field_bool(txn, subspace, id, field).await?;
				if current != Some(true) {
					let child_values = future::try_join_all(children.iter().map(|child| {
						Self::update_get_process_field_bool(txn, subspace, child, field)
					}))
					.await?;
					let value = child_values
						.iter()
						.all(|child| child.as_ref().is_some_and(|value| *value));
					if value {
						Self::update_set_process_field_bool(txn, subspace, id, field, true);
						updated |= ProcessPropagateUpdateFields::STORED_SUBTREE;
					}
				}
			}

			Ok::<_, tg::Error>(updated)
		};

		let (
			command_count,
			command_depth,
			command_size,
			command_solvable,
			command_solved,
			command_stored,
			error_count,
			error_depth,
			error_size,
			error_solvable,
			error_solved,
			error_stored,
			log_count,
			log_depth,
			log_size,
			log_solvable,
			log_solved,
			log_stored,
			output_count,
			output_depth,
			output_size,
			output_solvable,
			output_solved,
			output_stored,
			subtree_count,
			subtree_stored,
		) = try_join!(
			command_count_future,
			command_depth_future,
			command_size_future,
			command_solvable_future,
			command_solved_future,
			command_stored_future,
			error_count_future,
			error_depth_future,
			error_size_future,
			error_solvable_future,
			error_solved_future,
			error_stored_future,
			log_count_future,
			log_depth_future,
			log_size_future,
			log_solvable_future,
			log_solved_future,
			log_stored_future,
			output_count_future,
			output_depth_future,
			output_size_future,
			output_solvable_future,
			output_solved_future,
			output_stored_future,
			subtree_count_future,
			subtree_stored_future,
		)?;

		let updated =
			ProcessPropagateUpdateFields::empty()
				| command_count
				| command_depth
				| command_size
				| command_solvable
				| command_solved
				| command_stored
				| error_count
				| error_depth
				| error_size | error_solvable
				| error_solved
				| error_stored
				| log_count | log_depth
				| log_size | log_solvable
				| log_solved | log_stored
				| output_count
				| output_depth
				| output_size
				| output_solvable
				| output_solved
				| output_stored
				| subtree_count
				| subtree_stored;

		Ok(updated)
	}

	async fn update_get_object_field_bool<F: Into<ObjectField>>(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		field: F,
	) -> tg::Result<Option<bool>> {
		let key = Self::pack(
			subspace,
			&Key::Object {
				id: id.clone(),
				field: field.into(),
			},
		);
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object field"))?;
		match value {
			Some(bytes) if bytes.is_empty() || bytes[0] != 0 => Ok(Some(true)),
			Some(_) => Ok(Some(false)),
			None => Ok(None),
		}
	}

	async fn update_get_object_field_u64<F: Into<ObjectField>>(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		field: F,
	) -> tg::Result<Option<u64>> {
		let key = Self::pack(
			subspace,
			&Key::Object {
				id: id.clone(),
				field: field.into(),
			},
		);
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object field"))?;
		match value {
			Some(bytes) => {
				let value =
					varint::decode_uvarint(&bytes).ok_or_else(|| tg::error!("invalid varint"))?;
				Ok(Some(value))
			},
			None => Ok(None),
		}
	}

	fn update_set_object_field_bool<F: Into<ObjectField>>(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		field: F,
		value: bool,
	) {
		let key = Self::pack(
			subspace,
			&Key::Object {
				id: id.clone(),
				field: field.into(),
			},
		);
		txn.set(&key, &[u8::from(value)]);
	}

	fn update_set_object_field_u64<F: Into<ObjectField>>(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		field: F,
		value: u64,
	) {
		let key = Self::pack(
			subspace,
			&Key::Object {
				id: id.clone(),
				field: field.into(),
			},
		);
		txn.set(&key, &varint::encode_uvarint(value));
	}

	async fn update_get_process_field_bool<F: Into<ProcessField>>(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		field: F,
	) -> tg::Result<Option<bool>> {
		let key = Self::pack(
			subspace,
			&Key::Process {
				id: id.clone(),
				field: field.into(),
			},
		);
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process field"))?;
		match value {
			Some(bytes) if bytes.is_empty() || bytes[0] != 0 => Ok(Some(true)),
			Some(_) => Ok(Some(false)),
			None => Ok(None),
		}
	}

	async fn update_get_process_field_u64<F: Into<ProcessField>>(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		field: F,
	) -> tg::Result<Option<u64>> {
		let key = Self::pack(
			subspace,
			&Key::Process {
				id: id.clone(),
				field: field.into(),
			},
		);
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process field"))?;
		match value {
			Some(bytes) => {
				let value =
					varint::decode_uvarint(&bytes).ok_or_else(|| tg::error!("invalid varint"))?;
				Ok(Some(value))
			},
			None => Ok(None),
		}
	}

	fn update_set_process_field_bool<F: Into<ProcessField>>(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		field: F,
		value: bool,
	) {
		let key = Self::pack(
			subspace,
			&Key::Process {
				id: id.clone(),
				field: field.into(),
			},
		);
		txn.set(&key, &[u8::from(value)]);
	}

	fn update_set_process_field_u64<F: Into<ProcessField>>(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		field: F,
		value: u64,
	) {
		let key = Self::pack(
			subspace,
			&Key::Process {
				id: id.clone(),
				field: field.into(),
			},
		);
		txn.set(&key, &varint::encode_uvarint(value));
	}

	async fn enqueue_object_parents(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		fields: ObjectPropagateUpdateFields,
		version: &fdbt::Versionstamp,
		partition_total: u64,
	) -> tg::Result<()> {
		let parents = Self::get_object_parents_with_transaction(txn, subspace, id).await?;
		for parent in parents {
			Self::enqueue_object_propagate(
				txn,
				subspace,
				&parent,
				fields,
				version,
				partition_total,
			)
			.await?;
		}

		let processes = Self::get_object_processes_with_transaction(txn, subspace, id).await?;
		for (process, kind) in processes {
			let fields = Self::object_to_process_fields(fields, kind);
			Self::enqueue_process_propagate(
				txn,
				subspace,
				&process,
				fields,
				version,
				partition_total,
			)
			.await?;
		}

		Ok(())
	}

	async fn enqueue_object_propagate(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		fields: ObjectPropagateUpdateFields,
		version: &fdbt::Versionstamp,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Self::pack(
			subspace,
			&Key::Update {
				id: tg::Either::Left(id.clone()),
			},
		);
		let existing = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get existing update"))?
			.map(|value| Update::deserialize(&value))
			.transpose()?;
		if let Some(existing) = existing {
			match existing {
				Update::Put => {
					return Ok(());
				},
				Update::Propagate(PropagateUpdate::Object(update)) => {
					let fields =
						fields | ObjectPropagateUpdateFields::from_bits_truncate(update.fields);
					let update =
						Update::Propagate(PropagateUpdate::Object(ObjectPropagateUpdate {
							fields: fields.bits(),
						}));
					let value = update.serialize()?;
					txn.set(&key, &value);
					return Ok(());
				},
				Update::Propagate(PropagateUpdate::Process(_)) => {},
			}
		}

		let key = Self::pack(
			subspace,
			&Key::Update {
				id: tg::Either::Left(id.clone()),
			},
		);
		let update = Update::Propagate(PropagateUpdate::Object(ObjectPropagateUpdate {
			fields: fields.bits(),
		}));
		let value = update.serialize()?;
		txn.set(&key, &value);
		let id_bytes = id.to_bytes();
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		let key = Self::pack(
			subspace,
			&Key::UpdateVersion {
				partition,
				version: version.clone(),
				id: tg::Either::Left(id.clone()),
			},
		);
		txn.set(&key, &[]);

		Ok(())
	}

	async fn enqueue_process_parents(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		fields: ProcessPropagateUpdateFields,
		version: &fdbt::Versionstamp,
		partition_total: u64,
	) -> tg::Result<()> {
		let parents = Self::get_process_parents_with_transaction(txn, subspace, id).await?;
		for parent in parents {
			Self::enqueue_process_propagate(
				txn,
				subspace,
				&parent,
				fields,
				version,
				partition_total,
			)
			.await?;
		}
		Ok(())
	}

	async fn enqueue_process_propagate(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		fields: ProcessPropagateUpdateFields,
		version: &fdbt::Versionstamp,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Self::pack(
			subspace,
			&Key::Update {
				id: tg::Either::Right(id.clone()),
			},
		);
		let existing = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get existing update"))?
			.map(|value| Update::deserialize(&value))
			.transpose()?;
		if let Some(existing) = existing {
			match existing {
				Update::Put => {
					return Ok(());
				},
				Update::Propagate(PropagateUpdate::Object(_)) => {},
				Update::Propagate(PropagateUpdate::Process(update)) => {
					let fields =
						fields | ProcessPropagateUpdateFields::from_bits_truncate(update.fields);
					let update =
						Update::Propagate(PropagateUpdate::Process(ProcessPropagateUpdate {
							fields: fields.bits(),
						}));
					let value = update.serialize()?;
					txn.set(&key, &value);
					return Ok(());
				},
			}
		}

		let key = Self::pack(
			subspace,
			&Key::Update {
				id: tg::Either::Right(id.clone()),
			},
		);
		let update = Update::Propagate(PropagateUpdate::Process(ProcessPropagateUpdate {
			fields: fields.bits(),
		}));
		let value = update.serialize()?;
		txn.set(&key, &value);
		let id_bytes = id.to_bytes();
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		let key = Self::pack(
			subspace,
			&Key::UpdateVersion {
				partition,
				version: version.clone(),
				id: tg::Either::Right(id.clone()),
			},
		);
		txn.set(&key, &[]);

		Ok(())
	}

	fn object_to_process_fields(
		object_fields: ObjectPropagateUpdateFields,
		kind: ProcessObjectKind,
	) -> ProcessPropagateUpdateFields {
		let mut process_fields = ProcessPropagateUpdateFields::empty();

		if object_fields.contains(ObjectPropagateUpdateFields::STORED_SUBTREE) {
			process_fields |= match kind {
				ProcessObjectKind::Command => {
					ProcessPropagateUpdateFields::STORED_NODE_COMMAND
						| ProcessPropagateUpdateFields::STORED_SUBTREE_COMMAND
				},
				ProcessObjectKind::Error => {
					ProcessPropagateUpdateFields::STORED_NODE_ERROR
						| ProcessPropagateUpdateFields::STORED_SUBTREE_ERROR
				},
				ProcessObjectKind::Log => {
					ProcessPropagateUpdateFields::STORED_NODE_LOG
						| ProcessPropagateUpdateFields::STORED_SUBTREE_LOG
				},
				ProcessObjectKind::Output => {
					ProcessPropagateUpdateFields::STORED_NODE_OUTPUT
						| ProcessPropagateUpdateFields::STORED_SUBTREE_OUTPUT
				},
			};
		}

		if object_fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_COUNT) {
			process_fields |= match kind {
				ProcessObjectKind::Command => {
					ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_COUNT
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_COUNT
				},
				ProcessObjectKind::Error => {
					ProcessPropagateUpdateFields::METADATA_NODE_ERROR_COUNT
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_COUNT
				},
				ProcessObjectKind::Log => {
					ProcessPropagateUpdateFields::METADATA_NODE_LOG_COUNT
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_COUNT
				},
				ProcessObjectKind::Output => {
					ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_COUNT
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_COUNT
				},
			};
		}

		if object_fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_DEPTH) {
			process_fields |= match kind {
				ProcessObjectKind::Command => {
					ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_DEPTH
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_DEPTH
				},
				ProcessObjectKind::Error => {
					ProcessPropagateUpdateFields::METADATA_NODE_ERROR_DEPTH
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_DEPTH
				},
				ProcessObjectKind::Log => {
					ProcessPropagateUpdateFields::METADATA_NODE_LOG_DEPTH
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_DEPTH
				},
				ProcessObjectKind::Output => {
					ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_DEPTH
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_DEPTH
				},
			};
		}

		if object_fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SIZE) {
			process_fields |= match kind {
				ProcessObjectKind::Command => {
					ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SIZE
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SIZE
				},
				ProcessObjectKind::Error => {
					ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SIZE
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SIZE
				},
				ProcessObjectKind::Log => {
					ProcessPropagateUpdateFields::METADATA_NODE_LOG_SIZE
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SIZE
				},
				ProcessObjectKind::Output => {
					ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SIZE
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SIZE
				},
			};
		}

		if object_fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVABLE) {
			process_fields |= match kind {
				ProcessObjectKind::Command => {
					ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVABLE
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SOLVABLE
				},
				ProcessObjectKind::Error => {
					ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVABLE
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVABLE
				},
				ProcessObjectKind::Log => {
					ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVABLE
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVABLE
				},
				ProcessObjectKind::Output => {
					ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVABLE
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVABLE
				},
			};
		}

		if object_fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVED) {
			process_fields |= match kind {
				ProcessObjectKind::Command => {
					ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVED
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SOLVED
				},
				ProcessObjectKind::Error => {
					ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVED
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVED
				},
				ProcessObjectKind::Log => {
					ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVED
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVED
				},
				ProcessObjectKind::Output => {
					ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVED
						| ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVED
				},
			};
		}

		process_fields
	}
}
