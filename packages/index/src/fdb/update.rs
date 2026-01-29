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
	futures::future,
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
		let begin = self.pack(&(KeyKind::UpdateVersion.to_i32().unwrap(),));
		let end = self.pack(&(KeyKind::UpdateVersion.to_i32().unwrap(), versionstamp));
		let range = fdb::RangeOption {
			begin: fdb::KeySelector::first_greater_or_equal(begin),
			end: fdb::KeySelector::first_greater_or_equal(end),
			limit: Some(1),
			mode: fdb::options::StreamingMode::WantAll,
			..Default::default()
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if updates are finished"))?;
		let finished = entries.is_empty();

		Ok(finished)
	}

	pub async fn update_batch(&self, batch_size: usize) -> tg::Result<usize> {
		self.database
			.run(|txn, _| {
				let this = self.clone();
				async move {
					this.update_batch_inner(&txn, batch_size)
						.await
						.map_err(|source| fdb::FdbBindingError::CustomError(source.into()))
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to process update batch"))
	}

	async fn update_batch_inner(
		&self,
		txn: &fdb::Transaction,
		batch_size: usize,
	) -> tg::Result<usize> {
		let prefix = self.pack(&(KeyKind::UpdateVersion.to_i32().unwrap(),));
		let subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			limit: Some(batch_size),
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&subspace)
		};
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get update version range"))?;
		let entries = entries
			.iter()
			.map(|entry| {
				let key = self.unpack(entry.key())?;
				let Key::UpdateVersion { version, id } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((version, id))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		let mut count = 0;
		for (version, id) in entries {
			let key = self.pack(&Key::Update { id: id.clone() });
			let value = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get update key"))?;

			let Some(update) = value else {
				let key = self.pack(&Key::UpdateVersion {
					version: version.clone(),
					id: id.clone(),
				});
				txn.clear(&key);
				count += 1;
				continue;
			};

			let update = Update::deserialize(&update)?;

			let fields = match (&id, &update) {
				(tg::Either::Left(id), Update::Propagate(PropagateUpdate::Object(update))) => {
					let fields = ObjectPropagateUpdateFields::from_bits_truncate(update.fields);
					let fields = self.update_object(txn, id, fields).await?;
					tg::Either::Left(fields)
				},
				(tg::Either::Left(id), _) => {
					let fields = ObjectPropagateUpdateFields::ALL;
					let fields = self.update_object(txn, id, fields).await?;
					tg::Either::Left(fields)
				},
				(tg::Either::Right(id), Update::Propagate(PropagateUpdate::Process(update))) => {
					let fields = ProcessPropagateUpdateFields::from_bits_truncate(update.fields);
					let fields = self.update_process(txn, id, fields).await?;
					tg::Either::Right(fields)
				},
				(tg::Either::Right(id), _) => {
					let fields = ProcessPropagateUpdateFields::ALL;
					let fields = self.update_process(txn, id, fields).await?;
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
						self.enqueue_object_parents(txn, id, fields, &version)
							.await?;
					},
					tg::Either::Right(id) => {
						let fields = ProcessPropagateUpdateFields::ALL;
						self.enqueue_process_parents(txn, id, fields, &version)
							.await?;
					},
				}
			} else {
				match (&id, &fields) {
					(tg::Either::Left(id), tg::Either::Left(fields)) if !fields.is_empty() => {
						self.enqueue_object_parents(txn, id, *fields, &version)
							.await?;
					},
					(tg::Either::Right(id), tg::Either::Right(fields)) if !fields.is_empty() => {
						self.enqueue_process_parents(txn, id, *fields, &version)
							.await?;
					},
					_ => {},
				}
			}

			let key = self.pack(&Key::Update { id: id.clone() });
			txn.clear(&key);
			let key = self.pack(&Key::UpdateVersion {
				version: version.clone(),
				id: id.clone(),
			});
			txn.clear(&key);

			count += 1;
		}

		Ok(count)
	}

	async fn update_object(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
		fields: ObjectPropagateUpdateFields,
	) -> tg::Result<ObjectPropagateUpdateFields> {
		let children = self.get_object_children_with_transaction(txn, id).await?;

		let mut updated = ObjectPropagateUpdateFields::empty();

		if fields.contains(ObjectPropagateUpdateFields::STORED_SUBTREE) {
			let current = self
				.update_get_object_field_bool(txn, id, ObjectStoredField::Subtree)
				.await?;
			if current != Some(true) {
				let child_values = future::try_join_all(children.iter().map(|child| {
					self.update_get_object_field_bool(txn, child, ObjectStoredField::Subtree)
				}))
				.await?;
				let value = child_values
					.iter()
					.all(|child| child.as_ref().is_some_and(|value| *value));
				if value {
					self.update_set_object_field_bool(txn, id, ObjectStoredField::Subtree, true);
					updated |= ObjectPropagateUpdateFields::STORED_SUBTREE;
				}
			}
		}

		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_COUNT) {
			let current = self
				.update_get_object_field_u64(txn, id, ObjectMetadataField::SubtreeCount)
				.await?;
			if current.is_none() {
				let child_values = future::try_join_all(children.iter().map(|child| {
					self.update_get_object_field_u64(txn, child, ObjectMetadataField::SubtreeCount)
				}))
				.await?;
				let value = child_values.iter().copied().sum::<Option<u64>>();
				if let Some(value) = value {
					let value = 1 + value;
					let field = ObjectMetadataField::SubtreeCount;
					self.update_set_object_field_u64(txn, id, field, value);
					updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_COUNT;
				}
			}
		}

		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_DEPTH) {
			let current = self
				.update_get_object_field_u64(txn, id, ObjectMetadataField::SubtreeDepth)
				.await?;
			if current.is_none() {
				let child_values = future::try_join_all(children.iter().map(|child| {
					self.update_get_object_field_u64(txn, child, ObjectMetadataField::SubtreeDepth)
				}))
				.await?;
				let value = child_values
					.iter()
					.copied()
					.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
				if let Some(value) = value {
					let value = 1 + value;
					let field = ObjectMetadataField::SubtreeDepth;
					self.update_set_object_field_u64(txn, id, field, value);
					updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_DEPTH;
				}
			}
		}

		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SIZE) {
			let current = self
				.update_get_object_field_u64(txn, id, ObjectMetadataField::SubtreeSize)
				.await?;
			if current.is_none() {
				let child_values = future::try_join_all(children.iter().map(|child| {
					self.update_get_object_field_u64(txn, child, ObjectMetadataField::SubtreeSize)
				}))
				.await?;
				let value = child_values.iter().copied().sum::<Option<u64>>();
				if let Some(value) = value {
					let node_size = self
						.update_get_object_field_u64(txn, id, ObjectMetadataField::NodeSize)
						.await?
						.unwrap_or(0);
					let value = node_size + value;
					let field = ObjectMetadataField::SubtreeSize;
					self.update_set_object_field_u64(txn, id, field, value);
					updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_SIZE;
				}
			}
		}

		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVABLE) {
			let current = self
				.update_get_object_field_bool(txn, id, ObjectMetadataField::SubtreeSolvable)
				.await?;
			if current.is_none() {
				let node_solvable = self
					.update_get_object_field_bool(txn, id, ObjectMetadataField::NodeSolvable)
					.await?;
				let field = ObjectMetadataField::SubtreeSolvable;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_object_field_bool(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.try_fold(node_solvable.unwrap_or(false), |output, value| {
						value.map(|value| output || value)
					});
				if let Some(value) = value {
					let field = ObjectMetadataField::SubtreeSolvable;
					self.update_set_object_field_bool(txn, id, field, value);
					updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVABLE;
				}
			}
		}

		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVED) {
			let current = self
				.update_get_object_field_bool(txn, id, ObjectMetadataField::SubtreeSolved)
				.await?;
			if current.is_none() {
				let node_solved = self
					.update_get_object_field_bool(txn, id, ObjectMetadataField::NodeSolved)
					.await?;
				let field = ObjectMetadataField::SubtreeSolved;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_object_field_bool(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.try_fold(node_solved.unwrap_or(true), |output, value| {
						value.map(|value| output && value)
					});
				if let Some(value) = value {
					let field = ObjectMetadataField::SubtreeSolved;
					self.update_set_object_field_bool(txn, id, field, value);
					updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVED;
				}
			}
		}

		Ok(updated)
	}

	#[allow(clippy::too_many_lines)]
	async fn update_process(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		fields: ProcessPropagateUpdateFields,
	) -> tg::Result<ProcessPropagateUpdateFields> {
		let children = self.get_process_children_with_transaction(txn, id).await?;

		let objects = self.get_process_objects_with_transaction(txn, id).await?;
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

		let mut updated = ProcessPropagateUpdateFields::empty();

		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_COUNT) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeCommandCount)
				.await?;
			if current.is_none()
				&& let Some(object) = &command_object
			{
				let value = self
					.update_get_object_field_u64(txn, object, ObjectMetadataField::SubtreeCount)
					.await?;
				if let Some(value) = value {
					let field = ProcessMetadataField::NodeCommandCount;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_COUNT;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_DEPTH) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeCommandDepth)
				.await?;
			if current.is_none()
				&& let Some(object) = &command_object
			{
				let value = self
					.update_get_object_field_u64(txn, object, ObjectMetadataField::SubtreeDepth)
					.await?;
				if let Some(value) = value {
					let field = ProcessMetadataField::NodeCommandDepth;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_DEPTH;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SIZE) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeCommandSize)
				.await?;
			if current.is_none()
				&& let Some(object) = &command_object
			{
				let value = self
					.update_get_object_field_u64(txn, object, ObjectMetadataField::SubtreeSize)
					.await?;
				if let Some(value) = value {
					let field = ProcessMetadataField::NodeCommandSize;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SIZE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVABLE) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeCommandSolvable)
				.await?;
			if current.is_none()
				&& let Some(object) = &command_object
			{
				let value = self
					.update_get_object_field_bool(txn, object, ObjectMetadataField::SubtreeSolvable)
					.await?;
				if let Some(value) = value {
					let field = ProcessMetadataField::NodeCommandSolvable;
					self.update_set_process_field_bool(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVABLE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVED) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeCommandSolved)
				.await?;
			if current.is_none()
				&& let Some(object) = &command_object
			{
				let value = self
					.update_get_object_field_bool(txn, object, ObjectMetadataField::SubtreeSolved)
					.await?;
				if let Some(value) = value {
					let field = ProcessMetadataField::NodeCommandSolved;
					self.update_set_process_field_bool(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVED;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_COUNT) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeErrorCount)
				.await?;
			if current.is_none() {
				let child_values = future::try_join_all(error_objects.iter().map(|object| {
					self.update_get_object_field_u64(txn, object, ObjectMetadataField::SubtreeCount)
				}))
				.await?;
				let value = child_values.iter().copied().sum::<Option<u64>>();
				if let Some(value) = value {
					let field = ProcessMetadataField::NodeErrorCount;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_ERROR_COUNT;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_DEPTH) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeErrorDepth)
				.await?;
			if current.is_none() {
				let child_values = future::try_join_all(error_objects.iter().map(|object| {
					self.update_get_object_field_u64(txn, object, ObjectMetadataField::SubtreeDepth)
				}))
				.await?;
				let value = child_values
					.iter()
					.copied()
					.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
				if let Some(value) = value {
					let field = ProcessMetadataField::NodeErrorDepth;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_ERROR_DEPTH;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SIZE) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeErrorSize)
				.await?;
			if current.is_none() {
				let child_values = future::try_join_all(error_objects.iter().map(|object| {
					self.update_get_object_field_u64(txn, object, ObjectMetadataField::SubtreeSize)
				}))
				.await?;
				let value = child_values.iter().copied().sum::<Option<u64>>();
				if let Some(value) = value {
					let field = ProcessMetadataField::NodeErrorSize;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SIZE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVABLE) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeErrorSolvable)
				.await?;
			if current.is_none() {
				let field = ObjectMetadataField::SubtreeSolvable;
				let child_values = future::try_join_all(
					error_objects
						.iter()
						.map(|object| self.update_get_object_field_bool(txn, object, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.try_fold(false, |output, value| value.map(|value| output || value));
				if let Some(value) = value {
					let field = ProcessMetadataField::NodeErrorSolvable;
					self.update_set_process_field_bool(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVABLE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVED) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeErrorSolved)
				.await?;
			if current.is_none() {
				let field = ObjectMetadataField::SubtreeSolved;
				let child_values = future::try_join_all(
					error_objects
						.iter()
						.map(|object| self.update_get_object_field_bool(txn, object, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.try_fold(true, |output, value| value.map(|value| output && value));
				if let Some(value) = value {
					let field = ProcessMetadataField::NodeErrorSolved;
					self.update_set_process_field_bool(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVED;
				}
			}
		}

		if let Some(Some(object)) = &log_object {
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_COUNT) {
				let current = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeLogCount)
					.await?;
				if current.is_none() {
					let value = self
						.update_get_object_field_u64(txn, object, ObjectMetadataField::SubtreeCount)
						.await?;
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeLogCount;
						self.update_set_process_field_u64(txn, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_COUNT;
					}
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_DEPTH) {
				let current = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeLogDepth)
					.await?;
				if current.is_none() {
					let value = self
						.update_get_object_field_u64(txn, object, ObjectMetadataField::SubtreeDepth)
						.await?;
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeLogDepth;
						self.update_set_process_field_u64(txn, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_DEPTH;
					}
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SIZE) {
				let current = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeLogSize)
					.await?;
				if current.is_none() {
					let value = self
						.update_get_object_field_u64(txn, object, ObjectMetadataField::SubtreeSize)
						.await?;
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeLogSize;
						self.update_set_process_field_u64(txn, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SIZE;
					}
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVABLE) {
				let current = self
					.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeLogSolvable)
					.await?;
				if current.is_none() {
					let field = ObjectMetadataField::SubtreeSolvable;
					let value = self
						.update_get_object_field_bool(txn, object, field)
						.await?;
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeLogSolvable;
						self.update_set_process_field_bool(txn, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVABLE;
					}
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVED) {
				let current = self
					.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeLogSolved)
					.await?;
				if current.is_none() {
					let field = ObjectMetadataField::SubtreeSolved;
					let value = self
						.update_get_object_field_bool(txn, object, field)
						.await?;
					if let Some(value) = value {
						let field = ProcessMetadataField::NodeLogSolved;
						self.update_set_process_field_bool(txn, id, field, value);
						updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVED;
					}
				}
			}
		} else if log_object.is_none() {
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_COUNT) {
				let current = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeLogCount)
					.await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeLogCount;
					self.update_set_process_field_u64(txn, id, field, 0);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_COUNT;
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_DEPTH) {
				let current = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeLogDepth)
					.await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeLogDepth;
					self.update_set_process_field_u64(txn, id, field, 0);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_DEPTH;
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SIZE) {
				let current = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeLogSize)
					.await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeLogSize;
					self.update_set_process_field_u64(txn, id, field, 0);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SIZE;
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVABLE) {
				let current = self
					.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeLogSolvable)
					.await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeLogSolvable;
					self.update_set_process_field_bool(txn, id, field, false);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVABLE;
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVED) {
				let current = self
					.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeLogSolved)
					.await?;
				if current.is_none() {
					let field = ProcessMetadataField::NodeLogSolved;
					self.update_set_process_field_bool(txn, id, field, true);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVED;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_COUNT) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeOutputCount)
				.await?;
			if current.is_none() {
				let child_values = future::try_join_all(output_objects.iter().map(|object| {
					self.update_get_object_field_u64(txn, object, ObjectMetadataField::SubtreeCount)
				}))
				.await?;
				let value = child_values.iter().copied().sum::<Option<u64>>();
				if let Some(value) = value {
					let field = ProcessMetadataField::NodeOutputCount;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_COUNT;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_DEPTH) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeOutputDepth)
				.await?;
			if current.is_none() {
				let child_values = future::try_join_all(output_objects.iter().map(|object| {
					self.update_get_object_field_u64(txn, object, ObjectMetadataField::SubtreeDepth)
				}))
				.await?;
				let value = child_values
					.iter()
					.copied()
					.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
				if let Some(value) = value {
					let field = ProcessMetadataField::NodeOutputDepth;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_DEPTH;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SIZE) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeOutputSize)
				.await?;
			if current.is_none() {
				let child_values = future::try_join_all(output_objects.iter().map(|object| {
					self.update_get_object_field_u64(txn, object, ObjectMetadataField::SubtreeSize)
				}))
				.await?;
				let value = child_values.iter().copied().sum::<Option<u64>>();
				if let Some(value) = value {
					let field = ProcessMetadataField::NodeOutputSize;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SIZE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVABLE) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeOutputSolvable)
				.await?;
			if current.is_none() {
				let field = ObjectMetadataField::SubtreeSolvable;
				let child_values = future::try_join_all(
					output_objects
						.iter()
						.map(|object| self.update_get_object_field_bool(txn, object, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.try_fold(false, |output, value| value.map(|value| output || value));
				if let Some(value) = value {
					let field = ProcessMetadataField::NodeOutputSolvable;
					self.update_set_process_field_bool(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVABLE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVED) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeOutputSolved)
				.await?;
			if current.is_none() {
				let field = ObjectMetadataField::SubtreeSolved;
				let child_values = future::try_join_all(
					output_objects
						.iter()
						.map(|object| self.update_get_object_field_bool(txn, object, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.try_fold(true, |output, value| value.map(|value| output && value));
				if let Some(value) = value {
					let field = ProcessMetadataField::NodeOutputSolved;
					self.update_set_process_field_bool(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVED;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_COUNT) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::SubtreeCount)
				.await?;
			if current.is_none() {
				let field = ProcessMetadataField::SubtreeCount;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_u64(txn, child, field)),
				)
				.await?;
				let value = child_values.iter().copied().sum::<Option<u64>>();
				if let Some(value) = value {
					let value = 1 + value;
					let field = ProcessMetadataField::SubtreeCount;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COUNT;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_COUNT) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::SubtreeCommandCount)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeCommandCount)
					.await?;
				let field = ProcessMetadataField::SubtreeCommandCount;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_u64(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeCommandCount;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_COUNT;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_DEPTH) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::SubtreeCommandDepth)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeCommandDepth)
					.await?;
				let field = ProcessMetadataField::SubtreeCommandDepth;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_u64(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output.max(value)))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeCommandDepth;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_DEPTH;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SIZE) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::SubtreeCommandSize)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeCommandSize)
					.await?;
				let field = ProcessMetadataField::SubtreeCommandSize;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_u64(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeCommandSize;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SIZE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SOLVABLE) {
			let field = ProcessMetadataField::SubtreeCommandSolvable;
			let current = self.update_get_process_field_bool(txn, id, field).await?;
			if current.is_none() {
				let field = ProcessMetadataField::NodeCommandSolvable;
				let node_value = self.update_get_process_field_bool(txn, id, field).await?;
				let field = ProcessMetadataField::SubtreeCommandSolvable;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_bool(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output || value))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeCommandSolvable;
					self.update_set_process_field_bool(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SOLVABLE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SOLVED) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::SubtreeCommandSolved)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeCommandSolved)
					.await?;
				let field = ProcessMetadataField::SubtreeCommandSolved;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_bool(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output && value))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeCommandSolved;
					self.update_set_process_field_bool(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SOLVED;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_COUNT) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::SubtreeErrorCount)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeErrorCount)
					.await?;
				let field = ProcessMetadataField::SubtreeErrorCount;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_u64(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeErrorCount;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_COUNT;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_DEPTH) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::SubtreeErrorDepth)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeErrorDepth)
					.await?;
				let field = ProcessMetadataField::SubtreeErrorDepth;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_u64(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output.max(value)))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeErrorDepth;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_DEPTH;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SIZE) {
			let field = ProcessMetadataField::SubtreeErrorSize;
			let current = self.update_get_process_field_u64(txn, id, field).await?;
			if current.is_none() {
				let field = ProcessMetadataField::NodeErrorSize;
				let node_value = self.update_get_process_field_u64(txn, id, field).await?;
				let field = ProcessMetadataField::SubtreeErrorSize;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_u64(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeErrorSize;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SIZE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVABLE) {
			let field = ProcessMetadataField::SubtreeErrorSolvable;
			let current = self.update_get_process_field_bool(txn, id, field).await?;
			if current.is_none() {
				let field = ProcessMetadataField::NodeErrorSolvable;
				let node_value = self.update_get_process_field_bool(txn, id, field).await?;
				let field = ProcessMetadataField::SubtreeErrorSolvable;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_bool(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output || value))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeErrorSolvable;
					self.update_set_process_field_bool(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVABLE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVED) {
			let field = ProcessMetadataField::SubtreeErrorSolved;
			let current = self.update_get_process_field_bool(txn, id, field).await?;
			if current.is_none() {
				let field = ProcessMetadataField::NodeErrorSolved;
				let node_value = self.update_get_process_field_bool(txn, id, field).await?;
				let field = ProcessMetadataField::SubtreeErrorSolved;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_bool(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output && value))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeErrorSolved;
					self.update_set_process_field_bool(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVED;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_COUNT) {
			let field = ProcessMetadataField::SubtreeLogCount;
			let current = self.update_get_process_field_u64(txn, id, field).await?;
			if current.is_none() {
				let field = ProcessMetadataField::NodeLogCount;
				let node_value = self.update_get_process_field_u64(txn, id, field).await?;
				let field = ProcessMetadataField::SubtreeLogCount;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_u64(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeLogCount;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_COUNT;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_DEPTH) {
			let field = ProcessMetadataField::SubtreeLogDepth;
			let current = self.update_get_process_field_u64(txn, id, field).await?;
			if current.is_none() {
				let field = ProcessMetadataField::NodeLogDepth;
				let node_value = self.update_get_process_field_u64(txn, id, field).await?;
				let field = ProcessMetadataField::SubtreeLogDepth;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_u64(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output.max(value)))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeLogDepth;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_DEPTH;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SIZE) {
			let field = ProcessMetadataField::SubtreeLogSize;
			let current = self.update_get_process_field_u64(txn, id, field).await?;
			if current.is_none() {
				let field = ProcessMetadataField::NodeLogSize;
				let node_value = self.update_get_process_field_u64(txn, id, field).await?;
				let field = ProcessMetadataField::SubtreeLogSize;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_u64(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeLogSize;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SIZE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVABLE) {
			let field = ProcessMetadataField::SubtreeLogSolvable;
			let current = self.update_get_process_field_bool(txn, id, field).await?;
			if current.is_none() {
				let field = ProcessMetadataField::NodeLogSolvable;
				let node_value = self.update_get_process_field_bool(txn, id, field).await?;
				let field = ProcessMetadataField::SubtreeLogSolvable;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_bool(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output || value))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeLogSolvable;
					self.update_set_process_field_bool(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVABLE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVED) {
			let field = ProcessMetadataField::SubtreeLogSolved;
			let current = self.update_get_process_field_bool(txn, id, field).await?;
			if current.is_none() {
				let field = ProcessMetadataField::NodeLogSolved;
				let node_value = self.update_get_process_field_bool(txn, id, field).await?;
				let field = ProcessMetadataField::SubtreeLogSolved;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_bool(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output && value))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeLogSolved;
					self.update_set_process_field_bool(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVED;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_COUNT) {
			let field = ProcessMetadataField::SubtreeOutputCount;
			let current = self.update_get_process_field_u64(txn, id, field).await?;
			if current.is_none() {
				let field = ProcessMetadataField::NodeOutputCount;
				let node_value = self.update_get_process_field_u64(txn, id, field).await?;
				let field = ProcessMetadataField::SubtreeOutputCount;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_u64(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeOutputCount;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_COUNT;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_DEPTH) {
			let field = ProcessMetadataField::SubtreeOutputDepth;
			let current = self.update_get_process_field_u64(txn, id, field).await?;
			if current.is_none() {
				let field = ProcessMetadataField::NodeOutputDepth;
				let node_value = self.update_get_process_field_u64(txn, id, field).await?;
				let field = ProcessMetadataField::SubtreeOutputDepth;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_u64(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output.max(value)))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeOutputDepth;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_DEPTH;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SIZE) {
			let field = ProcessMetadataField::SubtreeOutputSize;
			let current = self.update_get_process_field_u64(txn, id, field).await?;
			if current.is_none() {
				let field = ProcessMetadataField::NodeOutputSize;
				let node_value = self.update_get_process_field_u64(txn, id, field).await?;
				let field = ProcessMetadataField::SubtreeOutputSize;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_u64(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeOutputSize;
					self.update_set_process_field_u64(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SIZE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVABLE) {
			let field = ProcessMetadataField::SubtreeOutputSolvable;
			let current = self.update_get_process_field_bool(txn, id, field).await?;
			if current.is_none() {
				let field = ProcessMetadataField::NodeOutputSolvable;
				let node_value = self.update_get_process_field_bool(txn, id, field).await?;
				let field = ProcessMetadataField::SubtreeOutputSolvable;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_bool(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output || value))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeOutputSolvable;
					self.update_set_process_field_bool(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVABLE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVED) {
			let field = ProcessMetadataField::SubtreeOutputSolved;
			let current = self.update_get_process_field_bool(txn, id, field).await?;
			if current.is_none() {
				let field = ProcessMetadataField::NodeOutputSolved;
				let node_value = self.update_get_process_field_bool(txn, id, field).await?;
				let field = ProcessMetadataField::SubtreeOutputSolved;
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_bool(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.copied()
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output && value))
					});
				if let Some(value) = value {
					let field = ProcessMetadataField::SubtreeOutputSolved;
					self.update_set_process_field_bool(txn, id, field, value);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVED;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_NODE_COMMAND) {
			let field = ProcessStoredField::NodeCommand;
			let current = self.update_get_process_field_bool(txn, id, field).await?;
			if current != Some(true)
				&& let Some(object) = &command_object
			{
				let field = ObjectStoredField::Subtree;
				let value = self
					.update_get_object_field_bool(txn, object, field)
					.await?;
				if value == Some(true) {
					let field = ProcessStoredField::NodeCommand;
					self.update_set_process_field_bool(txn, id, field, true);
					updated |= ProcessPropagateUpdateFields::STORED_NODE_COMMAND;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_NODE_ERROR) {
			let field = ProcessStoredField::NodeError;
			let current = self.update_get_process_field_bool(txn, id, field).await?;
			if current != Some(true) {
				let field = ObjectStoredField::Subtree;
				let child_values = future::try_join_all(
					error_objects
						.iter()
						.map(|object| self.update_get_object_field_bool(txn, object, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.all(|child| child.as_ref().is_some_and(|value| *value));
				if value {
					let field = ProcessStoredField::NodeError;
					self.update_set_process_field_bool(txn, id, field, true);
					updated |= ProcessPropagateUpdateFields::STORED_NODE_ERROR;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_NODE_LOG) {
			let field = ProcessStoredField::NodeLog;
			let current = self.update_get_process_field_bool(txn, id, field).await?;
			if current != Some(true) {
				if let Some(Some(object)) = &log_object {
					let field = ObjectStoredField::Subtree;
					let value = self
						.update_get_object_field_bool(txn, object, field)
						.await?;
					if value == Some(true) {
						let field = ProcessStoredField::NodeLog;
						self.update_set_process_field_bool(txn, id, field, true);
						updated |= ProcessPropagateUpdateFields::STORED_NODE_LOG;
					}
				} else if log_object.is_none() {
					self.update_set_process_field_bool(txn, id, ProcessStoredField::NodeLog, true);
					updated |= ProcessPropagateUpdateFields::STORED_NODE_LOG;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_NODE_OUTPUT) {
			let field = ProcessStoredField::NodeOutput;
			let current = self.update_get_process_field_bool(txn, id, field).await?;
			if current != Some(true) {
				let field = ObjectStoredField::Subtree;
				let child_values = future::try_join_all(
					output_objects
						.iter()
						.map(|object| self.update_get_object_field_bool(txn, object, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.all(|child| child.as_ref().is_some_and(|value| *value));
				if value {
					let field = ProcessStoredField::NodeOutput;
					self.update_set_process_field_bool(txn, id, field, true);
					updated |= ProcessPropagateUpdateFields::STORED_NODE_OUTPUT;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_SUBTREE) {
			let field = ProcessStoredField::Subtree;
			let current = self.update_get_process_field_bool(txn, id, field).await?;
			if current != Some(true) {
				let child_values = future::try_join_all(
					children
						.iter()
						.map(|child| self.update_get_process_field_bool(txn, child, field)),
				)
				.await?;
				let value = child_values
					.iter()
					.all(|child| child.as_ref().is_some_and(|value| *value));
				if value {
					self.update_set_process_field_bool(txn, id, ProcessStoredField::Subtree, true);
					updated |= ProcessPropagateUpdateFields::STORED_SUBTREE;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_SUBTREE_COMMAND) {
			let field = ProcessStoredField::SubtreeCommand;
			let current = self.update_get_process_field_bool(txn, id, field).await?;
			if current != Some(true) {
				let field = ProcessStoredField::NodeCommand;
				let node_command = self.update_get_process_field_bool(txn, id, field).await?;
				if node_command == Some(true) {
					let field = ProcessStoredField::SubtreeCommand;
					let child_values = future::try_join_all(
						children
							.iter()
							.map(|child| self.update_get_process_field_bool(txn, child, field)),
					)
					.await?;
					let value = child_values
						.iter()
						.all(|child| child.as_ref().is_some_and(|value| *value));
					if value {
						let field = ProcessStoredField::SubtreeCommand;
						self.update_set_process_field_bool(txn, id, field, true);
						updated |= ProcessPropagateUpdateFields::STORED_SUBTREE_COMMAND;
					}
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_SUBTREE_ERROR) {
			let field = ProcessStoredField::SubtreeError;
			let current = self.update_get_process_field_bool(txn, id, field).await?;
			if current != Some(true) {
				let field = ProcessStoredField::NodeError;
				let node_error = self.update_get_process_field_bool(txn, id, field).await?;
				if node_error == Some(true) {
					let field = ProcessStoredField::SubtreeError;
					let child_values = future::try_join_all(
						children
							.iter()
							.map(|child| self.update_get_process_field_bool(txn, child, field)),
					)
					.await?;
					let value = child_values
						.iter()
						.all(|child| child.as_ref().is_some_and(|value| *value));
					if value {
						let field = ProcessStoredField::SubtreeError;
						self.update_set_process_field_bool(txn, id, field, true);
						updated |= ProcessPropagateUpdateFields::STORED_SUBTREE_ERROR;
					}
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_SUBTREE_LOG) {
			let field = ProcessStoredField::SubtreeLog;
			let current = self.update_get_process_field_bool(txn, id, field).await?;
			if current != Some(true) {
				let field = ProcessStoredField::NodeLog;
				let node_log = self.update_get_process_field_bool(txn, id, field).await?;
				if node_log == Some(true) {
					let field = ProcessStoredField::SubtreeLog;
					let child_values = future::try_join_all(
						children
							.iter()
							.map(|child| self.update_get_process_field_bool(txn, child, field)),
					)
					.await?;
					let value = child_values
						.iter()
						.all(|child| child.as_ref().is_some_and(|value| *value));
					if value {
						let field = ProcessStoredField::SubtreeLog;
						self.update_set_process_field_bool(txn, id, field, true);
						updated |= ProcessPropagateUpdateFields::STORED_SUBTREE_LOG;
					}
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_SUBTREE_OUTPUT) {
			let field = ProcessStoredField::SubtreeOutput;
			let current = self.update_get_process_field_bool(txn, id, field).await?;
			if current != Some(true) {
				let field = ProcessStoredField::NodeOutput;
				let node_output = self.update_get_process_field_bool(txn, id, field).await?;
				if node_output == Some(true) {
					let field = ProcessStoredField::SubtreeOutput;
					let child_values = future::try_join_all(
						children
							.iter()
							.map(|child| self.update_get_process_field_bool(txn, child, field)),
					)
					.await?;
					let value = child_values
						.iter()
						.all(|child| child.as_ref().is_some_and(|value| *value));
					if value {
						let field = ProcessStoredField::SubtreeOutput;
						self.update_set_process_field_bool(txn, id, field, true);
						updated |= ProcessPropagateUpdateFields::STORED_SUBTREE_OUTPUT;
					}
				}
			}
		}

		Ok(updated)
	}

	async fn update_get_object_field_bool<F: Into<ObjectField>>(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
		field: F,
	) -> tg::Result<Option<bool>> {
		let key = self.pack(&Key::Object {
			id: id.clone(),
			field: field.into(),
		});
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
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
		field: F,
	) -> tg::Result<Option<u64>> {
		let key = self.pack(&Key::Object {
			id: id.clone(),
			field: field.into(),
		});
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
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
		field: F,
		value: bool,
	) {
		let key = self.pack(&Key::Object {
			id: id.clone(),
			field: field.into(),
		});
		txn.set(&key, &[u8::from(value)]);
	}

	fn update_set_object_field_u64<F: Into<ObjectField>>(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
		field: F,
		value: u64,
	) {
		let key = self.pack(&Key::Object {
			id: id.clone(),
			field: field.into(),
		});
		txn.set(&key, &varint::encode_uvarint(value));
	}

	async fn update_get_process_field_bool<F: Into<ProcessField>>(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		field: F,
	) -> tg::Result<Option<bool>> {
		let key = self.pack(&Key::Process {
			id: id.clone(),
			field: field.into(),
		});
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
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		field: F,
	) -> tg::Result<Option<u64>> {
		let key = self.pack(&Key::Process {
			id: id.clone(),
			field: field.into(),
		});
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
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		field: F,
		value: bool,
	) {
		let key = self.pack(&Key::Process {
			id: id.clone(),
			field: field.into(),
		});
		txn.set(&key, &[u8::from(value)]);
	}

	fn update_set_process_field_u64<F: Into<ProcessField>>(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		field: F,
		value: u64,
	) {
		let key = self.pack(&Key::Process {
			id: id.clone(),
			field: field.into(),
		});
		txn.set(&key, &varint::encode_uvarint(value));
	}

	async fn enqueue_object_parents(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
		fields: ObjectPropagateUpdateFields,
		version: &fdbt::Versionstamp,
	) -> tg::Result<()> {
		let parents = self.get_object_parents_with_transaction(txn, id).await?;
		for parent in parents {
			self.enqueue_object_propagate(txn, &parent, fields, version)
				.await?;
		}

		let processes = self.get_object_processes_with_transaction(txn, id).await?;
		for (process, kind) in processes {
			let fields = Self::object_to_process_fields(fields, kind);
			self.enqueue_process_propagate(txn, &process, fields, version)
				.await?;
		}

		Ok(())
	}

	async fn enqueue_object_propagate(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
		fields: ObjectPropagateUpdateFields,
		version: &fdbt::Versionstamp,
	) -> tg::Result<()> {
		let key = self.pack(&Key::Update {
			id: tg::Either::Left(id.clone()),
		});
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

		let key = self.pack(&Key::Update {
			id: tg::Either::Left(id.clone()),
		});
		let update = Update::Propagate(PropagateUpdate::Object(ObjectPropagateUpdate {
			fields: fields.bits(),
		}));
		let value = update.serialize()?;
		txn.set(&key, &value);
		let key = self.pack(&Key::UpdateVersion {
			version: version.clone(),
			id: tg::Either::Left(id.clone()),
		});
		txn.set(&key, &[]);

		Ok(())
	}

	async fn enqueue_process_parents(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		fields: ProcessPropagateUpdateFields,
		version: &fdbt::Versionstamp,
	) -> tg::Result<()> {
		let parents = self.get_process_parents_with_transaction(txn, id).await?;
		for parent in parents {
			self.enqueue_process_propagate(txn, &parent, fields, version)
				.await?;
		}
		Ok(())
	}

	async fn enqueue_process_propagate(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		fields: ProcessPropagateUpdateFields,
		version: &fdbt::Versionstamp,
	) -> tg::Result<()> {
		let key = self.pack(&Key::Update {
			id: tg::Either::Right(id.clone()),
		});
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

		let key = self.pack(&Key::Update {
			id: tg::Either::Right(id.clone()),
		});
		let update = Update::Propagate(PropagateUpdate::Process(ProcessPropagateUpdate {
			fields: fields.bits(),
		}));
		let value = update.serialize()?;
		txn.set(&key, &value);
		let key = self.pack(&Key::UpdateVersion {
			version: version.clone(),
			id: tg::Either::Right(id.clone()),
		});
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
