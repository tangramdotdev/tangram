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

		struct ChildObject {
			stored_subtree: Option<bool>,
			subtree_count: Option<u64>,
			subtree_depth: Option<u64>,
			subtree_size: Option<u64>,
			subtree_solvable: Option<bool>,
			subtree_solved: Option<bool>,
		}

		let mut child_objects: Vec<Option<ChildObject>> = Vec::with_capacity(children.len());
		for child in &children {
			let stored_subtree = self
				.update_get_object_field_bool(txn, child, ObjectStoredField::Subtree)
				.await?;
			let subtree_count = self
				.update_get_object_field_u64(txn, child, ObjectMetadataField::SubtreeCount)
				.await?;
			let subtree_depth = self
				.update_get_object_field_u64(txn, child, ObjectMetadataField::SubtreeDepth)
				.await?;
			let subtree_size = self
				.update_get_object_field_u64(txn, child, ObjectMetadataField::SubtreeSize)
				.await?;
			let subtree_solvable = self
				.update_get_object_field_bool(txn, child, ObjectMetadataField::SubtreeSolvable)
				.await?;
			let subtree_solved = self
				.update_get_object_field_bool(txn, child, ObjectMetadataField::SubtreeSolved)
				.await?;
			if stored_subtree.is_some()
				|| subtree_count.is_some()
				|| subtree_depth.is_some()
				|| subtree_size.is_some()
				|| subtree_solvable.is_some()
				|| subtree_solved.is_some()
			{
				child_objects.push(Some(ChildObject {
					stored_subtree,
					subtree_count,
					subtree_depth,
					subtree_size,
					subtree_solvable,
					subtree_solved,
				}));
			} else {
				child_objects.push(None);
			}
		}

		let mut updated = ObjectPropagateUpdateFields::empty();

		if fields.contains(ObjectPropagateUpdateFields::STORED_SUBTREE) {
			let current = self
				.update_get_object_field_bool(txn, id, ObjectStoredField::Subtree)
				.await?;
			if current != Some(true) {
				let value = child_objects.iter().all(|child| {
					child
						.as_ref()
						.is_some_and(|obj| obj.stored_subtree == Some(true))
				});
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
				let value = child_objects
					.iter()
					.map(|option| option.as_ref().and_then(|child| child.subtree_count))
					.sum::<Option<u64>>();
				if let Some(value) = value {
					let value = 1 + value;
					self.update_set_object_field_u64(
						txn,
						id,
						ObjectMetadataField::SubtreeCount,
						value,
					);
					updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_COUNT;
				}
			}
		}

		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_DEPTH) {
			let current = self
				.update_get_object_field_u64(txn, id, ObjectMetadataField::SubtreeDepth)
				.await?;
			if current.is_none() {
				let value = child_objects
					.iter()
					.map(|option| option.as_ref().and_then(|child| child.subtree_depth))
					.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
				if let Some(value) = value {
					let value = 1 + value;
					self.update_set_object_field_u64(
						txn,
						id,
						ObjectMetadataField::SubtreeDepth,
						value,
					);
					updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_DEPTH;
				}
			}
		}

		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SIZE) {
			let current = self
				.update_get_object_field_u64(txn, id, ObjectMetadataField::SubtreeSize)
				.await?;
			if current.is_none() {
				let node_size = self
					.update_get_object_field_u64(txn, id, ObjectMetadataField::NodeSize)
					.await?;
				if let Some(node_size) = node_size {
					let value = child_objects
						.iter()
						.map(|option| option.as_ref().and_then(|child| child.subtree_size))
						.sum::<Option<u64>>();
					if let Some(value) = value {
						let value = node_size + value;
						self.update_set_object_field_u64(
							txn,
							id,
							ObjectMetadataField::SubtreeSize,
							value,
						);
						updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_SIZE;
					}
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
				if let Some(node_solvable) = node_solvable {
					let value = child_objects
						.iter()
						.map(|option| option.as_ref().and_then(|child| child.subtree_solvable))
						.try_fold(node_solvable, |output, value| {
							value.map(|value| output || value)
						});
					if let Some(value) = value {
						self.update_set_object_field_bool(
							txn,
							id,
							ObjectMetadataField::SubtreeSolvable,
							value,
						);
						updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVABLE;
					}
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
				if let Some(node_solved) = node_solved {
					let value = child_objects
						.iter()
						.map(|option| option.as_ref().and_then(|child| child.subtree_solved))
						.try_fold(node_solved, |output, value| {
							value.map(|value| output && value)
						});
					if let Some(value) = value {
						self.update_set_object_field_bool(
							txn,
							id,
							ObjectMetadataField::SubtreeSolved,
							value,
						);
						updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVED;
					}
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
		let process_children = self.get_process_children_with_transaction(txn, id).await?;

		struct ChildProcess {
			stored_subtree: Option<bool>,
			stored_subtree_command: Option<bool>,
			stored_subtree_error: Option<bool>,
			stored_subtree_log: Option<bool>,
			stored_subtree_output: Option<bool>,
			subtree_count: Option<u64>,
			subtree_command_count: Option<u64>,
			subtree_command_depth: Option<u64>,
			subtree_command_size: Option<u64>,
			subtree_command_solvable: Option<bool>,
			subtree_command_solved: Option<bool>,
			subtree_error_count: Option<u64>,
			subtree_error_depth: Option<u64>,
			subtree_error_size: Option<u64>,
			subtree_error_solvable: Option<bool>,
			subtree_error_solved: Option<bool>,
			subtree_log_count: Option<u64>,
			subtree_log_depth: Option<u64>,
			subtree_log_size: Option<u64>,
			subtree_log_solvable: Option<bool>,
			subtree_log_solved: Option<bool>,
			subtree_output_count: Option<u64>,
			subtree_output_depth: Option<u64>,
			subtree_output_size: Option<u64>,
			subtree_output_solvable: Option<bool>,
			subtree_output_solved: Option<bool>,
		}

		let mut children: Vec<Option<ChildProcess>> = Vec::with_capacity(process_children.len());
		for child in &process_children {
			let stored_subtree = self
				.update_get_process_field_bool(txn, child, ProcessStoredField::Subtree)
				.await?;
			let stored_subtree_command = self
				.update_get_process_field_bool(txn, child, ProcessStoredField::SubtreeCommand)
				.await?;
			let stored_subtree_error = self
				.update_get_process_field_bool(txn, child, ProcessStoredField::SubtreeError)
				.await?;
			let stored_subtree_log = self
				.update_get_process_field_bool(txn, child, ProcessStoredField::SubtreeLog)
				.await?;
			let stored_subtree_output = self
				.update_get_process_field_bool(txn, child, ProcessStoredField::SubtreeOutput)
				.await?;
			let subtree_count = self
				.update_get_process_field_u64(txn, child, ProcessMetadataField::SubtreeCount)
				.await?;
			let subtree_command_count = self
				.update_get_process_field_u64(txn, child, ProcessMetadataField::SubtreeCommandCount)
				.await?;
			let subtree_command_depth = self
				.update_get_process_field_u64(txn, child, ProcessMetadataField::SubtreeCommandDepth)
				.await?;
			let subtree_command_size = self
				.update_get_process_field_u64(txn, child, ProcessMetadataField::SubtreeCommandSize)
				.await?;
			let subtree_command_solvable = self
				.update_get_process_field_bool(
					txn,
					child,
					ProcessMetadataField::SubtreeCommandSolvable,
				)
				.await?;
			let subtree_command_solved = self
				.update_get_process_field_bool(
					txn,
					child,
					ProcessMetadataField::SubtreeCommandSolved,
				)
				.await?;
			let subtree_error_count = self
				.update_get_process_field_u64(txn, child, ProcessMetadataField::SubtreeErrorCount)
				.await?;
			let subtree_error_depth = self
				.update_get_process_field_u64(txn, child, ProcessMetadataField::SubtreeErrorDepth)
				.await?;
			let subtree_error_size = self
				.update_get_process_field_u64(txn, child, ProcessMetadataField::SubtreeErrorSize)
				.await?;
			let subtree_error_solvable = self
				.update_get_process_field_bool(
					txn,
					child,
					ProcessMetadataField::SubtreeErrorSolvable,
				)
				.await?;
			let subtree_error_solved = self
				.update_get_process_field_bool(txn, child, ProcessMetadataField::SubtreeErrorSolved)
				.await?;
			let subtree_log_count = self
				.update_get_process_field_u64(txn, child, ProcessMetadataField::SubtreeLogCount)
				.await?;
			let subtree_log_depth = self
				.update_get_process_field_u64(txn, child, ProcessMetadataField::SubtreeLogDepth)
				.await?;
			let subtree_log_size = self
				.update_get_process_field_u64(txn, child, ProcessMetadataField::SubtreeLogSize)
				.await?;
			let subtree_log_solvable = self
				.update_get_process_field_bool(txn, child, ProcessMetadataField::SubtreeLogSolvable)
				.await?;
			let subtree_log_solved = self
				.update_get_process_field_bool(txn, child, ProcessMetadataField::SubtreeLogSolved)
				.await?;
			let subtree_output_count = self
				.update_get_process_field_u64(txn, child, ProcessMetadataField::SubtreeOutputCount)
				.await?;
			let subtree_output_depth = self
				.update_get_process_field_u64(txn, child, ProcessMetadataField::SubtreeOutputDepth)
				.await?;
			let subtree_output_size = self
				.update_get_process_field_u64(txn, child, ProcessMetadataField::SubtreeOutputSize)
				.await?;
			let subtree_output_solvable = self
				.update_get_process_field_bool(
					txn,
					child,
					ProcessMetadataField::SubtreeOutputSolvable,
				)
				.await?;
			let subtree_output_solved = self
				.update_get_process_field_bool(
					txn,
					child,
					ProcessMetadataField::SubtreeOutputSolved,
				)
				.await?;
			let exists = stored_subtree.is_some()
				|| stored_subtree_command.is_some()
				|| subtree_count.is_some()
				|| subtree_command_count.is_some();
			if exists {
				children.push(Some(ChildProcess {
					stored_subtree,
					stored_subtree_command,
					stored_subtree_error,
					stored_subtree_log,
					stored_subtree_output,
					subtree_count,
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_command_solvable,
					subtree_command_solved,
					subtree_error_count,
					subtree_error_depth,
					subtree_error_size,
					subtree_error_solvable,
					subtree_error_solved,
					subtree_log_count,
					subtree_log_depth,
					subtree_log_size,
					subtree_log_solvable,
					subtree_log_solved,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_output_solvable,
					subtree_output_solved,
				}));
			} else {
				children.push(None);
			}
		}

		let objects = self.get_process_objects_with_transaction(txn, id).await?;

		struct ObjectData {
			stored_subtree: Option<bool>,
			subtree_count: Option<u64>,
			subtree_depth: Option<u64>,
			subtree_size: Option<u64>,
			subtree_solvable: Option<bool>,
			subtree_solved: Option<bool>,
		}

		let mut command_object: Option<ObjectData> = None;
		let mut error_objects: Vec<Option<ObjectData>> = Vec::new();
		let mut log_object: Option<Option<ObjectData>> = None;
		let mut output_objects: Vec<Option<ObjectData>> = Vec::new();

		for (object_id, kind) in &objects {
			let stored_subtree = self
				.update_get_object_field_bool(txn, object_id, ObjectStoredField::Subtree)
				.await?;
			let subtree_count = self
				.update_get_object_field_u64(txn, object_id, ObjectMetadataField::SubtreeCount)
				.await?;
			let subtree_depth = self
				.update_get_object_field_u64(txn, object_id, ObjectMetadataField::SubtreeDepth)
				.await?;
			let subtree_size = self
				.update_get_object_field_u64(txn, object_id, ObjectMetadataField::SubtreeSize)
				.await?;
			let subtree_solvable = self
				.update_get_object_field_bool(txn, object_id, ObjectMetadataField::SubtreeSolvable)
				.await?;
			let subtree_solved = self
				.update_get_object_field_bool(txn, object_id, ObjectMetadataField::SubtreeSolved)
				.await?;

			let exists = stored_subtree.is_some()
				|| subtree_count.is_some()
				|| subtree_depth.is_some()
				|| subtree_size.is_some()
				|| subtree_solvable.is_some()
				|| subtree_solved.is_some();

			let data = if exists {
				Some(ObjectData {
					stored_subtree,
					subtree_count,
					subtree_depth,
					subtree_size,
					subtree_solvable,
					subtree_solved,
				})
			} else {
				None
			};

			match kind {
				ProcessObjectKind::Command => {
					command_object = data;
				},
				ProcessObjectKind::Error => {
					error_objects.push(data);
				},
				ProcessObjectKind::Log => {
					log_object = Some(data);
				},
				ProcessObjectKind::Output => {
					output_objects.push(data);
				},
			}
		}

		let mut updated = ProcessPropagateUpdateFields::empty();

		if let Some(object) = &command_object {
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_COUNT) {
				let current = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeCommandCount)
					.await?;
				if current.is_none()
					&& let Some(value) = object.subtree_count
				{
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::NodeCommandCount,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_COUNT;
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_DEPTH) {
				let current = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeCommandDepth)
					.await?;
				if current.is_none()
					&& let Some(value) = object.subtree_depth
				{
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::NodeCommandDepth,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_DEPTH;
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SIZE) {
				let current = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeCommandSize)
					.await?;
				if current.is_none()
					&& let Some(value) = object.subtree_size
				{
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::NodeCommandSize,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SIZE;
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVABLE) {
				let current = self
					.update_get_process_field_bool(
						txn,
						id,
						ProcessMetadataField::NodeCommandSolvable,
					)
					.await?;
				if current.is_none()
					&& let Some(value) = object.subtree_solvable
				{
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::NodeCommandSolvable,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVABLE;
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVED) {
				let current = self
					.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeCommandSolved)
					.await?;
				if current.is_none()
					&& let Some(value) = object.subtree_solved
				{
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::NodeCommandSolved,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVED;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_COUNT) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeErrorCount)
				.await?;
			if current.is_none() {
				let value = error_objects
					.iter()
					.map(|option| option.as_ref().and_then(|obj| obj.subtree_count))
					.sum::<Option<u64>>();
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::NodeErrorCount,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_ERROR_COUNT;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_DEPTH) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeErrorDepth)
				.await?;
			if current.is_none() {
				let value = error_objects
					.iter()
					.map(|option| option.as_ref().and_then(|obj| obj.subtree_depth))
					.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::NodeErrorDepth,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_ERROR_DEPTH;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SIZE) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeErrorSize)
				.await?;
			if current.is_none() {
				let value = error_objects
					.iter()
					.map(|option| option.as_ref().and_then(|obj| obj.subtree_size))
					.sum::<Option<u64>>();
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::NodeErrorSize,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SIZE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVABLE) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeErrorSolvable)
				.await?;
			if current.is_none() {
				let value = error_objects
					.iter()
					.map(|option| option.as_ref().and_then(|obj| obj.subtree_solvable))
					.try_fold(false, |output, value| value.map(|value| output || value));
				if let Some(value) = value {
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::NodeErrorSolvable,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVABLE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVED) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeErrorSolved)
				.await?;
			if current.is_none() {
				let value = error_objects
					.iter()
					.map(|option| option.as_ref().and_then(|obj| obj.subtree_solved))
					.try_fold(true, |output, value| value.map(|value| output && value));
				if let Some(value) = value {
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::NodeErrorSolved,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVED;
				}
			}
		}

		if let Some(Some(object)) = &log_object {
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_COUNT) {
				let current = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeLogCount)
					.await?;
				if current.is_none()
					&& let Some(value) = object.subtree_count
				{
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::NodeLogCount,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_COUNT;
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_DEPTH) {
				let current = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeLogDepth)
					.await?;
				if current.is_none()
					&& let Some(value) = object.subtree_depth
				{
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::NodeLogDepth,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_DEPTH;
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SIZE) {
				let current = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeLogSize)
					.await?;
				if current.is_none()
					&& let Some(value) = object.subtree_size
				{
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::NodeLogSize,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SIZE;
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVABLE) {
				let current = self
					.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeLogSolvable)
					.await?;
				if current.is_none()
					&& let Some(value) = object.subtree_solvable
				{
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::NodeLogSolvable,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVABLE;
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVED) {
				let current = self
					.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeLogSolved)
					.await?;
				if current.is_none()
					&& let Some(value) = object.subtree_solved
				{
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::NodeLogSolved,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVED;
				}
			}
		} else if log_object.is_none() {
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_COUNT) {
				let current = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeLogCount)
					.await?;
				if current.is_none() {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::NodeLogCount,
						0,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_COUNT;
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_DEPTH) {
				let current = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeLogDepth)
					.await?;
				if current.is_none() {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::NodeLogDepth,
						0,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_DEPTH;
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SIZE) {
				let current = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeLogSize)
					.await?;
				if current.is_none() {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::NodeLogSize,
						0,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SIZE;
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVABLE) {
				let current = self
					.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeLogSolvable)
					.await?;
				if current.is_none() {
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::NodeLogSolvable,
						false,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVABLE;
				}
			}
			if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVED) {
				let current = self
					.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeLogSolved)
					.await?;
				if current.is_none() {
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::NodeLogSolved,
						true,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVED;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_COUNT) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeOutputCount)
				.await?;
			if current.is_none() {
				let value = output_objects
					.iter()
					.map(|option| option.as_ref().and_then(|obj| obj.subtree_count))
					.sum::<Option<u64>>();
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::NodeOutputCount,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_COUNT;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_DEPTH) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeOutputDepth)
				.await?;
			if current.is_none() {
				let value = output_objects
					.iter()
					.map(|option| option.as_ref().and_then(|obj| obj.subtree_depth))
					.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::NodeOutputDepth,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_DEPTH;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SIZE) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeOutputSize)
				.await?;
			if current.is_none() {
				let value = output_objects
					.iter()
					.map(|option| option.as_ref().and_then(|obj| obj.subtree_size))
					.sum::<Option<u64>>();
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::NodeOutputSize,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SIZE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVABLE) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeOutputSolvable)
				.await?;
			if current.is_none() {
				let value = output_objects
					.iter()
					.map(|option| option.as_ref().and_then(|obj| obj.subtree_solvable))
					.try_fold(false, |output, value| value.map(|value| output || value));
				if let Some(value) = value {
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::NodeOutputSolvable,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVABLE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVED) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeOutputSolved)
				.await?;
			if current.is_none() {
				let value = output_objects
					.iter()
					.map(|option| option.as_ref().and_then(|obj| obj.subtree_solved))
					.try_fold(true, |output, value| value.map(|value| output && value));
				if let Some(value) = value {
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::NodeOutputSolved,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVED;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_COUNT) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::SubtreeCount)
				.await?;
			if current.is_none() {
				let value = children
					.iter()
					.map(|option| option.as_ref().and_then(|child| child.subtree_count))
					.sum::<Option<u64>>();
				if let Some(value) = value {
					let value = 1 + value;
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::SubtreeCount,
						value,
					);
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
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.subtree_command_count)
					})
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::SubtreeCommandCount,
						value,
					);
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
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.subtree_command_depth)
					})
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output.max(value)))
					});
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::SubtreeCommandDepth,
						value,
					);
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
				let value = children
					.iter()
					.map(|option| option.as_ref().and_then(|child| child.subtree_command_size))
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::SubtreeCommandSize,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SIZE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SOLVABLE) {
			let current = self
				.update_get_process_field_bool(
					txn,
					id,
					ProcessMetadataField::SubtreeCommandSolvable,
				)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_bool(
						txn,
						id,
						ProcessMetadataField::NodeCommandSolvable,
					)
					.await?;
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.subtree_command_solvable)
					})
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output || value))
					});
				if let Some(value) = value {
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::SubtreeCommandSolvable,
						value,
					);
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
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.subtree_command_solved)
					})
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output && value))
					});
				if let Some(value) = value {
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::SubtreeCommandSolved,
						value,
					);
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
				let value = children
					.iter()
					.map(|option| option.as_ref().and_then(|child| child.subtree_error_count))
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::SubtreeErrorCount,
						value,
					);
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
				let value = children
					.iter()
					.map(|option| option.as_ref().and_then(|child| child.subtree_error_depth))
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output.max(value)))
					});
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::SubtreeErrorDepth,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_DEPTH;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SIZE) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::SubtreeErrorSize)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeErrorSize)
					.await?;
				let value = children
					.iter()
					.map(|option| option.as_ref().and_then(|child| child.subtree_error_size))
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::SubtreeErrorSize,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SIZE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVABLE) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::SubtreeErrorSolvable)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeErrorSolvable)
					.await?;
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.subtree_error_solvable)
					})
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output || value))
					});
				if let Some(value) = value {
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::SubtreeErrorSolvable,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVABLE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVED) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::SubtreeErrorSolved)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeErrorSolved)
					.await?;
				let value = children
					.iter()
					.map(|option| option.as_ref().and_then(|child| child.subtree_error_solved))
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output && value))
					});
				if let Some(value) = value {
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::SubtreeErrorSolved,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVED;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_COUNT) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::SubtreeLogCount)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeLogCount)
					.await?;
				let value = children
					.iter()
					.map(|option| option.as_ref().and_then(|child| child.subtree_log_count))
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::SubtreeLogCount,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_COUNT;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_DEPTH) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::SubtreeLogDepth)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeLogDepth)
					.await?;
				let value = children
					.iter()
					.map(|option| option.as_ref().and_then(|child| child.subtree_log_depth))
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output.max(value)))
					});
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::SubtreeLogDepth,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_DEPTH;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SIZE) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::SubtreeLogSize)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeLogSize)
					.await?;
				let value = children
					.iter()
					.map(|option| option.as_ref().and_then(|child| child.subtree_log_size))
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::SubtreeLogSize,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SIZE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVABLE) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::SubtreeLogSolvable)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeLogSolvable)
					.await?;
				let value = children
					.iter()
					.map(|option| option.as_ref().and_then(|child| child.subtree_log_solvable))
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output || value))
					});
				if let Some(value) = value {
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::SubtreeLogSolvable,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVABLE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVED) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::SubtreeLogSolved)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeLogSolved)
					.await?;
				let value = children
					.iter()
					.map(|option| option.as_ref().and_then(|child| child.subtree_log_solved))
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output && value))
					});
				if let Some(value) = value {
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::SubtreeLogSolved,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVED;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_COUNT) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::SubtreeOutputCount)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeOutputCount)
					.await?;
				let value = children
					.iter()
					.map(|option| option.as_ref().and_then(|child| child.subtree_output_count))
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::SubtreeOutputCount,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_COUNT;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_DEPTH) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::SubtreeOutputDepth)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeOutputDepth)
					.await?;
				let value = children
					.iter()
					.map(|option| option.as_ref().and_then(|child| child.subtree_output_depth))
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output.max(value)))
					});
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::SubtreeOutputDepth,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_DEPTH;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SIZE) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::SubtreeOutputSize)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_u64(txn, id, ProcessMetadataField::NodeOutputSize)
					.await?;
				let value = children
					.iter()
					.map(|option| option.as_ref().and_then(|child| child.subtree_output_size))
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::SubtreeOutputSize,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SIZE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVABLE) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::SubtreeOutputSolvable)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_bool(
						txn,
						id,
						ProcessMetadataField::NodeOutputSolvable,
					)
					.await?;
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.subtree_output_solvable)
					})
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output || value))
					});
				if let Some(value) = value {
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::SubtreeOutputSolvable,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVABLE;
				}
			}
		}
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVED) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessMetadataField::SubtreeOutputSolved)
				.await?;
			if current.is_none() {
				let node_value = self
					.update_get_process_field_bool(txn, id, ProcessMetadataField::NodeOutputSolved)
					.await?;
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.subtree_output_solved)
					})
					.fold(node_value, |output, value| {
						output.and_then(|output| value.map(|value| output && value))
					});
				if let Some(value) = value {
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessMetadataField::SubtreeOutputSolved,
						value,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVED;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_NODE_COMMAND) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessStoredField::NodeCommand)
				.await?;
			if current != Some(true)
				&& let Some(object) = &command_object
				&& object.stored_subtree == Some(true)
			{
				self.update_set_process_field_bool(txn, id, ProcessStoredField::NodeCommand, true);
				updated |= ProcessPropagateUpdateFields::STORED_NODE_COMMAND;
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_NODE_ERROR) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessStoredField::NodeError)
				.await?;
			if current != Some(true) {
				let value = error_objects.iter().all(|option| {
					option
						.as_ref()
						.is_some_and(|obj| obj.stored_subtree == Some(true))
				});
				if value {
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessStoredField::NodeError,
						true,
					);
					updated |= ProcessPropagateUpdateFields::STORED_NODE_ERROR;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_NODE_LOG) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessStoredField::NodeLog)
				.await?;
			if current != Some(true) {
				if let Some(Some(object)) = &log_object {
					if object.stored_subtree == Some(true) {
						self.update_set_process_field_bool(
							txn,
							id,
							ProcessStoredField::NodeLog,
							true,
						);
						updated |= ProcessPropagateUpdateFields::STORED_NODE_LOG;
					}
				} else if log_object.is_none() {
					self.update_set_process_field_bool(txn, id, ProcessStoredField::NodeLog, true);
					updated |= ProcessPropagateUpdateFields::STORED_NODE_LOG;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_NODE_OUTPUT) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessStoredField::NodeOutput)
				.await?;
			if current != Some(true) {
				let value = output_objects.iter().all(|option| {
					option
						.as_ref()
						.is_some_and(|obj| obj.stored_subtree == Some(true))
				});
				if value {
					self.update_set_process_field_bool(
						txn,
						id,
						ProcessStoredField::NodeOutput,
						true,
					);
					updated |= ProcessPropagateUpdateFields::STORED_NODE_OUTPUT;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_SUBTREE) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessStoredField::Subtree)
				.await?;
			if current != Some(true) {
				let value = children.iter().all(|child| {
					child
						.as_ref()
						.is_some_and(|c| c.stored_subtree == Some(true))
				});
				if value {
					self.update_set_process_field_bool(txn, id, ProcessStoredField::Subtree, true);
					updated |= ProcessPropagateUpdateFields::STORED_SUBTREE;
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_SUBTREE_COMMAND) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessStoredField::SubtreeCommand)
				.await?;
			if current != Some(true) {
				let node_command = self
					.update_get_process_field_bool(txn, id, ProcessStoredField::NodeCommand)
					.await?;
				if node_command == Some(true) {
					let value = children.iter().all(|child| {
						child
							.as_ref()
							.is_some_and(|c| c.stored_subtree_command == Some(true))
					});
					if value {
						self.update_set_process_field_bool(
							txn,
							id,
							ProcessStoredField::SubtreeCommand,
							true,
						);
						updated |= ProcessPropagateUpdateFields::STORED_SUBTREE_COMMAND;
					}
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_SUBTREE_ERROR) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessStoredField::SubtreeError)
				.await?;
			if current != Some(true) {
				let node_error = self
					.update_get_process_field_bool(txn, id, ProcessStoredField::NodeError)
					.await?;
				if node_error == Some(true) {
					let value = children.iter().all(|child| {
						child
							.as_ref()
							.is_some_and(|c| c.stored_subtree_error == Some(true))
					});
					if value {
						self.update_set_process_field_bool(
							txn,
							id,
							ProcessStoredField::SubtreeError,
							true,
						);
						updated |= ProcessPropagateUpdateFields::STORED_SUBTREE_ERROR;
					}
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_SUBTREE_LOG) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessStoredField::SubtreeLog)
				.await?;
			if current != Some(true) {
				let node_log = self
					.update_get_process_field_bool(txn, id, ProcessStoredField::NodeLog)
					.await?;
				if node_log == Some(true) {
					let value = children.iter().all(|child| {
						child
							.as_ref()
							.is_some_and(|c| c.stored_subtree_log == Some(true))
					});
					if value {
						self.update_set_process_field_bool(
							txn,
							id,
							ProcessStoredField::SubtreeLog,
							true,
						);
						updated |= ProcessPropagateUpdateFields::STORED_SUBTREE_LOG;
					}
				}
			}
		}

		if fields.contains(ProcessPropagateUpdateFields::STORED_SUBTREE_OUTPUT) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessStoredField::SubtreeOutput)
				.await?;
			if current != Some(true) {
				let node_output = self
					.update_get_process_field_bool(txn, id, ProcessStoredField::NodeOutput)
					.await?;
				if node_output == Some(true) {
					let value = children.iter().all(|child| {
						child
							.as_ref()
							.is_some_and(|c| c.stored_subtree_output == Some(true))
					});
					if value {
						self.update_set_process_field_bool(
							txn,
							id,
							ProcessStoredField::SubtreeOutput,
							true,
						);
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
