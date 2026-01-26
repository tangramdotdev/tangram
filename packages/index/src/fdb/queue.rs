use {
	super::{
		Index, Key, Kind, ObjectField, ObjectMetadataField, ObjectPropagateUpdate,
		ObjectPropagateUpdateFields, ObjectStoredField, ProcessField, ProcessMetadataField,
		ProcessPropagateUpdate, ProcessPropagateUpdateFields, ProcessStoredField, Update,
	},
	crate::ProcessObjectKind,
	foundationdb as fdb,
	foundationdb_tuple::{self as fdbt, Subspace},
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
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
		// Read a batch.
		let prefix = self.pack(&(Kind::UpdateVersion.to_i32().unwrap(),));
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
				let key: Key = self.unpack(entry.key())?;
				let Key::UpdateVersion { version, id } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((version, id))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		let mut count = 0;
		for (version, id) in entries {
			// Check if the update exists.
			let key = self.pack(&Key::Update { id: id.clone() });
			let update = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get update key"))?;

			// If the update key does not exist, delete the stale entry and continue.
			let Some(update) = update else {
				let key = self.pack(&Key::UpdateVersion { version, id });
				txn.clear(&key);
				count += 1;
				continue;
			};

			// Deserialize the update.
			let update = Update::deserialize(&update)?;

			// Process the update based on type.
			match (&id, &update) {
				(tg::Either::Left(id), Update::Put) => {
					self.update_object_put(txn, id, &version).await?;
				},
				(tg::Either::Right(id), Update::Put) => {
					self.update_process_put(txn, id, &version).await?;
				},
				(tg::Either::Left(id), Update::PropagateObject(propagate)) => {
					let fields = ObjectPropagateUpdateFields::from_bits_truncate(propagate.fields);
					self.update_object_propagate(txn, id, fields, &version)
						.await?;
				},
				(tg::Either::Right(id), Update::PropagateProcess(propagate)) => {
					let fields = ProcessPropagateUpdateFields::from_bits_truncate(propagate.fields);
					self.update_process_propagate(txn, id, fields, &version)
						.await?;
				},
				_ => {
					return Err(tg::error!("mismatched update type and id"));
				},
			}

			// Delete both the update and update version keys.
			txn.clear(&key);
			let key = self.pack(&Key::UpdateVersion { version, id });
			txn.clear(&key);

			count += 1;
		}

		Ok(count)
	}

	async fn update_object_put(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
		version: &fdbt::Versionstamp,
	) -> tg::Result<()> {
		// Recompute all fields from children.
		let updated = self
			.update_recompute_object_fields(txn, id, ObjectPropagateUpdateFields::ALL)
			.await?;

		// Enqueue parent updates for any fields that became set.
		if !updated.is_empty() {
			self.update_enqueue_object_parent_updates(txn, id, updated, version)
				.await?;
		}

		Ok(())
	}

	async fn update_object_propagate(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
		fields: ObjectPropagateUpdateFields,
		version: &fdbt::Versionstamp,
	) -> tg::Result<()> {
		// Recompute only the fields fields from children.
		let updated = self.update_recompute_object_fields(txn, id, fields).await?;

		// Enqueue parent updates for any fields that became set.
		if !updated.is_empty() {
			self.update_enqueue_object_parent_updates(txn, id, updated, version)
				.await?;
		}

		Ok(())
	}

	async fn update_process_put(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		version: &fdbt::Versionstamp,
	) -> tg::Result<()> {
		// Recompute all fields from children.
		let updated = self
			.update_recompute_process_fields(txn, id, ProcessPropagateUpdateFields::ALL)
			.await?;

		// Enqueue parent updates for any fields that became set.
		if !updated.is_empty() {
			self.update_enqueue_process_parent_updates(txn, id, updated, version)
				.await?;
		}

		Ok(())
	}

	async fn update_process_propagate(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		fields: ProcessPropagateUpdateFields,
		version: &fdbt::Versionstamp,
	) -> tg::Result<()> {
		// Recompute only the fields fields from children.
		let updated = self
			.update_recompute_process_fields(txn, id, fields)
			.await?;

		// Enqueue parent updates for any fields that became set.
		if !updated.is_empty() {
			self.update_enqueue_process_parent_updates(txn, id, updated, version)
				.await?;
		}

		Ok(())
	}

	async fn update_recompute_object_fields(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
		fields: ObjectPropagateUpdateFields,
	) -> tg::Result<ObjectPropagateUpdateFields> {
		let children = self.update_get_object_children(txn, id).await?;
		let mut updated = ObjectPropagateUpdateFields::empty();

		// Compute stored.subtree = all(children.stored.subtree). Vacuously true for leaf nodes.
		if fields.contains(ObjectPropagateUpdateFields::STORED_SUBTREE) {
			let current = self
				.update_get_object_field_bool(txn, id, ObjectStoredField::Subtree)
				.await?;
			if !current {
				let mut all = true;
				for child in &children {
					if !self
						.update_get_object_field_bool(txn, child, ObjectStoredField::Subtree)
						.await?
					{
						all = false;
						break;
					}
				}
				if all {
					self.update_set_object_field_bool(txn, id, ObjectStoredField::Subtree);
					updated |= ObjectPropagateUpdateFields::STORED_SUBTREE;
				}
			}
		}

		// Compute subtree_count = 1 + sum(children.subtree_count). For leaf nodes, this is 1.
		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_COUNT) {
			let current = self
				.update_get_object_field_u64(txn, id, ObjectMetadataField::SubtreeCount)
				.await?;
			if current.is_none() {
				let mut sum: u64 = 1;
				let mut all = true;
				for child in &children {
					if let Some(count) = self
						.update_get_object_field_u64(txn, child, ObjectMetadataField::SubtreeCount)
						.await?
					{
						sum = sum.saturating_add(count);
					} else {
						all = false;
						break;
					}
				}
				if all {
					self.update_set_object_field_u64(
						txn,
						id,
						ObjectMetadataField::SubtreeCount,
						sum,
					);
					updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_COUNT;
				}
			}
		}

		// Compute subtree_depth = 1 + max(children.subtree_depth). For leaf nodes, this is 1.
		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_DEPTH) {
			let current = self
				.update_get_object_field_u64(txn, id, ObjectMetadataField::SubtreeDepth)
				.await?;
			if current.is_none() {
				let mut max: u64 = 0;
				let mut all = true;
				for child in &children {
					if let Some(depth) = self
						.update_get_object_field_u64(txn, child, ObjectMetadataField::SubtreeDepth)
						.await?
					{
						max = max.max(depth);
					} else {
						all = false;
						break;
					}
				}
				if all {
					self.update_set_object_field_u64(
						txn,
						id,
						ObjectMetadataField::SubtreeDepth,
						1 + max,
					);
					updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_DEPTH;
				}
			}
		}

		// Compute subtree_size = node_size + sum(children.subtree_size). For leaf nodes, this equals node_size.
		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SIZE) {
			let current = self
				.update_get_object_field_u64(txn, id, ObjectMetadataField::SubtreeSize)
				.await?;
			if current.is_none() {
				let node = self
					.update_get_object_field_u64(txn, id, ObjectMetadataField::NodeSize)
					.await?
					.ok_or_else(|| tg::error!("node size is not set"))?;
				let mut sum: u64 = node;
				let mut all = true;
				for child in &children {
					if let Some(size) = self
						.update_get_object_field_u64(txn, child, ObjectMetadataField::SubtreeSize)
						.await?
					{
						sum = sum.saturating_add(size);
					} else {
						all = false;
						break;
					}
				}
				if all {
					self.update_set_object_field_u64(
						txn,
						id,
						ObjectMetadataField::SubtreeSize,
						sum,
					);
					updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_SIZE;
				}
			}
		}

		// Compute subtree_solvable = node_solvable OR any(children.subtree_solvable). For leaf nodes, this equals node_solvable.
		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVABLE) {
			let current = self
				.update_get_object_field_bool(txn, id, ObjectMetadataField::SubtreeSolvable)
				.await?;
			if !current {
				let node = self
					.update_get_object_field_bool(txn, id, ObjectMetadataField::NodeSolvable)
					.await?;
				if node {
					self.update_set_object_field_bool(
						txn,
						id,
						ObjectMetadataField::SubtreeSolvable,
					);
					updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVABLE;
				} else {
					for child in &children {
						if self
							.update_get_object_field_bool(
								txn,
								child,
								ObjectMetadataField::SubtreeSolvable,
							)
							.await?
						{
							self.update_set_object_field_bool(
								txn,
								id,
								ObjectMetadataField::SubtreeSolvable,
							);
							updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVABLE;
							break;
						}
					}
				}
			}
		}

		// Compute subtree_solved = node_solved AND all(children.subtree_solved). For leaf nodes, this equals node_solved.
		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVED) {
			let current = self
				.update_get_object_field_bool(txn, id, ObjectMetadataField::SubtreeSolved)
				.await?;
			if !current {
				let node = self
					.update_get_object_field_bool(txn, id, ObjectMetadataField::NodeSolved)
					.await?;
				if node {
					let mut all = true;
					for child in &children {
						if !self
							.update_get_object_field_bool(
								txn,
								child,
								ObjectMetadataField::SubtreeSolved,
							)
							.await?
						{
							all = false;
							break;
						}
					}
					if all {
						self.update_set_object_field_bool(
							txn,
							id,
							ObjectMetadataField::SubtreeSolved,
						);
						updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVED;
					}
				}
			}
		}

		Ok(updated)
	}

	async fn update_recompute_process_fields(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		fields: ProcessPropagateUpdateFields,
	) -> tg::Result<ProcessPropagateUpdateFields> {
		let mut updated = ProcessPropagateUpdateFields::empty();

		updated |= self
			.update_recompute_process_node_stored_fields(txn, id, fields)
			.await?;

		updated |= self
			.update_recompute_process_node_metadata_fields(txn, id, fields)
			.await?;

		updated |= self
			.update_recompute_process_subtree_stored_fields(txn, id, fields)
			.await?;

		updated |= self
			.update_recompute_process_subtree_metadata_fields(txn, id, fields)
			.await?;

		Ok(updated)
	}

	async fn update_recompute_process_node_stored_fields(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		fields: ProcessPropagateUpdateFields,
	) -> tg::Result<ProcessPropagateUpdateFields> {
		let mut updated = ProcessPropagateUpdateFields::empty();

		let objects = self.update_get_process_objects(txn, id).await?;

		for (object, kind) in &objects {
			let field = match kind {
				ProcessObjectKind::Command => ProcessStoredField::NodeCommand,
				ProcessObjectKind::Error => ProcessStoredField::NodeError,
				ProcessObjectKind::Log => ProcessStoredField::NodeLog,
				ProcessObjectKind::Output => ProcessStoredField::NodeOutput,
			};

			let flag = match kind {
				ProcessObjectKind::Command => ProcessPropagateUpdateFields::STORED_NODE_COMMAND,
				ProcessObjectKind::Error => ProcessPropagateUpdateFields::STORED_NODE_ERROR,
				ProcessObjectKind::Log => ProcessPropagateUpdateFields::STORED_NODE_LOG,
				ProcessObjectKind::Output => ProcessPropagateUpdateFields::STORED_NODE_OUTPUT,
			};

			if fields.contains(flag) {
				let current = self.update_get_process_field_bool(txn, id, field).await?;
				if !current {
					let stored = self
						.update_get_object_field_bool(txn, object, ObjectStoredField::Subtree)
						.await?;
					if stored {
						self.update_set_process_field_bool(txn, id, field);
						updated |= flag;
					}
				}
			}
		}

		Ok(updated)
	}

	async fn update_recompute_process_node_metadata_fields(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		fields: ProcessPropagateUpdateFields,
	) -> tg::Result<ProcessPropagateUpdateFields> {
		let mut updated = ProcessPropagateUpdateFields::empty();

		let objects = self.update_get_process_objects(txn, id).await?;

		for (object, kind) in &objects {
			let mappings: &[(
				ProcessPropagateUpdateFields,
				ProcessMetadataField,
				ObjectMetadataField,
			)] = match kind {
				ProcessObjectKind::Command => &[
					(
						ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_COUNT,
						ProcessMetadataField::NodeCommandCount,
						ObjectMetadataField::SubtreeCount,
					),
					(
						ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_DEPTH,
						ProcessMetadataField::NodeCommandDepth,
						ObjectMetadataField::SubtreeDepth,
					),
					(
						ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SIZE,
						ProcessMetadataField::NodeCommandSize,
						ObjectMetadataField::SubtreeSize,
					),
				],
				ProcessObjectKind::Error => &[
					(
						ProcessPropagateUpdateFields::METADATA_NODE_ERROR_COUNT,
						ProcessMetadataField::NodeErrorCount,
						ObjectMetadataField::SubtreeCount,
					),
					(
						ProcessPropagateUpdateFields::METADATA_NODE_ERROR_DEPTH,
						ProcessMetadataField::NodeErrorDepth,
						ObjectMetadataField::SubtreeDepth,
					),
					(
						ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SIZE,
						ProcessMetadataField::NodeErrorSize,
						ObjectMetadataField::SubtreeSize,
					),
				],
				ProcessObjectKind::Log => &[
					(
						ProcessPropagateUpdateFields::METADATA_NODE_LOG_COUNT,
						ProcessMetadataField::NodeLogCount,
						ObjectMetadataField::SubtreeCount,
					),
					(
						ProcessPropagateUpdateFields::METADATA_NODE_LOG_DEPTH,
						ProcessMetadataField::NodeLogDepth,
						ObjectMetadataField::SubtreeDepth,
					),
					(
						ProcessPropagateUpdateFields::METADATA_NODE_LOG_SIZE,
						ProcessMetadataField::NodeLogSize,
						ObjectMetadataField::SubtreeSize,
					),
				],
				ProcessObjectKind::Output => &[
					(
						ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_COUNT,
						ProcessMetadataField::NodeOutputCount,
						ObjectMetadataField::SubtreeCount,
					),
					(
						ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_DEPTH,
						ProcessMetadataField::NodeOutputDepth,
						ObjectMetadataField::SubtreeDepth,
					),
					(
						ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SIZE,
						ProcessMetadataField::NodeOutputSize,
						ObjectMetadataField::SubtreeSize,
					),
				],
			};

			for (flag, process_field, object_field) in mappings {
				if fields.contains(*flag) {
					let current = self
						.update_get_process_field_u64(txn, id, *process_field)
						.await?;
					if current.is_none()
						&& let Some(value) = self
							.update_get_object_field_u64(txn, object, *object_field)
							.await?
					{
						self.update_set_process_field_u64(txn, id, *process_field, value);
						updated |= *flag;
					}
				}
			}

			let solvable: (
				ProcessPropagateUpdateFields,
				ProcessMetadataField,
				ObjectMetadataField,
			) = match kind {
				ProcessObjectKind::Command => (
					ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVABLE,
					ProcessMetadataField::NodeCommandSolvable,
					ObjectMetadataField::SubtreeSolvable,
				),
				ProcessObjectKind::Error => (
					ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVABLE,
					ProcessMetadataField::NodeErrorSolvable,
					ObjectMetadataField::SubtreeSolvable,
				),
				ProcessObjectKind::Log => (
					ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVABLE,
					ProcessMetadataField::NodeLogSolvable,
					ObjectMetadataField::SubtreeSolvable,
				),
				ProcessObjectKind::Output => (
					ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVABLE,
					ProcessMetadataField::NodeOutputSolvable,
					ObjectMetadataField::SubtreeSolvable,
				),
			};

			if fields.contains(solvable.0) {
				let current = self
					.update_get_process_field_bool(txn, id, solvable.1)
					.await?;
				if !current {
					let value = self
						.update_get_object_field_bool(txn, object, solvable.2)
						.await?;
					if value {
						self.update_set_process_field_bool(txn, id, solvable.1);
						updated |= solvable.0;
					}
				}
			}

			let solved: (
				ProcessPropagateUpdateFields,
				ProcessMetadataField,
				ObjectMetadataField,
			) = match kind {
				ProcessObjectKind::Command => (
					ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVED,
					ProcessMetadataField::NodeCommandSolved,
					ObjectMetadataField::SubtreeSolved,
				),
				ProcessObjectKind::Error => (
					ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVED,
					ProcessMetadataField::NodeErrorSolved,
					ObjectMetadataField::SubtreeSolved,
				),
				ProcessObjectKind::Log => (
					ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVED,
					ProcessMetadataField::NodeLogSolved,
					ObjectMetadataField::SubtreeSolved,
				),
				ProcessObjectKind::Output => (
					ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVED,
					ProcessMetadataField::NodeOutputSolved,
					ObjectMetadataField::SubtreeSolved,
				),
			};

			if fields.contains(solved.0) {
				let current = self
					.update_get_process_field_bool(txn, id, solved.1)
					.await?;
				if !current {
					let value = self
						.update_get_object_field_bool(txn, object, solved.2)
						.await?;
					if value {
						self.update_set_process_field_bool(txn, id, solved.1);
						updated |= solved.0;
					}
				}
			}
		}

		Ok(updated)
	}

	async fn update_recompute_process_subtree_stored_fields(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		fields: ProcessPropagateUpdateFields,
	) -> tg::Result<ProcessPropagateUpdateFields> {
		let mut updated = ProcessPropagateUpdateFields::empty();

		let children = self.update_get_process_children(txn, id).await?;

		// Compute stored.subtree = all(children.stored.subtree). Vacuously true for leaf processes.
		if fields.contains(ProcessPropagateUpdateFields::STORED_SUBTREE) {
			let current = self
				.update_get_process_field_bool(txn, id, ProcessStoredField::Subtree)
				.await?;
			if !current {
				let mut all = true;
				for child in &children {
					if !self
						.update_get_process_field_bool(txn, child, ProcessStoredField::Subtree)
						.await?
					{
						all = false;
						break;
					}
				}
				if all {
					self.update_set_process_field_bool(txn, id, ProcessStoredField::Subtree);
					updated |= ProcessPropagateUpdateFields::STORED_SUBTREE;
				}
			}
		}

		let mappings: &[(
			ProcessPropagateUpdateFields,
			ProcessStoredField,
			ProcessStoredField,
		)] = &[
			(
				ProcessPropagateUpdateFields::STORED_SUBTREE_COMMAND,
				ProcessStoredField::SubtreeCommand,
				ProcessStoredField::NodeCommand,
			),
			(
				ProcessPropagateUpdateFields::STORED_SUBTREE_ERROR,
				ProcessStoredField::SubtreeError,
				ProcessStoredField::NodeError,
			),
			(
				ProcessPropagateUpdateFields::STORED_SUBTREE_LOG,
				ProcessStoredField::SubtreeLog,
				ProcessStoredField::NodeLog,
			),
			(
				ProcessPropagateUpdateFields::STORED_SUBTREE_OUTPUT,
				ProcessStoredField::SubtreeOutput,
				ProcessStoredField::NodeOutput,
			),
		];

		for (flag, subtree, node) in mappings {
			if fields.contains(*flag) {
				let current = self
					.update_get_process_field_bool(txn, id, *subtree)
					.await?;
				if !current {
					let stored = self.update_get_process_field_bool(txn, id, *node).await?;
					if stored {
						let mut all = true;
						for child in &children {
							if !self
								.update_get_process_field_bool(txn, child, *subtree)
								.await?
							{
								all = false;
								break;
							}
						}
						if all {
							self.update_set_process_field_bool(txn, id, *subtree);
							updated |= *flag;
						}
					}
				}
			}
		}

		Ok(updated)
	}

	async fn update_recompute_process_subtree_metadata_fields(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		fields: ProcessPropagateUpdateFields,
	) -> tg::Result<ProcessPropagateUpdateFields> {
		let mut updated = ProcessPropagateUpdateFields::empty();

		let children = self.update_get_process_children(txn, id).await?;

		// Compute subtree_count = 1 + sum(children.subtree_count). For leaf processes, this is 1.
		if fields.contains(ProcessPropagateUpdateFields::METADATA_SUBTREE_COUNT) {
			let current = self
				.update_get_process_field_u64(txn, id, ProcessMetadataField::SubtreeCount)
				.await?;
			if current.is_none() {
				let mut sum: u64 = 1;
				let mut all = true;
				for child in &children {
					if let Some(count) = self
						.update_get_process_field_u64(
							txn,
							child,
							ProcessMetadataField::SubtreeCount,
						)
						.await?
					{
						sum = sum.saturating_add(count);
					} else {
						all = false;
						break;
					}
				}
				if all {
					self.update_set_process_field_u64(
						txn,
						id,
						ProcessMetadataField::SubtreeCount,
						sum,
					);
					updated |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COUNT;
				}
			}
		}

		let mappings: &[(
			ProcessPropagateUpdateFields,
			ProcessMetadataField,
			ProcessMetadataField,
		)] = &[
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_COUNT,
				ProcessMetadataField::SubtreeCommandCount,
				ProcessMetadataField::NodeCommandCount,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_DEPTH,
				ProcessMetadataField::SubtreeCommandDepth,
				ProcessMetadataField::NodeCommandDepth,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SIZE,
				ProcessMetadataField::SubtreeCommandSize,
				ProcessMetadataField::NodeCommandSize,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_COUNT,
				ProcessMetadataField::SubtreeErrorCount,
				ProcessMetadataField::NodeErrorCount,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_DEPTH,
				ProcessMetadataField::SubtreeErrorDepth,
				ProcessMetadataField::NodeErrorDepth,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SIZE,
				ProcessMetadataField::SubtreeErrorSize,
				ProcessMetadataField::NodeErrorSize,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_COUNT,
				ProcessMetadataField::SubtreeLogCount,
				ProcessMetadataField::NodeLogCount,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_DEPTH,
				ProcessMetadataField::SubtreeLogDepth,
				ProcessMetadataField::NodeLogDepth,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SIZE,
				ProcessMetadataField::SubtreeLogSize,
				ProcessMetadataField::NodeLogSize,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_COUNT,
				ProcessMetadataField::SubtreeOutputCount,
				ProcessMetadataField::NodeOutputCount,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_DEPTH,
				ProcessMetadataField::SubtreeOutputDepth,
				ProcessMetadataField::NodeOutputDepth,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SIZE,
				ProcessMetadataField::SubtreeOutputSize,
				ProcessMetadataField::NodeOutputSize,
			),
		];

		for (flag, subtree, node) in mappings {
			if fields.contains(*flag) {
				let current = self.update_get_process_field_u64(txn, id, *subtree).await?;
				if current.is_none() {
					let node_value = self.update_get_process_field_u64(txn, id, *node).await?;
					let is_command = matches!(
						subtree,
						ProcessMetadataField::SubtreeCommandCount
							| ProcessMetadataField::SubtreeCommandDepth
							| ProcessMetadataField::SubtreeCommandSize
					);
					let is_depth = matches!(
						subtree,
						ProcessMetadataField::SubtreeCommandDepth
							| ProcessMetadataField::SubtreeErrorDepth
							| ProcessMetadataField::SubtreeLogDepth
							| ProcessMetadataField::SubtreeOutputDepth
					);

					if is_command && node_value.is_none() {
						return Err(tg::error!("process command metadata is not set"));
					}

					let mut all = node_value.is_some() || children.is_empty();
					let mut result = node_value.unwrap_or(0);

					for child in &children {
						if let Some(value) = self
							.update_get_process_field_u64(txn, child, *subtree)
							.await?
						{
							if is_depth {
								result = result.max(value);
							} else {
								result = result.saturating_add(value);
							}
						} else {
							all = false;
							break;
						}
					}

					if all {
						self.update_set_process_field_u64(txn, id, *subtree, result);
						updated |= *flag;
					}
				}
			}
		}

		let solvable_mappings: &[(
			ProcessPropagateUpdateFields,
			ProcessMetadataField,
			ProcessMetadataField,
		)] = &[
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SOLVABLE,
				ProcessMetadataField::SubtreeCommandSolvable,
				ProcessMetadataField::NodeCommandSolvable,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVABLE,
				ProcessMetadataField::SubtreeErrorSolvable,
				ProcessMetadataField::NodeErrorSolvable,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVABLE,
				ProcessMetadataField::SubtreeLogSolvable,
				ProcessMetadataField::NodeLogSolvable,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVABLE,
				ProcessMetadataField::SubtreeOutputSolvable,
				ProcessMetadataField::NodeOutputSolvable,
			),
		];

		for (flag, subtree, node) in solvable_mappings {
			if fields.contains(*flag) {
				let current = self
					.update_get_process_field_bool(txn, id, *subtree)
					.await?;
				if !current {
					let solvable = self.update_get_process_field_bool(txn, id, *node).await?;
					if solvable {
						self.update_set_process_field_bool(txn, id, *subtree);
						updated |= *flag;
					} else {
						for child in &children {
							if self
								.update_get_process_field_bool(txn, child, *subtree)
								.await?
							{
								self.update_set_process_field_bool(txn, id, *subtree);
								updated |= *flag;
								break;
							}
						}
					}
				}
			}
		}

		let solved_mappings: &[(
			ProcessPropagateUpdateFields,
			ProcessMetadataField,
			ProcessMetadataField,
		)] = &[
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SOLVED,
				ProcessMetadataField::SubtreeCommandSolved,
				ProcessMetadataField::NodeCommandSolved,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVED,
				ProcessMetadataField::SubtreeErrorSolved,
				ProcessMetadataField::NodeErrorSolved,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVED,
				ProcessMetadataField::SubtreeLogSolved,
				ProcessMetadataField::NodeLogSolved,
			),
			(
				ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVED,
				ProcessMetadataField::SubtreeOutputSolved,
				ProcessMetadataField::NodeOutputSolved,
			),
		];

		for (flag, subtree, node) in solved_mappings {
			if fields.contains(*flag) {
				let current = self
					.update_get_process_field_bool(txn, id, *subtree)
					.await?;
				if !current {
					let solved = self.update_get_process_field_bool(txn, id, *node).await?;
					if solved {
						let mut all = true;
						for child in &children {
							if !self
								.update_get_process_field_bool(txn, child, *subtree)
								.await?
							{
								all = false;
								break;
							}
						}
						if all {
							self.update_set_process_field_bool(txn, id, *subtree);
							updated |= *flag;
						}
					}
				}
			}
		}

		Ok(updated)
	}

	async fn update_enqueue_object_parent_updates(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
		fields: ObjectPropagateUpdateFields,
		version: &fdbt::Versionstamp,
	) -> tg::Result<()> {
		let parents = self.update_get_object_parents(txn, id).await?;
		for parent in parents {
			self.update_enqueue_object_propagate(txn, &parent, fields, version)
				.await?;
		}

		let refs = self.update_get_object_process_refs(txn, id).await?;
		for (process, kind) in refs {
			let fields = Self::update_object_fields_to_process_fields(fields, kind);
			if !fields.is_empty() {
				self.update_enqueue_process_propagate(txn, &process, fields, version)
					.await?;
			}
		}

		Ok(())
	}

	async fn update_enqueue_process_parent_updates(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		fields: ProcessPropagateUpdateFields,
		version: &fdbt::Versionstamp,
	) -> tg::Result<()> {
		let parents = self.update_get_process_parents(txn, id).await?;
		for parent in parents {
			self.update_enqueue_process_propagate(txn, &parent, fields, version)
				.await?;
		}
		Ok(())
	}

	async fn update_enqueue_object_propagate(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
		fields: ObjectPropagateUpdateFields,
		version: &fdbt::Versionstamp,
	) -> tg::Result<()> {
		let either = tg::Either::Left(id.clone());
		let key = self.pack(&Key::Update { id: either.clone() });

		let existing = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get existing update"))?;

		if let Some(bytes) = existing {
			let update = Update::deserialize(&bytes)?;
			match update {
				Update::Put => {},
				Update::PropagateObject(mut propagate) => {
					let existing =
						ObjectPropagateUpdateFields::from_bits_truncate(propagate.fields);
					propagate.fields = (existing | fields).bits();
					let value = Update::PropagateObject(propagate).serialize()?;
					txn.set(&key, &value);
				},
				Update::PropagateProcess(_) => {
					return Err(tg::error!("unexpected propagate process for object id"));
				},
			}
		} else {
			let update = Update::PropagateObject(ObjectPropagateUpdate {
				fields: fields.bits(),
			});
			let value = update.serialize()?;
			txn.set(&key, &value);

			let key = self.pack(&Key::UpdateVersion {
				version: version.clone(),
				id: either,
			});
			txn.set(&key, &value);
		}

		Ok(())
	}

	async fn update_enqueue_process_propagate(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		fields: ProcessPropagateUpdateFields,
		version: &fdbt::Versionstamp,
	) -> tg::Result<()> {
		let either = tg::Either::Right(id.clone());
		let key = self.pack(&Key::Update { id: either.clone() });

		let existing = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get existing update"))?;

		if let Some(bytes) = existing {
			let update = Update::deserialize(&bytes)?;
			match update {
				Update::Put => {},
				Update::PropagateObject(_) => {
					return Err(tg::error!("unexpected propagate object for process id"));
				},
				Update::PropagateProcess(mut propagate) => {
					let existing =
						ProcessPropagateUpdateFields::from_bits_truncate(propagate.fields);
					propagate.fields = (existing | fields).bits();
					let value = Update::PropagateProcess(propagate).serialize()?;
					txn.set(&key, &value);
				},
			}
		} else {
			let update = Update::PropagateProcess(ProcessPropagateUpdate {
				fields: fields.bits(),
			});
			let value = update.serialize()?;
			txn.set(&key, &value);

			let key = self.pack(&Key::UpdateVersion {
				version: version.clone(),
				id: either,
			});
			txn.set(&key, &value);
		}

		Ok(())
	}

	fn update_object_fields_to_process_fields(
		fields: ObjectPropagateUpdateFields,
		kind: ProcessObjectKind,
	) -> ProcessPropagateUpdateFields {
		let mut result = ProcessPropagateUpdateFields::empty();

		if fields.contains(ObjectPropagateUpdateFields::STORED_SUBTREE) {
			result |= match kind {
				ProcessObjectKind::Command => ProcessPropagateUpdateFields::STORED_NODE_COMMAND,
				ProcessObjectKind::Error => ProcessPropagateUpdateFields::STORED_NODE_ERROR,
				ProcessObjectKind::Log => ProcessPropagateUpdateFields::STORED_NODE_LOG,
				ProcessObjectKind::Output => ProcessPropagateUpdateFields::STORED_NODE_OUTPUT,
			};
		}

		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_COUNT) {
			result |= match kind {
				ProcessObjectKind::Command => {
					ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_COUNT
				},
				ProcessObjectKind::Error => ProcessPropagateUpdateFields::METADATA_NODE_ERROR_COUNT,
				ProcessObjectKind::Log => ProcessPropagateUpdateFields::METADATA_NODE_LOG_COUNT,
				ProcessObjectKind::Output => {
					ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_COUNT
				},
			};
		}

		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_DEPTH) {
			result |= match kind {
				ProcessObjectKind::Command => {
					ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_DEPTH
				},
				ProcessObjectKind::Error => ProcessPropagateUpdateFields::METADATA_NODE_ERROR_DEPTH,
				ProcessObjectKind::Log => ProcessPropagateUpdateFields::METADATA_NODE_LOG_DEPTH,
				ProcessObjectKind::Output => {
					ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_DEPTH
				},
			};
		}

		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SIZE) {
			result |= match kind {
				ProcessObjectKind::Command => {
					ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SIZE
				},
				ProcessObjectKind::Error => ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SIZE,
				ProcessObjectKind::Log => ProcessPropagateUpdateFields::METADATA_NODE_LOG_SIZE,
				ProcessObjectKind::Output => {
					ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SIZE
				},
			};
		}

		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVABLE) {
			result |= match kind {
				ProcessObjectKind::Command => {
					ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVABLE
				},
				ProcessObjectKind::Error => {
					ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVABLE
				},
				ProcessObjectKind::Log => ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVABLE,
				ProcessObjectKind::Output => {
					ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVABLE
				},
			};
		}

		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVED) {
			result |= match kind {
				ProcessObjectKind::Command => {
					ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVED
				},
				ProcessObjectKind::Error => {
					ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVED
				},
				ProcessObjectKind::Log => ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVED,
				ProcessObjectKind::Output => {
					ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVED
				},
			};
		}

		result
	}

	async fn update_get_object_children(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
	) -> tg::Result<Vec<tg::object::Id>> {
		let bytes = id.to_bytes();
		let prefix = self.pack(&(Kind::ObjectChild.to_i32().unwrap(), bytes.as_ref()));
		let subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object children"))?;

		let mut children = Vec::new();
		for entry in &entries {
			let key: Key = self.unpack(entry.key())?;
			if let Key::ObjectChild { child, .. } = key {
				children.push(child);
			}
		}

		Ok(children)
	}

	async fn update_get_object_parents(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
	) -> tg::Result<Vec<tg::object::Id>> {
		let bytes = id.to_bytes();
		let prefix = self.pack(&(Kind::ChildObject.to_i32().unwrap(), bytes.as_ref()));
		let subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object parents"))?;

		let mut parents = Vec::new();
		for entry in &entries {
			let key: Key = self.unpack(entry.key())?;
			if let Key::ChildObject { object, .. } = key {
				parents.push(object);
			}
		}

		Ok(parents)
	}

	async fn update_get_object_process_refs(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, ProcessObjectKind)>> {
		let bytes = id.to_bytes();
		let prefix = self.pack(&(Kind::ObjectProcess.to_i32().unwrap(), bytes.as_ref()));
		let subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object process refs"))?;

		let mut refs = Vec::new();
		for entry in &entries {
			let key: Key = self.unpack(entry.key())?;
			if let Key::ObjectProcess { process, kind, .. } = key {
				refs.push((process, kind));
			}
		}

		Ok(refs)
	}

	async fn update_get_process_children(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::process::Id>> {
		let bytes = id.to_bytes();
		let prefix = self.pack(&(Kind::ProcessChild.to_i32().unwrap(), bytes.as_ref()));
		let subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process children"))?;

		let mut children = Vec::new();
		for entry in &entries {
			let key: Key = self.unpack(entry.key())?;
			if let Key::ProcessChild { child, .. } = key {
				children.push(child);
			}
		}

		Ok(children)
	}

	async fn update_get_process_parents(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::process::Id>> {
		let bytes = id.to_bytes();
		let prefix = self.pack(&(Kind::ChildProcess.to_i32().unwrap(), bytes.as_ref()));
		let subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process parents"))?;

		let mut parents = Vec::new();
		for entry in &entries {
			let key: Key = self.unpack(entry.key())?;
			if let Key::ChildProcess { parent, .. } = key {
				parents.push(parent);
			}
		}

		Ok(parents)
	}

	async fn update_get_process_objects(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
	) -> tg::Result<Vec<(tg::object::Id, ProcessObjectKind)>> {
		let bytes = id.to_bytes();
		let prefix = self.pack(&(Kind::ProcessObject.to_i32().unwrap(), bytes.as_ref()));
		let subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process objects"))?;

		let mut objects = Vec::new();
		for entry in &entries {
			let key: Key = self.unpack(entry.key())?;
			if let Key::ProcessObject { object, kind, .. } = key {
				objects.push((object, kind));
			}
		}

		Ok(objects)
	}

	async fn update_get_object_field_bool<F: Into<ObjectField>>(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
		field: F,
	) -> tg::Result<bool> {
		let key = self.pack(&Key::Object {
			id: id.clone(),
			field: field.into(),
		});
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object field"))?;
		Ok(value.is_some())
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
			Some(bytes) if bytes.len() == 8 => {
				Ok(Some(u64::from_le_bytes(bytes[..].try_into().unwrap())))
			},
			Some(_) => Err(tg::error!("invalid u64 value")),
			None => Ok(None),
		}
	}

	fn update_set_object_field_bool<F: Into<ObjectField>>(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
		field: F,
	) {
		let key = self.pack(&Key::Object {
			id: id.clone(),
			field: field.into(),
		});
		txn.set(&key, &[]);
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
		txn.set(&key, &value.to_le_bytes());
	}

	async fn update_get_process_field_bool<F: Into<ProcessField>>(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		field: F,
	) -> tg::Result<bool> {
		let key = self.pack(&Key::Process {
			id: id.clone(),
			field: field.into(),
		});
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process field"))?;
		Ok(value.is_some())
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
			Some(bytes) if bytes.len() == 8 => {
				Ok(Some(u64::from_le_bytes(bytes[..].try_into().unwrap())))
			},
			Some(_) => Err(tg::error!("invalid u64 value")),
			None => Ok(None),
		}
	}

	fn update_set_process_field_bool<F: Into<ProcessField>>(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		field: F,
	) {
		let key = self.pack(&Key::Process {
			id: id.clone(),
			field: field.into(),
		});
		txn.set(&key, &[]);
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
		txn.set(&key, &value.to_le_bytes());
	}
}
