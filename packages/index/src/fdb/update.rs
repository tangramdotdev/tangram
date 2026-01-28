use {
	super::{
		Index, Key, Kind, ObjectField, ObjectMetadataField, ObjectPropagateUpdate,
		ObjectPropagateUpdateFields, ObjectStoredField, ProcessField, ProcessMetadataField,
		ProcessPropagateUpdate, ProcessPropagateUpdateFields, ProcessStoredField, PropagateUpdate,
		Update,
	},
	crate::ProcessObjectKind,
	foundationdb as fdb,
	foundationdb_tuple::{self as fdbt, Subspace, TuplePack as _},
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

		// Convert the transaction_id to a versionstamp with max batch order (0xFFFF) to include
		// all entries with transaction version <= the given version, regardless of batch order.
		let mut stamp_bytes = [0u8; 10];
		stamp_bytes[0..8].copy_from_slice(&transaction_id.to_be_bytes());
		stamp_bytes[8..10].copy_from_slice(&0xFFFFu16.to_be_bytes());
		let end_versionstamp = fdbt::Versionstamp::complete(stamp_bytes, 0);

		// Scan from the beginning of UpdateVersion to the given transaction_id.
		let prefix = self.pack(&(Kind::UpdateVersion.to_i32().unwrap(),));

		// Pack the end key with the versionstamp.
		let mut end = prefix.clone();
		end.extend_from_slice(&(end_versionstamp,).pack_to_vec());

		let subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			begin: fdb::KeySelector::first_greater_or_equal(subspace.range().0),
			end: fdb::KeySelector::first_greater_or_equal(end),
			limit: Some(1),
			mode: fdb::options::StreamingMode::WantAll,
			..Default::default()
		};

		// Check if there are any entries.
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if updates are finished"))?;

		Ok(entries.is_empty())
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
				let value = entry.value().to_vec();
				Ok((version, id, value))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		let mut count = 0;
		for (version, id, version_value) in entries {
			// Check if the update exists.
			let key = self.pack(&Key::Update { id: id.clone() });
			let update_key_value = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get update key"))?;

			// Determine which update to process.
			let update = if let Some(update) = update_key_value {
				// Use the Update key value (may have been merged with additional fields).
				Update::deserialize(&update)?
			} else if !version_value.is_empty() {
				// The Update key was deleted (Put was processed), but this UpdateVersion has
				// a Propagate that should still be processed.
				let version_update = Update::deserialize(&version_value)?;
				match version_update {
					Update::Put => {
						// Stale Put entry, delete and continue.
						let key = self.pack(&Key::UpdateVersion {
							version: version.clone(),
							id,
						});
						txn.clear(&key);
						count += 1;
						continue;
					},
					Update::Propagate(_) => {
						// Valid Propagate, process it.
						version_update
					},
				}
			} else {
				// Stale entry with no value, delete and continue.
				let key = self.pack(&Key::UpdateVersion {
					version: version.clone(),
					id,
				});
				txn.clear(&key);
				count += 1;
				continue;
			};

			// Process the update based on type.
			match (&id, &update) {
				(tg::Either::Left(id), Update::Put) => {
					self.update_object_put(txn, id, &version).await?;
				},
				(tg::Either::Right(id), Update::Put) => {
					self.update_process_put(txn, id, &version).await?;
				},
				(tg::Either::Left(id), Update::Propagate(PropagateUpdate::Object(propagate))) => {
					let fields = ObjectPropagateUpdateFields::from_bits_truncate(propagate.fields);
					self.update_object_propagate(txn, id, fields, &version)
						.await?;
				},
				(tg::Either::Right(id), Update::Propagate(PropagateUpdate::Process(propagate))) => {
					let fields = ProcessPropagateUpdateFields::from_bits_truncate(propagate.fields);
					self.update_process_propagate(txn, id, fields, &version)
						.await?;
				},
				_ => {
					return Err(tg::error!("mismatched update type and id"));
				},
			}

			// Delete both the update and update version keys.
			let key = self.pack(&Key::Update { id: id.clone() });
			txn.clear(&key);
			let key = self.pack(&Key::UpdateVersion {
				version: version.clone(),
				id,
			});
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

		// For Put updates, always enqueue parent updates (the object is new to the index).
		// This matches LMDB behavior and ensures the propagation chain is started even if
		// this object's children are not yet ready. The parents will be re-processed later
		// when this object's fields are computed via Propagate.
		self.update_enqueue_object_parent_updates(
			txn,
			id,
			if updated.is_empty() {
				ObjectPropagateUpdateFields::ALL
			} else {
				updated
			},
			version,
		)
		.await?;

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

		// For Put updates, always enqueue parent updates (the process is new to the index).
		// This matches LMDB behavior and ensures the propagation chain is started even if
		// this process's children are not yet ready. The parents will be re-processed later
		// when this process's fields are computed via Propagate.
		self.update_enqueue_process_parent_updates(
			txn,
			id,
			if updated.is_empty() {
				ProcessPropagateUpdateFields::ALL
			} else {
				updated
			},
			version,
		)
		.await?;

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
			if current != Some(true) {
				let mut all = true;
				for child in &children {
					if self
						.update_get_object_field_bool(txn, child, ObjectStoredField::Subtree)
						.await? != Some(true)
					{
						all = false;
						break;
					}
				}
				if all {
					self.update_set_object_field_bool(txn, id, ObjectStoredField::Subtree, true);
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
			if current.is_none() {
				let node_solvable = self
					.update_get_object_field_bool(txn, id, ObjectMetadataField::NodeSolvable)
					.await?
					.ok_or_else(|| tg::error!("node solvable is not set"))?;
				// Check children for any solvable and track if all are computed.
				let mut any_solvable = false;
				let mut all_computed = true;
				for child in &children {
					match self
						.update_get_object_field_bool(
							txn,
							child,
							ObjectMetadataField::SubtreeSolvable,
						)
						.await?
					{
						Some(true) => {
							any_solvable = true;
						},
						Some(false) => {},
						None => {
							all_computed = false;
						},
					}
				}
				if all_computed {
					self.update_set_object_field_bool(
						txn,
						id,
						ObjectMetadataField::SubtreeSolvable,
						node_solvable || any_solvable,
					);
					updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVABLE;
				}
			}
		}

		// Compute subtree_solved = node_solved AND all(children.subtree_solved). For leaf nodes, this equals node_solved.
		if fields.contains(ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVED) {
			let current = self
				.update_get_object_field_bool(txn, id, ObjectMetadataField::SubtreeSolved)
				.await?;
			if current.is_none() {
				let node_solved = self
					.update_get_object_field_bool(txn, id, ObjectMetadataField::NodeSolved)
					.await?
					.ok_or_else(|| tg::error!("node solved is not set"))?;
				// Check if all children are solved and track if all are computed.
				let mut all_solved = true;
				let mut all_computed = true;
				for child in &children {
					match self
						.update_get_object_field_bool(
							txn,
							child,
							ObjectMetadataField::SubtreeSolved,
						)
						.await?
					{
						Some(true) => {},
						Some(false) => {
							all_solved = false;
						},
						None => {
							all_computed = false;
							all_solved = false;
						},
					}
				}
				if all_computed {
					self.update_set_object_field_bool(
						txn,
						id,
						ObjectMetadataField::SubtreeSolved,
						node_solved && all_solved,
					);
					updated |= ObjectPropagateUpdateFields::METADATA_SUBTREE_SOLVED;
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
		let mut fields = fields;

		updated |= self
			.update_recompute_process_node_stored_fields(txn, id, fields)
			.await?;

		updated |= self
			.update_recompute_process_node_metadata_fields(txn, id, fields)
			.await?;

		// When node.* fields are updated, also trigger subtree.* recomputation.
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_COUNT) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_COUNT;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_DEPTH) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_DEPTH;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SIZE) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SIZE;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVABLE) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SOLVABLE;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_COMMAND_SOLVED) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_COMMAND_SOLVED;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_COUNT) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_COUNT;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_DEPTH) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_DEPTH;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SIZE) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SIZE;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVABLE) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVABLE;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVED) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_ERROR_SOLVED;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_COUNT) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_COUNT;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_DEPTH) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_DEPTH;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SIZE) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SIZE;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVABLE) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVABLE;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVED) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_LOG_SOLVED;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_COUNT) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_COUNT;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_DEPTH) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_DEPTH;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SIZE) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SIZE;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVABLE) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVABLE;
		}
		if updated.contains(ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVED) {
			fields |= ProcessPropagateUpdateFields::METADATA_SUBTREE_OUTPUT_SOLVED;
		}

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

		// Track which object kinds exist for this process.
		let mut has_error = false;
		let mut has_log = false;
		let mut has_output = false;

		for (object, kind) in &objects {
			match kind {
				ProcessObjectKind::Command => {},
				ProcessObjectKind::Error => has_error = true,
				ProcessObjectKind::Log => has_log = true,
				ProcessObjectKind::Output => has_output = true,
			}

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
				if current != Some(true) {
					let stored = self
						.update_get_object_field_bool(txn, object, ObjectStoredField::Subtree)
						.await?;
					if stored == Some(true) {
						self.update_set_process_field_bool(txn, id, field, true);
						updated |= flag;
					}
				}
			}
		}

		// For missing object kinds, set stored to true.
		let missing_kinds = [
			(
				has_error,
				ProcessStoredField::NodeError,
				ProcessPropagateUpdateFields::STORED_NODE_ERROR,
			),
			(
				has_log,
				ProcessStoredField::NodeLog,
				ProcessPropagateUpdateFields::STORED_NODE_LOG,
			),
			(
				has_output,
				ProcessStoredField::NodeOutput,
				ProcessPropagateUpdateFields::STORED_NODE_OUTPUT,
			),
		];

		for (has_kind, field, flag) in missing_kinds {
			if !has_kind && fields.contains(flag) {
				let current = self.update_get_process_field_bool(txn, id, field).await?;
				if current != Some(true) {
					self.update_set_process_field_bool(txn, id, field, true);
					updated |= flag;
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

		// Track which object kinds exist for this process.
		let mut has_error = false;
		let mut has_log = false;
		let mut has_output = false;

		for (object, kind) in &objects {
			match kind {
				ProcessObjectKind::Command => {},
				ProcessObjectKind::Error => has_error = true,
				ProcessObjectKind::Log => has_log = true,
				ProcessObjectKind::Output => has_output = true,
			}
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
				if current.is_none() {
					let value = self
						.update_get_object_field_bool(txn, object, solvable.2)
						.await?;
					if let Some(v) = value {
						self.update_set_process_field_bool(txn, id, solvable.1, v);
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
				if current.is_none() {
					let value = self
						.update_get_object_field_bool(txn, object, solved.2)
						.await?;
					if let Some(v) = value {
						self.update_set_process_field_bool(txn, id, solved.1, v);
						updated |= solved.0;
					}
				}
			}
		}

		// For missing object kinds, set count=0, depth=0, size=0.
		let missing_kinds: &[(
			bool,
			&[(ProcessPropagateUpdateFields, ProcessMetadataField)],
		)] = &[
			(
				has_error,
				&[
					(
						ProcessPropagateUpdateFields::METADATA_NODE_ERROR_COUNT,
						ProcessMetadataField::NodeErrorCount,
					),
					(
						ProcessPropagateUpdateFields::METADATA_NODE_ERROR_DEPTH,
						ProcessMetadataField::NodeErrorDepth,
					),
					(
						ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SIZE,
						ProcessMetadataField::NodeErrorSize,
					),
				],
			),
			(
				has_log,
				&[
					(
						ProcessPropagateUpdateFields::METADATA_NODE_LOG_COUNT,
						ProcessMetadataField::NodeLogCount,
					),
					(
						ProcessPropagateUpdateFields::METADATA_NODE_LOG_DEPTH,
						ProcessMetadataField::NodeLogDepth,
					),
					(
						ProcessPropagateUpdateFields::METADATA_NODE_LOG_SIZE,
						ProcessMetadataField::NodeLogSize,
					),
				],
			),
			(
				has_output,
				&[
					(
						ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_COUNT,
						ProcessMetadataField::NodeOutputCount,
					),
					(
						ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_DEPTH,
						ProcessMetadataField::NodeOutputDepth,
					),
					(
						ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SIZE,
						ProcessMetadataField::NodeOutputSize,
					),
				],
			),
		];

		for (has_kind, mappings) in missing_kinds {
			if !has_kind {
				for (flag, process_field) in *mappings {
					if fields.contains(*flag) {
						let current = self
							.update_get_process_field_u64(txn, id, *process_field)
							.await?;
						if current.is_none() {
							self.update_set_process_field_u64(txn, id, *process_field, 0);
							updated |= *flag;
						}
					}
				}
			}
		}

		// For missing object kinds, set solvable=false (no solvable references) and
		// solved=true (vacuously true - all 0 objects are solved).
		let missing_solvable: &[(bool, ProcessPropagateUpdateFields, ProcessMetadataField)] = &[
			(
				has_error,
				ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVABLE,
				ProcessMetadataField::NodeErrorSolvable,
			),
			(
				has_log,
				ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVABLE,
				ProcessMetadataField::NodeLogSolvable,
			),
			(
				has_output,
				ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVABLE,
				ProcessMetadataField::NodeOutputSolvable,
			),
		];

		for (has_kind, flag, field) in missing_solvable {
			if !*has_kind && fields.contains(*flag) {
				let current = self.update_get_process_field_bool(txn, id, *field).await?;
				if current.is_none() {
					self.update_set_process_field_bool(txn, id, *field, false);
					updated |= *flag;
				}
			}
		}

		let missing_solved: &[(bool, ProcessPropagateUpdateFields, ProcessMetadataField)] = &[
			(
				has_error,
				ProcessPropagateUpdateFields::METADATA_NODE_ERROR_SOLVED,
				ProcessMetadataField::NodeErrorSolved,
			),
			(
				has_log,
				ProcessPropagateUpdateFields::METADATA_NODE_LOG_SOLVED,
				ProcessMetadataField::NodeLogSolved,
			),
			(
				has_output,
				ProcessPropagateUpdateFields::METADATA_NODE_OUTPUT_SOLVED,
				ProcessMetadataField::NodeOutputSolved,
			),
		];

		for (has_kind, flag, field) in missing_solved {
			if !*has_kind && fields.contains(*flag) {
				let current = self.update_get_process_field_bool(txn, id, *field).await?;
				if current.is_none() {
					self.update_set_process_field_bool(txn, id, *field, true);
					updated |= *flag;
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
			if current != Some(true) {
				let mut all = true;
				for child in &children {
					if self
						.update_get_process_field_bool(txn, child, ProcessStoredField::Subtree)
						.await? != Some(true)
					{
						all = false;
						break;
					}
				}
				if all {
					self.update_set_process_field_bool(txn, id, ProcessStoredField::Subtree, true);
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
				if current != Some(true) {
					let stored = self.update_get_process_field_bool(txn, id, *node).await?;
					if stored == Some(true) {
						let mut all = true;
						for child in &children {
							if self
								.update_get_process_field_bool(txn, child, *subtree)
								.await? != Some(true)
							{
								all = false;
								break;
							}
						}
						if all {
							self.update_set_process_field_bool(txn, id, *subtree, true);
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
					let is_depth = matches!(
						subtree,
						ProcessMetadataField::SubtreeCommandDepth
							| ProcessMetadataField::SubtreeErrorDepth
							| ProcessMetadataField::SubtreeLogDepth
							| ProcessMetadataField::SubtreeOutputDepth
					);

					// We need node_value to be computed before we can compute subtree.
					let mut all = node_value.is_some();
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
				if current.is_none() {
					let node_value = self.update_get_process_field_bool(txn, id, *node).await?;
					// Check children for any solvable and track if all are computed.
					let mut any_solvable = false;
					let mut all_computed = node_value.is_some();
					for child in &children {
						match self
							.update_get_process_field_bool(txn, child, *subtree)
							.await?
						{
							Some(true) => {
								any_solvable = true;
							},
							Some(false) => {},
							None => {
								all_computed = false;
							},
						}
					}
					if all_computed {
						let node_solvable = node_value == Some(true);
						self.update_set_process_field_bool(
							txn,
							id,
							*subtree,
							node_solvable || any_solvable,
						);
						updated |= *flag;
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
				if current.is_none() {
					let node_value = self.update_get_process_field_bool(txn, id, *node).await?;
					if let Some(node_solved) = node_value {
						// Check if all children have their subtree.solved computed.
						let mut all_solved = true;
						let mut all_computed = true;
						for child in &children {
							match self
								.update_get_process_field_bool(txn, child, *subtree)
								.await?
							{
								Some(true) => {},
								Some(false) => {
									all_solved = false;
								},
								None => {
									all_computed = false;
								},
							}
						}
						if all_computed {
							// Compute: node.solved AND all(children.subtree.solved).
							let value = node_solved && all_solved;
							self.update_set_process_field_bool(txn, id, *subtree, value);
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

		let parents = self.update_get_object_process_parents(txn, id).await?;
		for (process, kind) in parents {
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
				Update::Put => {
					// Put will process all fields, but we still need to create an UpdateVersion
					// entry so this propagate is processed after the Put completes. The Put might
					// not be able to compute all fields if dependencies are not ready yet.
					let update =
						Update::Propagate(PropagateUpdate::Object(ObjectPropagateUpdate {
							fields: fields.bits(),
						}));
					let value = update.serialize()?;
					let key = self.pack(&Key::UpdateVersion {
						version: version.clone(),
						id: either,
					});
					txn.set(&key, &value);
				},
				Update::Propagate(PropagateUpdate::Object(mut propagate)) => {
					let existing =
						ObjectPropagateUpdateFields::from_bits_truncate(propagate.fields);
					propagate.fields = (existing | fields).bits();
					let value =
						Update::Propagate(PropagateUpdate::Object(propagate)).serialize()?;
					txn.set(&key, &value);
				},
				Update::Propagate(PropagateUpdate::Process(_)) => {
					return Err(tg::error!("unexpected propagate process for object id"));
				},
			}
		} else {
			let update = Update::Propagate(PropagateUpdate::Object(ObjectPropagateUpdate {
				fields: fields.bits(),
			}));
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
				Update::Put => {
					// Put will process all fields, but we still need to create an UpdateVersion
					// entry so this propagate is processed after the Put completes. The Put might
					// not be able to compute all fields if dependencies are not ready yet.
					let update =
						Update::Propagate(PropagateUpdate::Process(ProcessPropagateUpdate {
							fields: fields.bits(),
						}));
					let value = update.serialize()?;
					let key = self.pack(&Key::UpdateVersion {
						version: version.clone(),
						id: either,
					});
					txn.set(&key, &value);
				},
				Update::Propagate(PropagateUpdate::Object(_)) => {
					return Err(tg::error!("unexpected propagate object for process id"));
				},
				Update::Propagate(PropagateUpdate::Process(mut propagate)) => {
					let existing =
						ProcessPropagateUpdateFields::from_bits_truncate(propagate.fields);
					propagate.fields = (existing | fields).bits();
					let value =
						Update::Propagate(PropagateUpdate::Process(propagate)).serialize()?;
					txn.set(&key, &value);
				},
			}
		} else {
			let update = Update::Propagate(PropagateUpdate::Process(ProcessPropagateUpdate {
				fields: fields.bits(),
			}));
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

	async fn update_get_object_process_parents(
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
			.map_err(|source| tg::error!(!source, "failed to get object process parents"))?;

		let mut parents = Vec::new();
		for entry in &entries {
			let key: Key = self.unpack(entry.key())?;
			if let Key::ObjectProcess { process, kind, .. } = key {
				parents.push((process, kind));
			}
		}

		Ok(parents)
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
}
