use {
	super::{Db, Index, Key, Kind, Update},
	crate::{Object, Process, ProcessObjectKind},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	num_traits::{FromPrimitive as _, ToPrimitive as _},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn update_batch(&self, batch_size: usize) -> tg::Result<usize> {
		let env = self.env.clone();
		let db = self.db;
		tokio::task::spawn_blocking(move || {
			let mut transaction = env
				.write_txn()
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
			let count = Self::task_update_batch(&db, &mut transaction, batch_size)?;
			transaction
				.commit()
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
			Ok(count)
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}

	pub async fn get_transaction_id(&self) -> tg::Result<u128> {
		let env = self.env.clone();
		tokio::task::spawn_blocking(move || {
			let transaction = env
				.read_txn()
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
			Ok(transaction.id() as u128)
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}

	pub async fn get_queue_size(&self, transaction_id: u128) -> tg::Result<u64> {
		let env = self.env.clone();
		let db = self.db;
		tokio::task::spawn_blocking(move || {
			let transaction = env
				.read_txn()
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
			Self::task_get_queue_size(&db, &transaction, transaction_id)
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}

	pub async fn sync(&self) -> tg::Result<()> {
		tokio::task::spawn_blocking({
			let env = self.env.clone();
			move || {
				env.force_sync()
					.map_err(|source| tg::error!(!source, "failed to sync"))
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))??;
		Ok(())
	}

	fn task_update_batch(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		batch_size: usize,
	) -> tg::Result<usize> {
		// Read a batch of UpdateVersion entries.
		let prefix = (Kind::UpdateVersion.to_i32().unwrap(),).pack_to_vec();
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to get update version range"))?
			.take(batch_size)
			.map(|entry| {
				let (key, _) = entry
					.map_err(|source| tg::error!(!source, "failed to read update version entry"))?;
				let key: Key = fdbt::unpack(key)
					.map_err(|source| tg::error!(!source, "failed to unpack key"))?;
				let Key::UpdateVersion { version, id } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((version, id))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		let mut count = 0;
		for (version, id) in entries {
			// Check if the update exists and get its type.
			let update_key = Key::Update { id: id.clone() }.pack_to_vec();
			let update_value = db
				.get(transaction, &update_key)
				.map_err(|source| tg::error!(!source, "failed to get update key"))?;

			// If the update key does not exist, delete the stale entry and continue.
			let Some(update_value) = update_value else {
				let version_key = Key::UpdateVersion {
					version,
					id: id.clone(),
				}
				.pack_to_vec();
				db.delete(transaction, &version_key)
					.map_err(|source| tg::error!(!source, "failed to delete update version key"))?;
				count += 1;
				continue;
			};

			// Parse the update. Default to Put for backwards compatibility.
			let update = update_value
				.first()
				.and_then(|&b| Update::from_u8(b))
				.unwrap_or(Update::Put);

			// Process the update.
			let changed = match &id {
				tg::Either::Left(object_id) => Self::recompute_object(db, transaction, object_id)?,
				tg::Either::Right(process_id) => {
					Self::recompute_process(db, transaction, process_id)?
				},
			};

			// For Put updates, always enqueue parent updates (the object is new to the index).
			// For Propagate updates, only enqueue parents if fields actually changed.
			let should_enqueue_parents = match update {
				Update::Put => true,
				Update::Propagate => changed,
			};
			if should_enqueue_parents {
				Self::enqueue_parents(db, transaction, &id, version)?;
			}

			// Delete both the Update and UpdateVersion keys.
			db.delete(transaction, &update_key)
				.map_err(|source| tg::error!(!source, "failed to delete update key"))?;
			let version_key = Key::UpdateVersion {
				version,
				id: id.clone(),
			}
			.pack_to_vec();
			db.delete(transaction, &version_key)
				.map_err(|source| tg::error!(!source, "failed to delete update version key"))?;

			count += 1;
		}

		Ok(count)
	}

	fn task_get_queue_size(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		transaction_id: u128,
	) -> tg::Result<u64> {
		// Count pending updates with version <= transaction_id. Parent updates enqueued during
		// processing use the same version as the child that triggered them.
		let version_limit = transaction_id as u64;
		let prefix = (Kind::UpdateVersion.to_i32().unwrap(),).pack_to_vec();
		let mut count = 0u64;
		for entry in db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to get update version range"))?
		{
			let (key, _) = entry
				.map_err(|source| tg::error!(!source, "failed to read update version entry"))?;
			let key: Key =
				fdbt::unpack(key).map_err(|source| tg::error!(!source, "failed to unpack key"))?;
			let Key::UpdateVersion { version, .. } = key else {
				break;
			};
			if version <= version_limit {
				count += 1;
			}
		}
		Ok(count)
	}

	fn recompute_object(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<bool> {
		// Get the current object.
		let key = Key::Object(id.clone()).pack_to_vec();
		let bytes = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
			.ok_or_else(|| tg::error!(%id, "object not found"))?;
		let mut object = Object::deserialize(bytes)?;

		// Get children.
		let children = Self::get_object_children(db, transaction, id)?;

		// Track if any field changed.
		let mut changed = false;

		// Compute stored.subtree = all(children.stored.subtree). Vacuously true for leaf nodes.
		if !object.stored.subtree {
			let all_children_stored = children.iter().all(|child| {
				Self::get_object(db, transaction, child)
					.ok()
					.flatten()
					.is_some_and(|o| o.stored.subtree)
			});
			if all_children_stored {
				object.stored.subtree = true;
				changed = true;
			}
		}

		// Compute metadata.subtree.count = 1 + sum(children.subtree.count). For leaf nodes, this is 1.
		if object.metadata.subtree.count.is_none() {
			let mut sum: u64 = 1;
			let mut all = true;
			for child in &children {
				if let Some(child_obj) = Self::get_object(db, transaction, child)? {
					if let Some(count) = child_obj.metadata.subtree.count {
						sum = sum.saturating_add(count);
					} else {
						all = false;
						break;
					}
				} else {
					all = false;
					break;
				}
			}
			if all {
				object.metadata.subtree.count = Some(sum);
				changed = true;
			}
		}

		// Compute metadata.subtree.depth = 1 + max(children.subtree.depth). For leaf nodes, this is 1.
		if object.metadata.subtree.depth.is_none() {
			let mut max: u64 = 0;
			let mut all = true;
			for child in &children {
				if let Some(child_obj) = Self::get_object(db, transaction, child)? {
					if let Some(depth) = child_obj.metadata.subtree.depth {
						max = max.max(depth);
					} else {
						all = false;
						break;
					}
				} else {
					all = false;
					break;
				}
			}
			if all {
				object.metadata.subtree.depth = Some(1 + max);
				changed = true;
			}
		}

		// Compute metadata.subtree.size = node.size + sum(children.subtree.size). For leaf nodes, this equals node.size.
		if object.metadata.subtree.size.is_none() {
			let node_size = object.metadata.node.size;
			let mut sum: u64 = node_size;
			let mut all = true;
			for child in &children {
				if let Some(child_obj) = Self::get_object(db, transaction, child)? {
					if let Some(size) = child_obj.metadata.subtree.size {
						sum = sum.saturating_add(size);
					} else {
						all = false;
						break;
					}
				} else {
					all = false;
					break;
				}
			}
			if all {
				object.metadata.subtree.size = Some(sum);
				changed = true;
			}
		}

		// Compute metadata.subtree.solvable = node.solvable OR any(children.subtree.solvable). For leaf nodes, this equals node.solvable.
		if object.metadata.subtree.solvable.is_none() {
			let node_solvable = object.metadata.node.solvable;
			// Check if any child is solvable, and track if all children have been computed.
			let mut any_solvable = false;
			let mut all_computed = true;
			for child in &children {
				if let Some(child_obj) = Self::get_object(db, transaction, child)? {
					match child_obj.metadata.subtree.solvable {
						Some(true) => {
							any_solvable = true;
						},
						Some(false) => {},
						None => {
							all_computed = false;
						},
					}
				} else {
					all_computed = false;
				}
			}
			if all_computed {
				object.metadata.subtree.solvable = Some(node_solvable || any_solvable);
				changed = true;
			}
		}

		// Compute metadata.subtree.solved = node.solved AND all(children.subtree.solved). For leaf nodes, this equals node.solved.
		if object.metadata.subtree.solved.is_none() {
			let node_solved = object.metadata.node.solved;
			// Check if all children are solved, and track if all children have been computed.
			let mut all_solved = true;
			let mut all_computed = true;
			for child in &children {
				if let Some(child_obj) = Self::get_object(db, transaction, child)? {
					match child_obj.metadata.subtree.solved {
						Some(true) => {},
						Some(false) => {
							all_solved = false;
						},
						None => {
							all_computed = false;
							all_solved = false;
						},
					}
				} else {
					all_computed = false;
					all_solved = false;
				}
			}
			if all_computed {
				object.metadata.subtree.solved = Some(node_solved && all_solved);
				changed = true;
			}
		}

		// Write the object back if changed.
		if changed {
			let value = object.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|source| tg::error!(!source, %id, "failed to put the object"))?;
		}

		Ok(changed)
	}

	fn recompute_process(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<bool> {
		// Get the current process.
		let key = Key::Process(id.clone()).pack_to_vec();
		let bytes = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?
			.ok_or_else(|| tg::error!(%id, "process not found"))?;
		let mut process = Process::deserialize(bytes)?;

		// Get process objects (command, error, log, output).
		let objects = Self::get_process_objects(db, transaction, id)?;

		// Get process children.
		let children = Self::get_process_children(db, transaction, id)?;

		// Track if any field changed.
		let mut changed = false;

		// Track which object kinds exist.
		let mut has_error = false;
		let mut has_log = false;
		let mut has_output = false;

		// Process each object kind to compute node stored and metadata fields.
		for (object_id, kind) in &objects {
			match kind {
				ProcessObjectKind::Command => {},
				ProcessObjectKind::Error => has_error = true,
				ProcessObjectKind::Log => has_log = true,
				ProcessObjectKind::Output => has_output = true,
			}

			// Get the object's subtree metadata.
			let object = Self::get_object(db, transaction, object_id)?;

			// Compute node stored fields from object's stored.subtree.
			if let Some(ref obj) = object
				&& obj.stored.subtree
			{
				let stored_field = match kind {
					ProcessObjectKind::Command => &mut process.stored.node_command,
					ProcessObjectKind::Error => &mut process.stored.node_error,
					ProcessObjectKind::Log => &mut process.stored.node_log,
					ProcessObjectKind::Output => &mut process.stored.node_output,
				};
				if !*stored_field {
					*stored_field = true;
					changed = true;
				}
			}

			// Compute node metadata fields from object's subtree metadata.
			if let Some(ref obj) = object {
				let (node_count, node_depth, node_size, node_solvable, node_solved) = match kind {
					ProcessObjectKind::Command => (
						&mut process.metadata.node.command.count,
						&mut process.metadata.node.command.depth,
						&mut process.metadata.node.command.size,
						&mut process.metadata.node.command.solvable,
						&mut process.metadata.node.command.solved,
					),
					ProcessObjectKind::Error => (
						&mut process.metadata.node.error.count,
						&mut process.metadata.node.error.depth,
						&mut process.metadata.node.error.size,
						&mut process.metadata.node.error.solvable,
						&mut process.metadata.node.error.solved,
					),
					ProcessObjectKind::Log => (
						&mut process.metadata.node.log.count,
						&mut process.metadata.node.log.depth,
						&mut process.metadata.node.log.size,
						&mut process.metadata.node.log.solvable,
						&mut process.metadata.node.log.solved,
					),
					ProcessObjectKind::Output => (
						&mut process.metadata.node.output.count,
						&mut process.metadata.node.output.depth,
						&mut process.metadata.node.output.size,
						&mut process.metadata.node.output.solvable,
						&mut process.metadata.node.output.solved,
					),
				};

				if node_count.is_none() && obj.metadata.subtree.count.is_some() {
					*node_count = obj.metadata.subtree.count;
					changed = true;
				}
				if node_depth.is_none() && obj.metadata.subtree.depth.is_some() {
					*node_depth = obj.metadata.subtree.depth;
					changed = true;
				}
				if node_size.is_none() && obj.metadata.subtree.size.is_some() {
					*node_size = obj.metadata.subtree.size;
					changed = true;
				}
				if node_solvable.is_none() && obj.metadata.subtree.solvable.is_some() {
					*node_solvable = obj.metadata.subtree.solvable;
					changed = true;
				}
				if node_solved.is_none() && obj.metadata.subtree.solved.is_some() {
					*node_solved = obj.metadata.subtree.solved;
					changed = true;
				}
			}
		}

		// For missing object kinds, set stored to true, count/depth/size to 0, solvable to false
		// (no solvable references), and solved to true (vacuously true - all 0 objects are solved).
		if !has_error {
			if !process.stored.node_error {
				process.stored.node_error = true;
				changed = true;
			}
			if process.metadata.node.error.count.is_none() {
				process.metadata.node.error.count = Some(0);
				changed = true;
			}
			if process.metadata.node.error.depth.is_none() {
				process.metadata.node.error.depth = Some(0);
				changed = true;
			}
			if process.metadata.node.error.size.is_none() {
				process.metadata.node.error.size = Some(0);
				changed = true;
			}
			if process.metadata.node.error.solvable.is_none() {
				process.metadata.node.error.solvable = Some(false);
				changed = true;
			}
			if process.metadata.node.error.solved.is_none() {
				process.metadata.node.error.solved = Some(true);
				changed = true;
			}
		}
		if !has_log {
			if !process.stored.node_log {
				process.stored.node_log = true;
				changed = true;
			}
			if process.metadata.node.log.count.is_none() {
				process.metadata.node.log.count = Some(0);
				changed = true;
			}
			if process.metadata.node.log.depth.is_none() {
				process.metadata.node.log.depth = Some(0);
				changed = true;
			}
			if process.metadata.node.log.size.is_none() {
				process.metadata.node.log.size = Some(0);
				changed = true;
			}
			if process.metadata.node.log.solvable.is_none() {
				process.metadata.node.log.solvable = Some(false);
				changed = true;
			}
			if process.metadata.node.log.solved.is_none() {
				process.metadata.node.log.solved = Some(true);
				changed = true;
			}
		}
		if !has_output {
			if !process.stored.node_output {
				process.stored.node_output = true;
				changed = true;
			}
			if process.metadata.node.output.count.is_none() {
				process.metadata.node.output.count = Some(0);
				changed = true;
			}
			if process.metadata.node.output.depth.is_none() {
				process.metadata.node.output.depth = Some(0);
				changed = true;
			}
			if process.metadata.node.output.size.is_none() {
				process.metadata.node.output.size = Some(0);
				changed = true;
			}
			if process.metadata.node.output.solvable.is_none() {
				process.metadata.node.output.solvable = Some(false);
				changed = true;
			}
			if process.metadata.node.output.solved.is_none() {
				process.metadata.node.output.solved = Some(true);
				changed = true;
			}
		}

		// Compute stored.subtree = all(children.stored.subtree). Vacuously true for leaf processes.
		if !process.stored.subtree {
			let mut all = true;
			for child in &children {
				if let Some(child_process) = Self::get_process(db, transaction, child)? {
					if !child_process.stored.subtree {
						all = false;
						break;
					}
				} else {
					all = false;
					break;
				}
			}
			if all {
				process.stored.subtree = true;
				changed = true;
			}
		}

		// Compute subtree stored fields for each object kind.
		if !process.stored.subtree_command && process.stored.node_command {
			let mut all = true;
			for child in &children {
				if let Some(child_process) = Self::get_process(db, transaction, child)? {
					if !child_process.stored.subtree_command {
						all = false;
						break;
					}
				} else {
					all = false;
					break;
				}
			}
			if all {
				process.stored.subtree_command = true;
				changed = true;
			}
		}
		if !process.stored.subtree_error && process.stored.node_error {
			let mut all = true;
			for child in &children {
				if let Some(child_process) = Self::get_process(db, transaction, child)? {
					if !child_process.stored.subtree_error {
						all = false;
						break;
					}
				} else {
					all = false;
					break;
				}
			}
			if all {
				process.stored.subtree_error = true;
				changed = true;
			}
		}
		if !process.stored.subtree_log && process.stored.node_log {
			let mut all = true;
			for child in &children {
				if let Some(child_process) = Self::get_process(db, transaction, child)? {
					if !child_process.stored.subtree_log {
						all = false;
						break;
					}
				} else {
					all = false;
					break;
				}
			}
			if all {
				process.stored.subtree_log = true;
				changed = true;
			}
		}
		if !process.stored.subtree_output && process.stored.node_output {
			let mut all = true;
			for child in &children {
				if let Some(child_process) = Self::get_process(db, transaction, child)? {
					if !child_process.stored.subtree_output {
						all = false;
						break;
					}
				} else {
					all = false;
					break;
				}
			}
			if all {
				process.stored.subtree_output = true;
				changed = true;
			}
		}

		// Compute subtree.count = 1 + sum(children.subtree.count). For leaf processes, this is 1.
		if process.metadata.subtree.count.is_none() {
			let mut sum: u64 = 1;
			let mut all = true;
			for child in &children {
				if let Some(child_process) = Self::get_process(db, transaction, child)? {
					if let Some(count) = child_process.metadata.subtree.count {
						sum = sum.saturating_add(count);
					} else {
						all = false;
						break;
					}
				} else {
					all = false;
					break;
				}
			}
			if all {
				process.metadata.subtree.count = Some(sum);
				changed = true;
			}
		}

		// Compute subtree metadata fields for command. Unlike error/log/output, command must always
		// exist. If node command metadata is not yet computed, do not attempt to compute subtree
		// values as this would incorrectly set them to 0 for leaf processes.
		let node_command_ready = process.metadata.node.command.count.is_some()
			&& process.metadata.node.command.depth.is_some()
			&& process.metadata.node.command.size.is_some();
		if node_command_ready {
			if process.metadata.subtree.command.count.is_none()
				&& let Some(result) = Self::compute_subtree_sum(
					db,
					transaction,
					&children,
					process.metadata.node.command.count,
					|p| p.metadata.subtree.command.count,
				)? {
				process.metadata.subtree.command.count = Some(result);
				changed = true;
			}
			if process.metadata.subtree.command.depth.is_none()
				&& let Some(result) = Self::compute_subtree_max(
					db,
					transaction,
					&children,
					process.metadata.node.command.depth,
					|p| p.metadata.subtree.command.depth,
				)? {
				process.metadata.subtree.command.depth = Some(result);
				changed = true;
			}
			if process.metadata.subtree.command.size.is_none()
				&& let Some(result) = Self::compute_subtree_sum(
					db,
					transaction,
					&children,
					process.metadata.node.command.size,
					|p| p.metadata.subtree.command.size,
				)? {
				process.metadata.subtree.command.size = Some(result);
				changed = true;
			}
		}

		// Compute subtree metadata fields for error.
		if process.metadata.subtree.error.count.is_none()
			&& let Some(result) = Self::compute_subtree_sum(
				db,
				transaction,
				&children,
				process.metadata.node.error.count,
				|p| p.metadata.subtree.error.count,
			)? {
			process.metadata.subtree.error.count = Some(result);
			changed = true;
		}
		if process.metadata.subtree.error.depth.is_none()
			&& let Some(result) = Self::compute_subtree_max(
				db,
				transaction,
				&children,
				process.metadata.node.error.depth,
				|p| p.metadata.subtree.error.depth,
			)? {
			process.metadata.subtree.error.depth = Some(result);
			changed = true;
		}
		if process.metadata.subtree.error.size.is_none()
			&& let Some(result) = Self::compute_subtree_sum(
				db,
				transaction,
				&children,
				process.metadata.node.error.size,
				|p| p.metadata.subtree.error.size,
			)? {
			process.metadata.subtree.error.size = Some(result);
			changed = true;
		}

		// Compute subtree metadata fields for log.
		if process.metadata.subtree.log.count.is_none()
			&& let Some(result) = Self::compute_subtree_sum(
				db,
				transaction,
				&children,
				process.metadata.node.log.count,
				|p| p.metadata.subtree.log.count,
			)? {
			process.metadata.subtree.log.count = Some(result);
			changed = true;
		}
		if process.metadata.subtree.log.depth.is_none()
			&& let Some(result) = Self::compute_subtree_max(
				db,
				transaction,
				&children,
				process.metadata.node.log.depth,
				|p| p.metadata.subtree.log.depth,
			)? {
			process.metadata.subtree.log.depth = Some(result);
			changed = true;
		}
		if process.metadata.subtree.log.size.is_none()
			&& let Some(result) = Self::compute_subtree_sum(
				db,
				transaction,
				&children,
				process.metadata.node.log.size,
				|p| p.metadata.subtree.log.size,
			)? {
			process.metadata.subtree.log.size = Some(result);
			changed = true;
		}

		// Compute subtree metadata fields for output.
		if process.metadata.subtree.output.count.is_none()
			&& let Some(result) = Self::compute_subtree_sum(
				db,
				transaction,
				&children,
				process.metadata.node.output.count,
				|p| p.metadata.subtree.output.count,
			)? {
			process.metadata.subtree.output.count = Some(result);
			changed = true;
		}
		if process.metadata.subtree.output.depth.is_none()
			&& let Some(result) = Self::compute_subtree_max(
				db,
				transaction,
				&children,
				process.metadata.node.output.depth,
				|p| p.metadata.subtree.output.depth,
			)? {
			process.metadata.subtree.output.depth = Some(result);
			changed = true;
		}
		if process.metadata.subtree.output.size.is_none()
			&& let Some(result) = Self::compute_subtree_sum(
				db,
				transaction,
				&children,
				process.metadata.node.output.size,
				|p| p.metadata.subtree.output.size,
			)? {
			process.metadata.subtree.output.size = Some(result);
			changed = true;
		}

		// Compute subtree solvable fields using helper.
		if process.metadata.subtree.command.solvable.is_none()
			&& let Some(value) = Self::compute_subtree_solvable(
				db,
				transaction,
				&children,
				process.metadata.node.command.solvable,
				|p| p.metadata.subtree.command.solvable,
			)? {
			process.metadata.subtree.command.solvable = Some(value);
			changed = true;
		}
		if process.metadata.subtree.error.solvable.is_none()
			&& let Some(value) = Self::compute_subtree_solvable(
				db,
				transaction,
				&children,
				process.metadata.node.error.solvable,
				|p| p.metadata.subtree.error.solvable,
			)? {
			process.metadata.subtree.error.solvable = Some(value);
			changed = true;
		}
		if process.metadata.subtree.log.solvable.is_none()
			&& let Some(value) = Self::compute_subtree_solvable(
				db,
				transaction,
				&children,
				process.metadata.node.log.solvable,
				|p| p.metadata.subtree.log.solvable,
			)? {
			process.metadata.subtree.log.solvable = Some(value);
			changed = true;
		}
		if process.metadata.subtree.output.solvable.is_none()
			&& let Some(value) = Self::compute_subtree_solvable(
				db,
				transaction,
				&children,
				process.metadata.node.output.solvable,
				|p| p.metadata.subtree.output.solvable,
			)? {
			process.metadata.subtree.output.solvable = Some(value);
			changed = true;
		}

		// Compute subtree solved fields using helper.
		if process.metadata.subtree.command.solved.is_none()
			&& let Some(value) = Self::compute_subtree_solved(
				db,
				transaction,
				&children,
				process.metadata.node.command.solved,
				|p| p.metadata.subtree.command.solved,
			)? {
			process.metadata.subtree.command.solved = Some(value);
			changed = true;
		}
		if process.metadata.subtree.error.solved.is_none()
			&& let Some(value) = Self::compute_subtree_solved(
				db,
				transaction,
				&children,
				process.metadata.node.error.solved,
				|p| p.metadata.subtree.error.solved,
			)? {
			process.metadata.subtree.error.solved = Some(value);
			changed = true;
		}
		if process.metadata.subtree.log.solved.is_none()
			&& let Some(value) = Self::compute_subtree_solved(
				db,
				transaction,
				&children,
				process.metadata.node.log.solved,
				|p| p.metadata.subtree.log.solved,
			)? {
			process.metadata.subtree.log.solved = Some(value);
			changed = true;
		}
		if process.metadata.subtree.output.solved.is_none()
			&& let Some(value) = Self::compute_subtree_solved(
				db,
				transaction,
				&children,
				process.metadata.node.output.solved,
				|p| p.metadata.subtree.output.solved,
			)? {
			process.metadata.subtree.output.solved = Some(value);
			changed = true;
		}

		// Write the process back if changed.
		if changed {
			let value = process.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|source| tg::error!(!source, %id, "failed to put the process"))?;
		}

		Ok(changed)
	}

	fn compute_subtree_sum(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		children: &[tg::process::Id],
		node_value: Option<u64>,
		child_accessor: fn(&Process) -> Option<u64>,
	) -> tg::Result<Option<u64>> {
		// We need node_value to be computed before we can compute subtree.
		let mut all = node_value.is_some();
		let mut result = node_value.unwrap_or(0);

		for child in children {
			if let Some(child_process) = Self::get_process(db, transaction, child)? {
				if let Some(value) = child_accessor(&child_process) {
					result = result.saturating_add(value);
				} else {
					all = false;
					break;
				}
			} else {
				all = false;
				break;
			}
		}

		if all { Ok(Some(result)) } else { Ok(None) }
	}

	fn compute_subtree_max(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		children: &[tg::process::Id],
		node_value: Option<u64>,
		child_accessor: fn(&Process) -> Option<u64>,
	) -> tg::Result<Option<u64>> {
		// We need node_value to be computed before we can compute subtree.
		let mut all = node_value.is_some();
		let mut result = node_value.unwrap_or(0);

		for child in children {
			if let Some(child_process) = Self::get_process(db, transaction, child)? {
				if let Some(value) = child_accessor(&child_process) {
					result = result.max(value);
				} else {
					all = false;
					break;
				}
			} else {
				all = false;
				break;
			}
		}

		if all { Ok(Some(result)) } else { Ok(None) }
	}

	/// Compute subtree solvable: node.solvable OR any(children.subtree.solvable).
	/// Returns Some(true) if solvable, Some(false) if not solvable, None if not all children have been computed yet.
	fn compute_subtree_solvable(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		children: &[tg::process::Id],
		node_value: Option<bool>,
		child_accessor: fn(&Process) -> Option<bool>,
	) -> tg::Result<Option<bool>> {
		// If node is solvable, subtree is solvable.
		if node_value == Some(true) {
			return Ok(Some(true));
		}

		// Check children.
		let mut any_solvable = false;
		let mut all_computed = node_value.is_some();
		for child in children {
			if let Some(child_process) = Self::get_process(db, transaction, child)? {
				match child_accessor(&child_process) {
					Some(true) => {
						any_solvable = true;
						break;
					},
					Some(false) => {},
					None => {
						all_computed = false;
					},
				}
			} else {
				all_computed = false;
			}
		}

		if any_solvable {
			Ok(Some(true))
		} else if all_computed {
			// Node is not solvable and no child is solvable.
			Ok(Some(false))
		} else {
			Ok(None)
		}
	}

	/// Compute subtree solved: node.solved AND all(children.subtree.solved).
	/// Returns Some(true) if solved, Some(false) if not solved, None if not all children have been computed yet.
	fn compute_subtree_solved(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		children: &[tg::process::Id],
		node_value: Option<bool>,
		child_accessor: fn(&Process) -> Option<bool>,
	) -> tg::Result<Option<bool>> {
		// If node value is not yet computed, we cannot compute subtree.
		let Some(node_solved) = node_value else {
			return Ok(None);
		};

		// If node is not solved, subtree cannot be solved.
		if !node_solved {
			return Ok(Some(false));
		}

		// Node is solved, check children.
		let mut all_solved = true;
		let mut any_unsolved = false;
		let mut all_computed = true;
		for child in children {
			if let Some(child_process) = Self::get_process(db, transaction, child)? {
				match child_accessor(&child_process) {
					Some(true) => {},
					Some(false) => {
						any_unsolved = true;
						all_solved = false;
						break;
					},
					None => {
						all_computed = false;
						all_solved = false;
					},
				}
			} else {
				all_computed = false;
				all_solved = false;
			}
		}

		if all_solved {
			Ok(Some(true))
		} else if any_unsolved || all_computed {
			Ok(Some(false))
		} else {
			Ok(None)
		}
	}

	fn enqueue_parents(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		version: u64,
	) -> tg::Result<()> {
		match id {
			tg::Either::Left(object_id) => {
				// Enqueue object parents with Propagate type.
				let parents = Self::get_object_parents(db, transaction, object_id)?;
				for parent in parents {
					Self::enqueue_update(
						db,
						transaction,
						tg::Either::Left(parent),
						Update::Propagate,
						Some(version),
					)?;
				}

				// Enqueue process parents with Propagate type.
				let process_parents = Self::get_object_process_parents(db, transaction, object_id)?;
				for (process, _kind) in process_parents {
					Self::enqueue_update(
						db,
						transaction,
						tg::Either::Right(process),
						Update::Propagate,
						Some(version),
					)?;
				}
			},
			tg::Either::Right(process_id) => {
				// Enqueue process parents with Propagate type.
				let parents = Self::get_process_parents(db, transaction, process_id)?;
				for parent in parents {
					Self::enqueue_update(
						db,
						transaction,
						tg::Either::Right(parent),
						Update::Propagate,
						Some(version),
					)?;
				}
			},
		}
		Ok(())
	}

	pub(super) fn enqueue_update(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: tg::Either<tg::object::Id, tg::process::Id>,
		update: Update,
		version: Option<u64>,
	) -> tg::Result<()> {
		// Check if update already exists (dedup).
		let update_key = Key::Update { id: id.clone() }.pack_to_vec();
		if db
			.get(transaction, &update_key)
			.map_err(|source| tg::error!(!source, "failed to get update key"))?
			.is_some()
		{
			return Ok(()); // Already queued.
		}

		// Set the Update key with the update type as the value.
		let update_value = [update.to_u8().unwrap()];
		db.put(transaction, &update_key, &update_value)
			.map_err(|source| tg::error!(!source, "failed to put update key"))?;

		// Set the UpdateVersion key (for ordered processing). Use the provided version if given,
		// otherwise use the current transaction ID.
		let version = version.unwrap_or_else(|| transaction.id() as u64);
		let version_key = Key::UpdateVersion { version, id }.pack_to_vec();
		db.put(transaction, &version_key, &[])
			.map_err(|source| tg::error!(!source, "failed to put update version key"))?;

		Ok(())
	}

	fn get_object(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Option<Object>> {
		let key = Key::Object(id.clone()).pack_to_vec();
		let bytes = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?;
		match bytes {
			Some(bytes) => Ok(Some(Object::deserialize(bytes)?)),
			None => Ok(None),
		}
	}

	fn get_process(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Option<Process>> {
		let key = Key::Process(id.clone()).pack_to_vec();
		let bytes = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?;
		match bytes {
			Some(bytes) => Ok(Some(Process::deserialize(bytes)?)),
			None => Ok(None),
		}
	}

	fn get_object_children(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Vec<tg::object::Id>> {
		let prefix = (Kind::ObjectChild.to_i32().unwrap(), id.to_bytes().as_ref()).pack_to_vec();
		let mut children = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to get object children"))?;
		for entry in iter {
			let (key, _) =
				entry.map_err(|source| tg::error!(!source, "failed to read object child entry"))?;
			let key: Key =
				fdbt::unpack(key).map_err(|source| tg::error!(!source, "failed to unpack key"))?;
			if let Key::ObjectChild { child, .. } = key {
				children.push(child);
			} else {
				break;
			}
		}
		Ok(children)
	}

	fn get_object_parents(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Vec<tg::object::Id>> {
		let prefix = (Kind::ChildObject.to_i32().unwrap(), id.to_bytes().as_ref()).pack_to_vec();
		let mut parents = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to get object parents"))?;
		for entry in iter {
			let (key, _) =
				entry.map_err(|source| tg::error!(!source, "failed to read child object entry"))?;
			let key: Key =
				fdbt::unpack(key).map_err(|source| tg::error!(!source, "failed to unpack key"))?;
			if let Key::ChildObject { object, .. } = key {
				parents.push(object);
			} else {
				break;
			}
		}
		Ok(parents)
	}

	fn get_object_process_parents(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, ProcessObjectKind)>> {
		let prefix = (
			Kind::ObjectProcess.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let mut parents = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to get object process parents"))?;
		for entry in iter {
			let (key, _) = entry
				.map_err(|source| tg::error!(!source, "failed to read object process entry"))?;
			let key: Key =
				fdbt::unpack(key).map_err(|source| tg::error!(!source, "failed to unpack key"))?;
			if let Key::ObjectProcess { process, kind, .. } = key {
				parents.push((process, kind));
			} else {
				break;
			}
		}
		Ok(parents)
	}

	fn get_process_children(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::process::Id>> {
		let prefix = (Kind::ProcessChild.to_i32().unwrap(), id.to_bytes().as_ref()).pack_to_vec();
		let mut children = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to get process children"))?;
		for entry in iter {
			let (key, _) = entry
				.map_err(|source| tg::error!(!source, "failed to read process child entry"))?;
			let key: Key =
				fdbt::unpack(key).map_err(|source| tg::error!(!source, "failed to unpack key"))?;
			if let Key::ProcessChild { child, .. } = key {
				children.push(child);
			} else {
				break;
			}
		}
		Ok(children)
	}

	fn get_process_parents(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::process::Id>> {
		let prefix = (Kind::ChildProcess.to_i32().unwrap(), id.to_bytes().as_ref()).pack_to_vec();
		let mut parents = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to get process parents"))?;
		for entry in iter {
			let (key, _) = entry
				.map_err(|source| tg::error!(!source, "failed to read child process entry"))?;
			let key: Key =
				fdbt::unpack(key).map_err(|source| tg::error!(!source, "failed to unpack key"))?;
			if let Key::ChildProcess { parent, .. } = key {
				parents.push(parent);
			} else {
				break;
			}
		}
		Ok(parents)
	}

	fn get_process_objects(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Vec<(tg::object::Id, ProcessObjectKind)>> {
		let prefix = (
			Kind::ProcessObject.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let mut objects = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to get process objects"))?;
		for entry in iter {
			let (key, _) = entry
				.map_err(|source| tg::error!(!source, "failed to read process object entry"))?;
			let key: Key =
				fdbt::unpack(key).map_err(|source| tg::error!(!source, "failed to unpack key"))?;
			if let Key::ProcessObject { object, kind, .. } = key {
				objects.push((object, kind));
			} else {
				break;
			}
		}
		Ok(objects)
	}
}
