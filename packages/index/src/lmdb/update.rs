use {
	super::{Db, Index, Key, Kind, Request, Response, Update},
	crate::{Object, Process, ProcessObjectKind},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	num_traits::{FromPrimitive as _, ToPrimitive as _},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn update_batch(&self, batch_size: usize) -> tg::Result<usize> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Update { batch_size };
		self.sender_low
			.send((request, sender))
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		match response {
			Response::UpdateCount(count) => Ok(count),
			_ => Err(tg::error!("unexpected response")),
		}
	}

	pub async fn updates_finished(&self, transaction_id: u64) -> tg::Result<bool> {
		let env = self.env.clone();
		let db = self.db;
		tokio::task::spawn_blocking(move || {
			let transaction = env
				.read_txn()
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
			let prefix = (Kind::UpdateVersion.to_i32().unwrap(),).pack_to_vec();
			for entry in db
				.prefix_iter(&transaction, &prefix)
				.map_err(|source| tg::error!(!source, "failed to get update version range"))?
			{
				let (key, _) = entry
					.map_err(|source| tg::error!(!source, "failed to read update version entry"))?;
				let key = fdbt::unpack(key)
					.map_err(|source| tg::error!(!source, "failed to unpack key"))?;
				let Key::UpdateVersion { version, .. } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				if version <= transaction_id {
					return Ok(false);
				}
			}
			Ok(true)
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}

	pub(super) fn task_update_batch(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		batch_size: usize,
	) -> tg::Result<usize> {
		// Read a batch.
		let prefix = (Kind::UpdateVersion.to_i32().unwrap(),).pack_to_vec();
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to get update version range"))?
			.take(batch_size)
			.map(|entry| {
				let (key, _) = entry
					.map_err(|source| tg::error!(!source, "failed to read update version entry"))?;
				let key = fdbt::unpack(key)
					.map_err(|source| tg::error!(!source, "failed to unpack key"))?;
				let Key::UpdateVersion { version, id } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((version, id))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		let mut count = 0;
		for (version, id) in entries {
			let key = Key::Update { id: id.clone() }.pack_to_vec();
			let value = db
				.get(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to get update key"))?;

			let Some(value) = value else {
				let key = Key::UpdateVersion {
					version,
					id: id.clone(),
				}
				.pack_to_vec();
				db.delete(transaction, &key)
					.map_err(|source| tg::error!(!source, "failed to delete update version key"))?;
				count += 1;
				continue;
			};

			let update = value
				.first()
				.and_then(|&b| Update::from_u8(b))
				.unwrap_or(Update::Put);

			let changed = match &id {
				tg::Either::Left(id) => Self::update_object(db, transaction, id)?,
				tg::Either::Right(id) => Self::update_process(db, transaction, id)?,
			};

			let enqueue_parents = match update {
				Update::Put => true,
				Update::Propagate => changed,
			};
			if enqueue_parents {
				Self::enqueue_parents(db, transaction, &id, version)?;
			}

			let key = Key::Update { id: id.clone() }.pack_to_vec();
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete update key"))?;
			let key = Key::UpdateVersion {
				version,
				id: id.clone(),
			}
			.pack_to_vec();
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete update version key"))?;

			count += 1;
		}

		Ok(count)
	}

	fn update_object(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<bool> {
		let key = Key::Object(id.clone()).pack_to_vec();
		let bytes = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
			.ok_or_else(|| tg::error!(%id, "object not found"))?;
		let mut object = Object::deserialize(bytes)?;

		let children = Self::get_object_children_with_transaction(db, transaction, id)?;

		// Cache all child objects to avoid repeated fetches.
		let child_objects: Vec<Option<Object>> = children
			.iter()
			.map(|child| Self::try_get_object_with_transaction(db, transaction, child))
			.collect::<tg::Result<_>>()?;

		let mut changed = false;

		if !object.stored.subtree {
			let all_children_stored = child_objects
				.iter()
				.all(|child| child.as_ref().is_some_and(|o| o.stored.subtree));
			if all_children_stored {
				object.stored.subtree = true;
				changed = true;
			}
		}

		if object.metadata.subtree.count.is_none() {
			let mut sum: u64 = 1;
			let mut all = true;
			for child_obj in &child_objects {
				if let Some(child_obj) = child_obj {
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

		if object.metadata.subtree.depth.is_none() {
			let mut max: u64 = 0;
			let mut all = true;
			for child_obj in &child_objects {
				if let Some(child_obj) = child_obj {
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

		if object.metadata.subtree.size.is_none() {
			let node_size = object.metadata.node.size;
			let mut sum: u64 = node_size;
			let mut all = true;
			for child_obj in &child_objects {
				if let Some(child_obj) = child_obj {
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

		if object.metadata.subtree.solvable.is_none() {
			let node_solvable = object.metadata.node.solvable;
			let mut any_solvable = false;
			let mut all_computed = true;
			for child_obj in &child_objects {
				if let Some(child_obj) = child_obj {
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

		if object.metadata.subtree.solved.is_none() {
			let node_solved = object.metadata.node.solved;
			let mut all_solved = true;
			let mut all_computed = true;
			for child_obj in &child_objects {
				if let Some(child_obj) = child_obj {
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

		if changed {
			let value = object.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|source| tg::error!(!source, %id, "failed to put the object"))?;
		}

		Ok(changed)
	}

	fn update_process(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<bool> {
		let key = Key::Process(id.clone()).pack_to_vec();
		let bytes = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?
			.ok_or_else(|| tg::error!(%id, "process not found"))?;
		let mut process = Process::deserialize(bytes)?;

		// Fetch all objects by kind once at the start to avoid repeated fetches.
		let object_ids = Self::get_process_objects_with_transaction(db, transaction, id)?;
		let mut command_object: Option<Object> = None;
		let mut error_objects: Vec<Object> = Vec::new();
		let mut log_objects: Vec<Object> = Vec::new();
		let mut output_object: Option<Object> = None;
		for (id, kind) in &object_ids {
			let Some(object) = Self::try_get_object_with_transaction(db, transaction, id)? else {
				continue;
			};
			match kind {
				ProcessObjectKind::Command => command_object = Some(object),
				ProcessObjectKind::Error => error_objects.push(object),
				ProcessObjectKind::Log => log_objects.push(object),
				ProcessObjectKind::Output => output_object = Some(object),
			}
		}

		// Fetch all child processes once at the start to avoid repeated fetches.
		let child_ids = Self::get_process_children_with_transaction(db, transaction, id)?;
		let child_processes: Vec<Option<Process>> = child_ids
			.iter()
			.map(|child| Self::try_get_process_with_transaction(db, transaction, child))
			.collect::<tg::Result<_>>()?;

		let mut changed = false;

		let has_command = object_ids
			.iter()
			.any(|(_, kind)| matches!(kind, ProcessObjectKind::Command));
		let has_error = object_ids
			.iter()
			.any(|(_, kind)| matches!(kind, ProcessObjectKind::Error));
		let has_log = object_ids
			.iter()
			.any(|(_, kind)| matches!(kind, ProcessObjectKind::Log));
		let has_output = object_ids
			.iter()
			.any(|(_, kind)| matches!(kind, ProcessObjectKind::Output));

		// Process command object.
		if let Some(object) = &command_object {
			if object.stored.subtree && !process.stored.node_command {
				process.stored.node_command = true;
				changed = true;
			}
			if process.metadata.node.command.count.is_none()
				&& object.metadata.subtree.count.is_some()
			{
				process.metadata.node.command.count = object.metadata.subtree.count;
				changed = true;
			}
			if process.metadata.node.command.depth.is_none()
				&& object.metadata.subtree.depth.is_some()
			{
				process.metadata.node.command.depth = object.metadata.subtree.depth;
				changed = true;
			}
			if process.metadata.node.command.size.is_none()
				&& object.metadata.subtree.size.is_some()
			{
				process.metadata.node.command.size = object.metadata.subtree.size;
				changed = true;
			}
			if process.metadata.node.command.solvable.is_none()
				&& object.metadata.subtree.solvable.is_some()
			{
				process.metadata.node.command.solvable = object.metadata.subtree.solvable;
				changed = true;
			}
			if process.metadata.node.command.solved.is_none()
				&& object.metadata.subtree.solved.is_some()
			{
				process.metadata.node.command.solved = object.metadata.subtree.solved;
				changed = true;
			}
		}

		// Process error objects.
		for object in &error_objects {
			if object.stored.subtree && !process.stored.node_error {
				process.stored.node_error = true;
				changed = true;
			}
			if process.metadata.node.error.count.is_none()
				&& object.metadata.subtree.count.is_some()
			{
				process.metadata.node.error.count = object.metadata.subtree.count;
				changed = true;
			}
			if process.metadata.node.error.depth.is_none()
				&& object.metadata.subtree.depth.is_some()
			{
				process.metadata.node.error.depth = object.metadata.subtree.depth;
				changed = true;
			}
			if process.metadata.node.error.size.is_none() && object.metadata.subtree.size.is_some()
			{
				process.metadata.node.error.size = object.metadata.subtree.size;
				changed = true;
			}
			if process.metadata.node.error.solvable.is_none()
				&& object.metadata.subtree.solvable.is_some()
			{
				process.metadata.node.error.solvable = object.metadata.subtree.solvable;
				changed = true;
			}
			if process.metadata.node.error.solved.is_none()
				&& object.metadata.subtree.solved.is_some()
			{
				process.metadata.node.error.solved = object.metadata.subtree.solved;
				changed = true;
			}
		}

		// Process log objects.
		for object in &log_objects {
			if object.stored.subtree && !process.stored.node_log {
				process.stored.node_log = true;
				changed = true;
			}
			if process.metadata.node.log.count.is_none() && object.metadata.subtree.count.is_some()
			{
				process.metadata.node.log.count = object.metadata.subtree.count;
				changed = true;
			}
			if process.metadata.node.log.depth.is_none() && object.metadata.subtree.depth.is_some()
			{
				process.metadata.node.log.depth = object.metadata.subtree.depth;
				changed = true;
			}
			if process.metadata.node.log.size.is_none() && object.metadata.subtree.size.is_some() {
				process.metadata.node.log.size = object.metadata.subtree.size;
				changed = true;
			}
			if process.metadata.node.log.solvable.is_none()
				&& object.metadata.subtree.solvable.is_some()
			{
				process.metadata.node.log.solvable = object.metadata.subtree.solvable;
				changed = true;
			}
			if process.metadata.node.log.solved.is_none()
				&& object.metadata.subtree.solved.is_some()
			{
				process.metadata.node.log.solved = object.metadata.subtree.solved;
				changed = true;
			}
		}

		// Process output object.
		if let Some(object) = &output_object {
			if object.stored.subtree && !process.stored.node_output {
				process.stored.node_output = true;
				changed = true;
			}
			if process.metadata.node.output.count.is_none()
				&& object.metadata.subtree.count.is_some()
			{
				process.metadata.node.output.count = object.metadata.subtree.count;
				changed = true;
			}
			if process.metadata.node.output.depth.is_none()
				&& object.metadata.subtree.depth.is_some()
			{
				process.metadata.node.output.depth = object.metadata.subtree.depth;
				changed = true;
			}
			if process.metadata.node.output.size.is_none() && object.metadata.subtree.size.is_some()
			{
				process.metadata.node.output.size = object.metadata.subtree.size;
				changed = true;
			}
			if process.metadata.node.output.solvable.is_none()
				&& object.metadata.subtree.solvable.is_some()
			{
				process.metadata.node.output.solvable = object.metadata.subtree.solvable;
				changed = true;
			}
			if process.metadata.node.output.solved.is_none()
				&& object.metadata.subtree.solved.is_some()
			{
				process.metadata.node.output.solved = object.metadata.subtree.solved;
				changed = true;
			}
		}

		if !has_command {
			if !process.stored.node_command {
				process.stored.node_command = true;
				changed = true;
			}
			if process.metadata.node.command.count.is_none() {
				process.metadata.node.command.count = Some(0);
				changed = true;
			}
			if process.metadata.node.command.depth.is_none() {
				process.metadata.node.command.depth = Some(0);
				changed = true;
			}
			if process.metadata.node.command.size.is_none() {
				process.metadata.node.command.size = Some(0);
				changed = true;
			}
			if process.metadata.node.command.solvable.is_none() {
				process.metadata.node.command.solvable = Some(false);
				changed = true;
			}
			if process.metadata.node.command.solved.is_none() {
				process.metadata.node.command.solved = Some(true);
				changed = true;
			}
		}

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

		if !process.stored.subtree {
			let all_stored = child_processes
				.iter()
				.all(|child| child.as_ref().is_some_and(|p| p.stored.subtree));
			if all_stored {
				process.stored.subtree = true;
				changed = true;
			}
		}

		if !process.stored.subtree_command && process.stored.node_command {
			let all_stored = child_processes
				.iter()
				.all(|child| child.as_ref().is_some_and(|p| p.stored.subtree_command));
			if all_stored {
				process.stored.subtree_command = true;
				changed = true;
			}
		}

		if !process.stored.subtree_error && process.stored.node_error {
			let all_stored = child_processes
				.iter()
				.all(|child| child.as_ref().is_some_and(|p| p.stored.subtree_error));
			if all_stored {
				process.stored.subtree_error = true;
				changed = true;
			}
		}

		if !process.stored.subtree_log && process.stored.node_log {
			let all_stored = child_processes
				.iter()
				.all(|child| child.as_ref().is_some_and(|p| p.stored.subtree_log));
			if all_stored {
				process.stored.subtree_log = true;
				changed = true;
			}
		}

		if !process.stored.subtree_output && process.stored.node_output {
			let all_stored = child_processes
				.iter()
				.all(|child| child.as_ref().is_some_and(|p| p.stored.subtree_output));
			if all_stored {
				process.stored.subtree_output = true;
				changed = true;
			}
		}

		if process.metadata.subtree.count.is_none() {
			let mut sum: u64 = 1;
			let mut all = true;
			for child_process in &child_processes {
				if let Some(child_process) = child_process {
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

		let node_command_ready = process.metadata.node.command.count.is_some()
			&& process.metadata.node.command.depth.is_some()
			&& process.metadata.node.command.size.is_some();
		if node_command_ready {
			if process.metadata.subtree.command.count.is_none()
				&& let Some(result) = Self::compute_subtree_sum(
					&child_processes,
					process.metadata.node.command.count,
					|p| p.metadata.subtree.command.count,
				) {
				process.metadata.subtree.command.count = Some(result);
				changed = true;
			}

			if process.metadata.subtree.command.depth.is_none()
				&& let Some(result) = Self::compute_subtree_max(
					&child_processes,
					process.metadata.node.command.depth,
					|p| p.metadata.subtree.command.depth,
				) {
				process.metadata.subtree.command.depth = Some(result);
				changed = true;
			}

			if process.metadata.subtree.command.size.is_none()
				&& let Some(result) = Self::compute_subtree_sum(
					&child_processes,
					process.metadata.node.command.size,
					|p| p.metadata.subtree.command.size,
				) {
				process.metadata.subtree.command.size = Some(result);
				changed = true;
			}
		}

		if process.metadata.subtree.error.count.is_none()
			&& let Some(result) = Self::compute_subtree_sum(
				&child_processes,
				process.metadata.node.error.count,
				|p| p.metadata.subtree.error.count,
			) {
			process.metadata.subtree.error.count = Some(result);
			changed = true;
		}

		if process.metadata.subtree.error.depth.is_none()
			&& let Some(result) = Self::compute_subtree_max(
				&child_processes,
				process.metadata.node.error.depth,
				|p| p.metadata.subtree.error.depth,
			) {
			process.metadata.subtree.error.depth = Some(result);
			changed = true;
		}

		if process.metadata.subtree.error.size.is_none()
			&& let Some(result) =
				Self::compute_subtree_sum(&child_processes, process.metadata.node.error.size, |p| {
					p.metadata.subtree.error.size
				}) {
			process.metadata.subtree.error.size = Some(result);
			changed = true;
		}

		if process.metadata.subtree.log.count.is_none()
			&& let Some(result) =
				Self::compute_subtree_sum(&child_processes, process.metadata.node.log.count, |p| {
					p.metadata.subtree.log.count
				}) {
			process.metadata.subtree.log.count = Some(result);
			changed = true;
		}
		if process.metadata.subtree.log.depth.is_none()
			&& let Some(result) =
				Self::compute_subtree_max(&child_processes, process.metadata.node.log.depth, |p| {
					p.metadata.subtree.log.depth
				}) {
			process.metadata.subtree.log.depth = Some(result);
			changed = true;
		}

		if process.metadata.subtree.log.size.is_none()
			&& let Some(result) =
				Self::compute_subtree_sum(&child_processes, process.metadata.node.log.size, |p| {
					p.metadata.subtree.log.size
				}) {
			process.metadata.subtree.log.size = Some(result);
			changed = true;
		}

		if process.metadata.subtree.output.count.is_none()
			&& let Some(result) = Self::compute_subtree_sum(
				&child_processes,
				process.metadata.node.output.count,
				|p| p.metadata.subtree.output.count,
			) {
			process.metadata.subtree.output.count = Some(result);
			changed = true;
		}

		if process.metadata.subtree.output.depth.is_none()
			&& let Some(result) = Self::compute_subtree_max(
				&child_processes,
				process.metadata.node.output.depth,
				|p| p.metadata.subtree.output.depth,
			) {
			process.metadata.subtree.output.depth = Some(result);
			changed = true;
		}

		if process.metadata.subtree.output.size.is_none()
			&& let Some(result) = Self::compute_subtree_sum(
				&child_processes,
				process.metadata.node.output.size,
				|p| p.metadata.subtree.output.size,
			) {
			process.metadata.subtree.output.size = Some(result);
			changed = true;
		}

		if process.metadata.subtree.command.solvable.is_none()
			&& let Some(value) = Self::compute_subtree_solvable(
				&child_processes,
				process.metadata.node.command.solvable,
				|p| p.metadata.subtree.command.solvable,
			) {
			process.metadata.subtree.command.solvable = Some(value);
			changed = true;
		}

		if process.metadata.subtree.error.solvable.is_none()
			&& let Some(value) = Self::compute_subtree_solvable(
				&child_processes,
				process.metadata.node.error.solvable,
				|p| p.metadata.subtree.error.solvable,
			) {
			process.metadata.subtree.error.solvable = Some(value);
			changed = true;
		}

		if process.metadata.subtree.log.solvable.is_none()
			&& let Some(value) = Self::compute_subtree_solvable(
				&child_processes,
				process.metadata.node.log.solvable,
				|p| p.metadata.subtree.log.solvable,
			) {
			process.metadata.subtree.log.solvable = Some(value);
			changed = true;
		}

		if process.metadata.subtree.output.solvable.is_none()
			&& let Some(value) = Self::compute_subtree_solvable(
				&child_processes,
				process.metadata.node.output.solvable,
				|p| p.metadata.subtree.output.solvable,
			) {
			process.metadata.subtree.output.solvable = Some(value);
			changed = true;
		}

		if process.metadata.subtree.command.solved.is_none()
			&& let Some(value) = Self::compute_subtree_solved(
				&child_processes,
				process.metadata.node.command.solved,
				|p| p.metadata.subtree.command.solved,
			) {
			process.metadata.subtree.command.solved = Some(value);
			changed = true;
		}

		if process.metadata.subtree.error.solved.is_none()
			&& let Some(value) = Self::compute_subtree_solved(
				&child_processes,
				process.metadata.node.error.solved,
				|p| p.metadata.subtree.error.solved,
			) {
			process.metadata.subtree.error.solved = Some(value);
			changed = true;
		}

		if process.metadata.subtree.log.solved.is_none()
			&& let Some(value) = Self::compute_subtree_solved(
				&child_processes,
				process.metadata.node.log.solved,
				|p| p.metadata.subtree.log.solved,
			) {
			process.metadata.subtree.log.solved = Some(value);
			changed = true;
		}

		if process.metadata.subtree.output.solved.is_none()
			&& let Some(value) = Self::compute_subtree_solved(
				&child_processes,
				process.metadata.node.output.solved,
				|p| p.metadata.subtree.output.solved,
			) {
			process.metadata.subtree.output.solved = Some(value);
			changed = true;
		}

		if changed {
			let value = process.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|source| tg::error!(!source, %id, "failed to put the process"))?;
		}

		Ok(changed)
	}

	fn compute_subtree_sum(
		child_processes: &[Option<Process>],
		node_value: Option<u64>,
		child_accessor: fn(&Process) -> Option<u64>,
	) -> Option<u64> {
		let mut all = node_value.is_some();
		let mut result = node_value.unwrap_or(0);
		for child_process in child_processes {
			if let Some(child_process) = child_process {
				if let Some(value) = child_accessor(child_process) {
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
		if all { Some(result) } else { None }
	}

	fn compute_subtree_max(
		child_processes: &[Option<Process>],
		node_value: Option<u64>,
		child_accessor: fn(&Process) -> Option<u64>,
	) -> Option<u64> {
		let mut all = node_value.is_some();
		let mut result = node_value.unwrap_or(0);
		for child_process in child_processes {
			if let Some(child_process) = child_process {
				if let Some(value) = child_accessor(child_process) {
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
		if all { Some(result) } else { None }
	}

	fn compute_subtree_solvable(
		child_processes: &[Option<Process>],
		node_value: Option<bool>,
		child_accessor: fn(&Process) -> Option<bool>,
	) -> Option<bool> {
		if node_value == Some(true) {
			return Some(true);
		}
		let mut any_solvable = false;
		let mut all_computed = node_value.is_some();
		for child_process in child_processes {
			if let Some(child_process) = child_process {
				match child_accessor(child_process) {
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
			Some(true)
		} else if all_computed {
			Some(false)
		} else {
			None
		}
	}

	fn compute_subtree_solved(
		child_processes: &[Option<Process>],
		node_value: Option<bool>,
		child_accessor: fn(&Process) -> Option<bool>,
	) -> Option<bool> {
		let node_solved = node_value?;
		if !node_solved {
			return Some(false);
		}
		let mut all_solved = true;
		let mut any_unsolved = false;
		let mut all_computed = true;
		for child_process in child_processes {
			if let Some(child_process) = child_process {
				match child_accessor(child_process) {
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
			Some(true)
		} else if any_unsolved || all_computed {
			Some(false)
		} else {
			None
		}
	}

	fn enqueue_parents(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		version: u64,
	) -> tg::Result<()> {
		match id {
			tg::Either::Left(id) => {
				let parents = Self::get_object_parents_with_transaction(db, transaction, id)?;
				for parent in parents {
					Self::enqueue_update(
						db,
						transaction,
						tg::Either::Left(parent),
						Update::Propagate,
						Some(version),
					)?;
				}
				let process_parents =
					Self::get_object_processes_with_transaction(db, transaction, id)?;
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
			tg::Either::Right(id) => {
				let parents = Self::get_process_parents_with_transaction(db, transaction, id)?;
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
		let key = Key::Update { id: id.clone() }.pack_to_vec();
		if db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to get update key"))?
			.is_some()
		{
			return Ok(());
		}

		let value = [update.to_u8().unwrap()];
		db.put(transaction, &key, &value)
			.map_err(|source| tg::error!(!source, "failed to put update key"))?;

		let version = version.unwrap_or_else(|| transaction.id() as u64);
		let key = Key::UpdateVersion { version, id }.pack_to_vec();
		db.put(transaction, &key, &[])
			.map_err(|source| tg::error!(!source, "failed to put update version key"))?;

		Ok(())
	}
}
