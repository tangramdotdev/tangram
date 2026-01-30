use {
	super::{Db, Index, Key, KeyKind, Request, Response, Update},
	crate::{Object, Process, ProcessObjectKind},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	num_traits::{FromPrimitive as _, ToPrimitive as _},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn updates_finished(&self, transaction_id: u64) -> tg::Result<bool> {
		let env = self.env.clone();
		let db = self.db;
		tokio::task::spawn_blocking(move || {
			let transaction = env
				.read_txn()
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
			let prefix = (KeyKind::UpdateVersion.to_i32().unwrap(),).pack_to_vec();
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

	pub async fn update_batch(
		&self,
		batch_size: usize,
		_partition_start: u64,
		_partition_count: u64,
	) -> tg::Result<usize> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Update { batch_size };
		self.sender_low
			.send((request, sender))
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::UpdateCount(count) = response else {
			return Err(tg::error!("unexpected response"));
		};
		Ok(count)
	}

	pub(super) fn task_update_batch(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		batch_size: usize,
	) -> tg::Result<usize> {
		let prefix = (KeyKind::UpdateVersion.to_i32().unwrap(),).pack_to_vec();
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
				.map_err(|source| tg::error!(!source, "failed to get update key"))?
				.ok_or_else(|| tg::error!("expected an update key for the update version key"))?;

			let update = value
				.first()
				.and_then(|value| Update::from_u8(*value))
				.ok_or_else(|| tg::error!("invalid update value"))?;

			let changed = match &id {
				tg::Either::Left(id) => Self::update_object(db, transaction, id)?,
				tg::Either::Right(id) => Self::update_process(db, transaction, id)?,
			};

			if match update {
				Update::Put => true,
				Update::Propagate => changed,
			} {
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

		let child_objects: Vec<Option<Object>> = children
			.iter()
			.map(|child| Self::try_get_object_with_transaction(db, transaction, child))
			.collect::<tg::Result<_>>()?;

		let mut changed = false;

		if !object.stored.subtree {
			let value = child_objects
				.iter()
				.all(|child| child.as_ref().is_some_and(|object| object.stored.subtree));
			if value {
				object.stored.subtree = true;
				changed = true;
			}
		}

		if object.metadata.subtree.count.is_none() {
			let value = child_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.count)
				})
				.sum::<Option<u64>>();
			if let Some(value) = value {
				let value = 1 + value;
				object.metadata.subtree.count = Some(value);
				changed = true;
			}
		}

		if object.metadata.subtree.depth.is_none() {
			let value = child_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.depth)
				})
				.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
			if let Some(value) = value {
				let value = 1 + value;
				object.metadata.subtree.depth = Some(value);
				changed = true;
			}
		}

		if object.metadata.subtree.size.is_none() {
			let value = child_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.size)
				})
				.sum::<Option<u64>>();
			if let Some(value) = value {
				let value = object.metadata.node.size + value;
				object.metadata.subtree.size = Some(value);
				changed = true;
			}
		}

		if object.metadata.subtree.solvable.is_none() {
			let value = child_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.solvable)
				})
				.try_fold(object.metadata.node.solvable, |output, value| {
					value.map(|value| output || value)
				});
			if let Some(value) = value {
				object.metadata.subtree.solvable = Some(value);
				changed = true;
			}
		}

		if object.metadata.subtree.solved.is_none() {
			let value = child_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.solved)
				})
				.try_fold(object.metadata.node.solved, |output, value| {
					value.map(|value| output && value)
				});
			if let Some(value) = value {
				object.metadata.subtree.solved = Some(value);
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

		let children = Self::get_process_children_with_transaction(db, transaction, id)?;
		let children = children
			.iter()
			.map(|child| Self::try_get_process_with_transaction(db, transaction, child))
			.collect::<tg::Result<Vec<_>>>()?;

		let objects = Self::get_process_objects_with_transaction(db, transaction, id)?;
		let mut command_object: Option<Object> = None;
		let mut error_objects: Vec<Option<Object>> = Vec::new();
		let mut log_object: Option<Option<Object>> = None;
		let mut output_objects: Vec<Option<Object>> = Vec::new();
		for (id, kind) in &objects {
			let object = Self::try_get_object_with_transaction(db, transaction, id)?;
			match kind {
				ProcessObjectKind::Command => {
					command_object = object;
				},
				ProcessObjectKind::Error => {
					error_objects.push(object);
				},
				ProcessObjectKind::Log => {
					log_object = Some(object);
				},
				ProcessObjectKind::Output => {
					output_objects.push(object);
				},
			}
		}

		let mut changed = false;

		if let Some(object) = &command_object {
			if process.metadata.node.command.count.is_none()
				&& let Some(value) = object.metadata.subtree.count
			{
				process.metadata.node.command.count = Some(value);
				changed = true;
			}
			if process.metadata.node.command.depth.is_none()
				&& let Some(value) = object.metadata.subtree.depth
			{
				process.metadata.node.command.depth = Some(value);
				changed = true;
			}
			if process.metadata.node.command.size.is_none()
				&& let Some(value) = object.metadata.subtree.size
			{
				process.metadata.node.command.size = Some(value);
				changed = true;
			}
			if process.metadata.node.command.solvable.is_none()
				&& let Some(value) = object.metadata.subtree.solvable
			{
				process.metadata.node.command.solvable = Some(value);
				changed = true;
			}
			if process.metadata.node.command.solved.is_none()
				&& let Some(value) = object.metadata.subtree.solved
			{
				process.metadata.node.command.solved = Some(value);
				changed = true;
			}
		}

		if process.metadata.node.error.count.is_none() {
			let value = error_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.count)
				})
				.sum::<Option<u64>>();
			if let Some(value) = value {
				process.metadata.node.error.count = Some(value);
				changed = true;
			}
		}
		if process.metadata.node.error.depth.is_none() {
			let value = error_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.depth)
				})
				.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
			if let Some(value) = value {
				process.metadata.node.error.depth = Some(value);
				changed = true;
			}
		}
		if process.metadata.node.error.size.is_none() {
			let value = error_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.size)
				})
				.sum::<Option<u64>>();
			if let Some(value) = value {
				process.metadata.node.error.size = Some(value);
				changed = true;
			}
		}
		if process.metadata.node.error.solvable.is_none() {
			let value = error_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.solvable)
				})
				.try_fold(false, |output, value| value.map(|value| output || value));
			if let Some(value) = value {
				process.metadata.node.error.solvable = Some(value);
				changed = true;
			}
		}
		if process.metadata.node.error.solved.is_none() {
			let value = error_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.solved)
				})
				.try_fold(true, |output, value| value.map(|value| output && value));
			if let Some(value) = value {
				process.metadata.node.error.solved = Some(value);
				changed = true;
			}
		}

		if let Some(Some(object)) = &log_object {
			if process.metadata.node.log.count.is_none()
				&& let Some(value) = object.metadata.subtree.count
			{
				process.metadata.node.log.count = Some(value);
				changed = true;
			}
			if process.metadata.node.log.depth.is_none()
				&& let Some(value) = object.metadata.subtree.depth
			{
				process.metadata.node.log.depth = Some(value);
				changed = true;
			}
			if process.metadata.node.log.size.is_none()
				&& let Some(value) = object.metadata.subtree.size
			{
				process.metadata.node.log.size = Some(value);
				changed = true;
			}
			if process.metadata.node.log.solvable.is_none()
				&& let Some(value) = object.metadata.subtree.solvable
			{
				process.metadata.node.log.solvable = Some(value);
				changed = true;
			}
			if process.metadata.node.log.solved.is_none()
				&& let Some(value) = object.metadata.subtree.solved
			{
				process.metadata.node.log.solved = Some(value);
				changed = true;
			}
		} else if log_object.is_none() {
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

		if process.metadata.node.output.count.is_none() {
			let value = output_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.count)
				})
				.sum::<Option<u64>>();
			if let Some(value) = value {
				process.metadata.node.output.count = Some(value);
				changed = true;
			}
		}
		if process.metadata.node.output.depth.is_none() {
			let value = output_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.depth)
				})
				.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
			if let Some(value) = value {
				process.metadata.node.output.depth = Some(value);
				changed = true;
			}
		}
		if process.metadata.node.output.size.is_none() {
			let value = output_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.size)
				})
				.sum::<Option<u64>>();
			if let Some(value) = value {
				process.metadata.node.output.size = Some(value);
				changed = true;
			}
		}
		if process.metadata.node.output.solvable.is_none() {
			let value = output_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.solvable)
				})
				.try_fold(false, |output, value| value.map(|value| output || value));
			if let Some(value) = value {
				process.metadata.node.output.solvable = Some(value);
				changed = true;
			}
		}
		if process.metadata.node.output.solved.is_none() {
			let value = output_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.solved)
				})
				.try_fold(true, |output, value| value.map(|value| output && value));
			if let Some(value) = value {
				process.metadata.node.output.solved = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.count.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.count)
				})
				.sum::<Option<u64>>();
			if let Some(value) = value {
				let value = 1 + value;
				process.metadata.subtree.count = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.command.count.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.command.count)
				})
				.fold(process.metadata.node.command.count, |output, value| {
					output.and_then(|output| value.map(|value| output + value))
				});
			if let Some(value) = value {
				process.metadata.subtree.command.count = Some(value);
				changed = true;
			}
		}
		if process.metadata.subtree.command.depth.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.command.depth)
				})
				.fold(process.metadata.node.command.depth, |output, value| {
					output.and_then(|output| value.map(|value| output.max(value)))
				});
			if let Some(value) = value {
				process.metadata.subtree.command.depth = Some(value);
				changed = true;
			}
		}
		if process.metadata.subtree.command.size.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.command.size)
				})
				.fold(process.metadata.node.command.size, |output, value| {
					output.and_then(|output| value.map(|value| output + value))
				});
			if let Some(value) = value {
				process.metadata.subtree.command.size = Some(value);
				changed = true;
			}
		}
		if process.metadata.subtree.command.solvable.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.command.solvable)
				})
				.fold(process.metadata.node.command.solvable, |output, value| {
					output.and_then(|output| value.map(|value| output || value))
				});
			if let Some(value) = value {
				process.metadata.subtree.command.solvable = Some(value);
				changed = true;
			}
		}
		if process.metadata.subtree.command.solved.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.command.solved)
				})
				.fold(process.metadata.node.command.solved, |output, value| {
					output.and_then(|output| value.map(|value| output && value))
				});
			if let Some(value) = value {
				process.metadata.subtree.command.solved = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.error.count.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.error.count)
				})
				.fold(process.metadata.node.error.count, |output, value| {
					output.and_then(|output| value.map(|value| output + value))
				});
			if let Some(value) = value {
				process.metadata.subtree.error.count = Some(value);
				changed = true;
			}
		}
		if process.metadata.subtree.error.depth.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.error.depth)
				})
				.fold(process.metadata.node.error.depth, |output, value| {
					output.and_then(|output| value.map(|value| output.max(value)))
				});
			if let Some(value) = value {
				process.metadata.subtree.error.depth = Some(value);
				changed = true;
			}
		}
		if process.metadata.subtree.error.size.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.error.size)
				})
				.fold(process.metadata.node.error.size, |output, value| {
					output.and_then(|output| value.map(|value| output + value))
				});
			if let Some(value) = value {
				process.metadata.subtree.error.size = Some(value);
				changed = true;
			}
		}
		if process.metadata.subtree.error.solvable.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.error.solvable)
				})
				.fold(process.metadata.node.error.solvable, |output, value| {
					output.and_then(|output| value.map(|value| output || value))
				});
			if let Some(value) = value {
				process.metadata.subtree.error.solvable = Some(value);
				changed = true;
			}
		}
		if process.metadata.subtree.error.solved.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.error.solved)
				})
				.fold(process.metadata.node.error.solved, |output, value| {
					output.and_then(|output| value.map(|value| output && value))
				});
			if let Some(value) = value {
				process.metadata.subtree.error.solved = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.log.count.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.log.count)
				})
				.fold(process.metadata.node.log.count, |output, value| {
					output.and_then(|output| value.map(|value| output + value))
				});
			if let Some(value) = value {
				process.metadata.subtree.log.count = Some(value);
				changed = true;
			}
		}
		if process.metadata.subtree.log.depth.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.log.depth)
				})
				.fold(process.metadata.node.log.depth, |output, value| {
					output.and_then(|output| value.map(|value| output.max(value)))
				});
			if let Some(value) = value {
				process.metadata.subtree.log.depth = Some(value);
				changed = true;
			}
		}
		if process.metadata.subtree.log.size.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.log.size)
				})
				.fold(process.metadata.node.log.size, |output, value| {
					output.and_then(|output| value.map(|value| output + value))
				});
			if let Some(value) = value {
				process.metadata.subtree.log.size = Some(value);
				changed = true;
			}
		}
		if process.metadata.subtree.log.solvable.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.log.solvable)
				})
				.fold(process.metadata.node.log.solvable, |output, value| {
					output.and_then(|output| value.map(|value| output || value))
				});
			if let Some(value) = value {
				process.metadata.subtree.log.solvable = Some(value);
				changed = true;
			}
		}
		if process.metadata.subtree.log.solved.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.log.solved)
				})
				.fold(process.metadata.node.log.solved, |output, value| {
					output.and_then(|output| value.map(|value| output && value))
				});
			if let Some(value) = value {
				process.metadata.subtree.log.solved = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.output.count.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.output.count)
				})
				.fold(process.metadata.node.output.count, |output, value| {
					output.and_then(|output| value.map(|value| output + value))
				});
			if let Some(value) = value {
				process.metadata.subtree.output.count = Some(value);
				changed = true;
			}
		}
		if process.metadata.subtree.output.depth.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.output.depth)
				})
				.fold(process.metadata.node.output.depth, |output, value| {
					output.and_then(|output| value.map(|value| output.max(value)))
				});
			if let Some(value) = value {
				process.metadata.subtree.output.depth = Some(value);
				changed = true;
			}
		}
		if process.metadata.subtree.output.size.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.output.size)
				})
				.fold(process.metadata.node.output.size, |output, value| {
					output.and_then(|output| value.map(|value| output + value))
				});
			if let Some(value) = value {
				process.metadata.subtree.output.size = Some(value);
				changed = true;
			}
		}
		if process.metadata.subtree.output.solvable.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.output.solvable)
				})
				.fold(process.metadata.node.output.solvable, |output, value| {
					output.and_then(|output| value.map(|value| output || value))
				});
			if let Some(value) = value {
				process.metadata.subtree.output.solvable = Some(value);
				changed = true;
			}
		}
		if process.metadata.subtree.output.solved.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.output.solved)
				})
				.fold(process.metadata.node.output.solved, |output, value| {
					output.and_then(|output| value.map(|value| output && value))
				});
			if let Some(value) = value {
				process.metadata.subtree.output.solved = Some(value);
				changed = true;
			}
		}

		if let Some(object) = &command_object
			&& !process.stored.node_command
			&& object.stored.subtree
		{
			process.stored.node_command = true;
			changed = true;
		}

		if !process.stored.node_error {
			let value = error_objects
				.iter()
				.all(|option| option.as_ref().is_some_and(|object| object.stored.subtree));
			if value {
				process.stored.node_error = true;
				changed = true;
			}
		}

		if let Some(Some(object)) = &log_object {
			if !process.stored.node_log && object.stored.subtree {
				process.stored.node_log = true;
				changed = true;
			}
		} else if log_object.is_none() && !process.stored.node_log {
			process.stored.node_log = true;
			changed = true;
		}

		if !process.stored.node_output {
			let value = output_objects
				.iter()
				.all(|option| option.as_ref().is_some_and(|object| object.stored.subtree));
			if value {
				process.stored.node_output = true;
				changed = true;
			}
		}

		if !process.stored.subtree {
			let value = children
				.iter()
				.all(|child| child.as_ref().is_some_and(|child| child.stored.subtree));
			if value {
				process.stored.subtree = true;
				changed = true;
			}
		}

		if !process.stored.subtree_command && process.stored.node_command {
			let value = children.iter().all(|child| {
				child
					.as_ref()
					.is_some_and(|child| child.stored.subtree_command)
			});
			if value {
				process.stored.subtree_command = true;
				changed = true;
			}
		}

		if !process.stored.subtree_error && process.stored.node_error {
			let value = children.iter().all(|child| {
				child
					.as_ref()
					.is_some_and(|child| child.stored.subtree_error)
			});
			if value {
				process.stored.subtree_error = true;
				changed = true;
			}
		}

		if !process.stored.subtree_log && process.stored.node_log {
			let value = children
				.iter()
				.all(|child| child.as_ref().is_some_and(|child| child.stored.subtree_log));
			if value {
				process.stored.subtree_log = true;
				changed = true;
			}
		}

		if !process.stored.subtree_output && process.stored.node_output {
			let value = children.iter().all(|child| {
				child
					.as_ref()
					.is_some_and(|child| child.stored.subtree_output)
			});
			if value {
				process.stored.subtree_output = true;
				changed = true;
			}
		}

		if changed {
			let value = process.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|source| tg::error!(!source, %id, "failed to put the process"))?;
		}

		Ok(changed)
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
		if let Some(existing) = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to get update key"))?
		{
			let existing = existing
				.first()
				.and_then(|value| Update::from_u8(*value))
				.unwrap_or(Update::Put);
			if existing == Update::Propagate && update == Update::Put {
				let value = [update.to_u8().unwrap()];
				db.put(transaction, &key, &value)
					.map_err(|source| tg::error!(!source, "failed to put update key"))?;
			}
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
