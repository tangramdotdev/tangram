use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	std::collections::{BTreeSet, VecDeque},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn get_process_depth_detections(
		&self,
		limit: usize,
	) -> tg::Result<Vec<tg::process::Id>> {
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let subspace = self.subspace.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let prefix = &(Kind::ProcessDepthDetection.to_i32().unwrap(),);
				let prefix = Self::pack(&subspace, prefix);
				let iter = db.prefix_iter(&transaction, &prefix).map_err(|error| {
					tg::error!(!error, "failed to get the process depth detections")
				})?;
				iter.take(limit)
					.map(|entry| {
						let (key, _) = entry.map_err(|error| {
							tg::error!(!error, "failed to read a process depth detection")
						})?;
						let key = Self::unpack(&subspace, key)?;
						let Key::Process(crate::lmdb::process::Key::ProcessDepthDetection(process)) =
							key
						else {
							return Err(tg::error!("unexpected key type"));
						};
						Ok(process)
					})
					.collect()
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	pub async fn try_get_cached_processes(
		&self,
		command: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, crate::process::Process)>> {
		tokio::task::spawn_blocking({
			let command = command.clone();
			let db = self.db;
			let env = self.env.clone();
			let subspace = self.subspace.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let command_bytes = command.to_bytes();
				let prefix = &(
					Kind::CommandProcess.to_i32().unwrap(),
					command_bytes.as_ref(),
				);
				let prefix = Self::pack(&subspace, prefix);
				let iter = db
					.prefix_iter(&transaction, &prefix)
					.map_err(|error| tg::error!(!error, "failed to get the cached processes"))?;
				let mut output = Vec::new();
				for entry in iter {
					let (key, _) = entry.map_err(|error| {
						tg::error!(!error, "failed to read a cached process entry")
					})?;
					let key = Self::unpack(&subspace, key)?;
					let Key::Process(crate::lmdb::process::Key::CommandProcess { process, .. }) =
						key
					else {
						return Err(tg::error!("unexpected key type"));
					};
					let Some(data) = Self::try_get_process_with_transaction(
						&db,
						&subspace,
						&transaction,
						&process,
					)?
					else {
						continue;
					};
					if data.data.is_none() {
						continue;
					}
					output.push((process, data));
				}
				output.sort_unstable_by(|(a_id, a), (b_id, b)| {
					let a_created_at = a.data.as_ref().unwrap().created_at;
					let b_created_at = b.data.as_ref().unwrap().created_at;
					b_created_at.cmp(&a_created_at).then_with(|| b_id.cmp(a_id))
				});
				Ok(output)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	pub async fn process_has_ancestor(
		&self,
		process: &tg::process::Id,
		ancestor: &tg::process::Id,
	) -> tg::Result<bool> {
		if process == ancestor {
			return Ok(true);
		}
		tokio::task::spawn_blocking({
			let db = self.db;
			let ancestor = ancestor.clone();
			let env = self.env.clone();
			let process = process.clone();
			let subspace = self.subspace.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let mut seen = BTreeSet::from([process.clone()]);
				let mut queue = VecDeque::from([process]);
				while let Some(process) = queue.pop_front() {
					for parent in Self::get_process_parents_with_transaction(
						&db,
						&subspace,
						&transaction,
						&process,
					)? {
						if parent == ancestor {
							return Ok(true);
						}
						if seen.insert(parent.clone()) {
							queue.push_back(parent);
						}
					}
				}
				Ok(false)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	pub async fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let subspace = self.subspace.clone();
			let ids = ids.to_owned();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let mut outputs = Vec::with_capacity(ids.len());
				for id in &ids {
					let option =
						Self::try_get_process_with_transaction(&db, &subspace, &transaction, id)?;
					outputs.push(option);
				}
				Ok(outputs)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	pub(crate) fn try_get_process_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Option<crate::process::Process>> {
		let key = Key::Process(crate::lmdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(crate::process::Process::deserialize(bytes)?))
	}

	pub(crate) fn get_process_children_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::process::Id>> {
		let id_bytes = id.to_bytes();
		let prefix = &(Kind::ProcessChild.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, prefix);
		let mut children = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get process children"))?;
		for entry in iter {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read process child entry"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Process(crate::lmdb::process::Key::ProcessChild { child, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			children.push(child);
		}
		Ok(children)
	}

	pub(crate) fn get_process_parents_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::process::Id>> {
		let id_bytes = id.to_bytes();
		let prefix = &(Kind::ChildProcess.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, prefix);
		let mut parents = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get process parents"))?;
		for entry in iter {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read child process entry"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Process(crate::lmdb::process::Key::ChildProcess { parent, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			parents.push(parent);
		}
		Ok(parents)
	}

	pub(crate) fn get_process_objects_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Vec<(tg::object::Id, crate::process::object::Kind)>> {
		let id_bytes = id.to_bytes();
		let prefix = &(Kind::ProcessObject.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, prefix);
		let mut objects = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get process objects"))?;
		for entry in iter {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read process object entry"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Process(crate::lmdb::process::Key::ProcessObject { kind, object, .. }) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			objects.push((object, kind));
		}
		Ok(objects)
	}
}
