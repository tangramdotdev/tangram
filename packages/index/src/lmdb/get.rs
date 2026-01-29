use {
	super::{Db, Index, Key, KeyKind},
	crate::{Object, Process, ProcessObjectKind},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_objects(&self, ids: &[tg::object::Id]) -> tg::Result<Vec<Option<Object>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let ids = ids.to_owned();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let mut outputs = Vec::with_capacity(ids.len());
				for id in &ids {
					let option = Self::try_get_object_with_transaction(&db, &transaction, id)?;
					outputs.push(option);
				}
				Ok(outputs)
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}

	pub async fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<Process>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let ids = ids.to_owned();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let mut outputs = Vec::with_capacity(ids.len());
				for id in &ids {
					let option = Self::try_get_process_with_transaction(&db, &transaction, id)?;
					outputs.push(option);
				}
				Ok(outputs)
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}

	pub(super) fn try_get_object_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Option<Object>> {
		let key = Key::Object(id.clone()).pack_to_vec();
		let bytes = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(Object::deserialize(bytes)?))
	}

	pub(super) fn try_get_process_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Option<Process>> {
		let key = Key::Process(id.clone()).pack_to_vec();
		let bytes = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(Process::deserialize(bytes)?))
	}

	pub(super) fn get_object_children_with_transaction(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Vec<tg::object::Id>> {
		let prefix = (
			KeyKind::ObjectChild.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let mut children = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to get object children"))?;
		for entry in iter {
			let (key, _) =
				entry.map_err(|source| tg::error!(!source, "failed to read object child entry"))?;
			let key =
				fdbt::unpack(key).map_err(|source| tg::error!(!source, "failed to unpack key"))?;
			let Key::ObjectChild { child, .. } = key else {
				return Err(tg::error!("unexpected key type"));
			};
			children.push(child);
		}
		Ok(children)
	}

	pub(super) fn get_object_parents_with_transaction(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Vec<tg::object::Id>> {
		let prefix = (
			KeyKind::ChildObject.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let mut parents = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to get object parents"))?;
		for entry in iter {
			let (key, _) =
				entry.map_err(|source| tg::error!(!source, "failed to read child object entry"))?;
			let key =
				fdbt::unpack(key).map_err(|source| tg::error!(!source, "failed to unpack key"))?;
			let Key::ChildObject { object, .. } = key else {
				return Err(tg::error!("unexpected key type"));
			};
			parents.push(object);
		}
		Ok(parents)
	}

	pub(super) fn get_object_processes_with_transaction(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, ProcessObjectKind)>> {
		let prefix = (
			KeyKind::ObjectProcess.to_i32().unwrap(),
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
			let key =
				fdbt::unpack(key).map_err(|source| tg::error!(!source, "failed to unpack key"))?;
			let Key::ObjectProcess { kind, process, .. } = key else {
				return Err(tg::error!("unexpected key type"));
			};
			parents.push((process, kind));
		}
		Ok(parents)
	}

	pub(super) fn get_process_children_with_transaction(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::process::Id>> {
		let prefix = (
			KeyKind::ProcessChild.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let mut children = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to get process children"))?;
		for entry in iter {
			let (key, _) = entry
				.map_err(|source| tg::error!(!source, "failed to read process child entry"))?;
			let key =
				fdbt::unpack(key).map_err(|source| tg::error!(!source, "failed to unpack key"))?;
			let Key::ProcessChild { child, .. } = key else {
				return Err(tg::error!("unexpected key type"));
			};
			children.push(child);
		}
		Ok(children)
	}

	pub(super) fn get_process_parents_with_transaction(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::process::Id>> {
		let prefix = (
			KeyKind::ChildProcess.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let mut parents = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to get process parents"))?;
		for entry in iter {
			let (key, _) = entry
				.map_err(|source| tg::error!(!source, "failed to read child process entry"))?;
			let key =
				fdbt::unpack(key).map_err(|source| tg::error!(!source, "failed to unpack key"))?;
			let Key::ChildProcess { parent, .. } = key else {
				return Err(tg::error!("unexpected key type"));
			};
			parents.push(parent);
		}
		Ok(parents)
	}

	pub(super) fn get_process_objects_with_transaction(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Vec<(tg::object::Id, ProcessObjectKind)>> {
		let prefix = (
			KeyKind::ProcessObject.to_i32().unwrap(),
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
			let key =
				fdbt::unpack(key).map_err(|source| tg::error!(!source, "failed to unpack key"))?;
			let Key::ProcessObject { kind, object, .. } = key else {
				return Err(tg::error!("unexpected key type"));
			};
			objects.push((object, kind));
		}
		Ok(objects)
	}
}
