use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_objects(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<crate::object::Object>>> {
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
						Self::try_get_object_with_transaction(&db, &subspace, &transaction, id)?;
					outputs.push(option);
				}
				Ok(outputs)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	pub(crate) fn try_get_object_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Option<crate::object::Object>> {
		let key = Key::Object(crate::lmdb::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(crate::object::Object::deserialize(bytes)?))
	}

	pub(crate) fn get_object_children_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Vec<tg::object::Id>> {
		let id_bytes = id.to_bytes();
		let prefix = &(Kind::ObjectChild.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, prefix);
		let mut children = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get object children"))?;
		for entry in iter {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read object child entry"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Object(crate::lmdb::object::Key::ObjectChild { child, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			children.push(child);
		}
		Ok(children)
	}

	pub(crate) fn get_object_parents_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Vec<tg::object::Id>> {
		let id_bytes = id.to_bytes();
		let prefix = &(Kind::ChildObject.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, prefix);
		let mut parents = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get object parents"))?;
		for entry in iter {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read child object entry"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Object(crate::lmdb::object::Key::ChildObject { object, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			parents.push(object);
		}
		Ok(parents)
	}

	pub(crate) fn get_object_processes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, crate::process::object::Kind)>> {
		let id_bytes = id.to_bytes();
		let prefix = &(Kind::ObjectProcess.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, prefix);
		let mut parents = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get object process parents"))?;
		for entry in iter {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read object process entry"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Object(crate::lmdb::object::Key::ObjectProcess { kind, process, .. }) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			parents.push((process, kind));
		}
		Ok(parents)
	}
}
