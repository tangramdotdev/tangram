use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn try_get_objects_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tangram_index::object::Object>>> {
		ids.iter()
			.map(|id| Self::try_get_object_with_transaction(db, subspace, transaction, id))
			.collect()
	}

	pub(crate) fn try_get_object_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Option<tangram_index::object::Object>> {
		let key = Key::Object(crate::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(tangram_index::object::Object::deserialize(bytes)?))
	}

	pub(crate) fn get_object_children_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<crate::page::Page<tg::object::Id>> {
		let bytes = id.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::ObjectChild.to_i32().unwrap(), bytes.as_ref()),
		);
		let (entries, cursor) = Self::get_entry_page_with_transaction(
			db,
			subspace,
			transaction,
			&prefix,
			after,
			limit,
		)?;
		let items = entries
			.into_iter()
			.map(|(key, _)| {
				let Key::Object(crate::object::Key::ObjectChild { child, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok((items, cursor))
	}

	pub(crate) fn get_object_parents_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<crate::page::Page<tg::object::Id>> {
		let bytes = id.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::ChildObject.to_i32().unwrap(), bytes.as_ref()),
		);
		let (entries, cursor) = Self::get_entry_page_with_transaction(
			db,
			subspace,
			transaction,
			&prefix,
			after,
			limit,
		)?;
		let items = entries
			.into_iter()
			.map(|(key, _)| {
				let Key::Object(crate::object::Key::ChildObject { object, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(object)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok((items, cursor))
	}

	pub(crate) fn get_object_processes_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<crate::page::Page<(tg::process::Id, tangram_index::process::object::Kind)>> {
		let bytes = id.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::ObjectProcess.to_i32().unwrap(), bytes.as_ref()),
		);
		let (entries, cursor) = Self::get_entry_page_with_transaction(
			db,
			subspace,
			transaction,
			&prefix,
			after,
			limit,
		)?;
		let items = entries
			.into_iter()
			.map(|(key, _)| {
				let Key::Object(crate::object::Key::ObjectProcess { kind, process, .. }) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((process, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok((items, cursor))
	}
}
