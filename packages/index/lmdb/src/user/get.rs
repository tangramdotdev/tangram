use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn try_get_users_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::user::Id],
	) -> tg::Result<Vec<Option<tangram_index::user::User>>> {
		ids.iter()
			.map(|id| Self::try_get_user_with_transaction(db, subspace, transaction, id))
			.collect()
	}

	pub(crate) fn try_get_user_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::user::Id,
	) -> tg::Result<Option<tangram_index::user::User>> {
		let key = Key::User(crate::user::Key::User(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the user"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(tangram_index::user::User::deserialize(bytes)?))
	}
}
