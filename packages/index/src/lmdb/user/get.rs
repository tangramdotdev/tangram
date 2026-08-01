use {
	crate::lmdb::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_users(
		&self,
		ids: &[tg::user::Id],
	) -> tg::Result<Vec<Option<crate::user::User>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = crate::read::Request::TryGetUsers {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetUsers(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn try_get_users_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::user::Id],
	) -> tg::Result<Vec<Option<crate::user::User>>> {
		ids.iter()
			.map(|id| Self::try_get_user_with_transaction(db, subspace, transaction, id))
			.collect()
	}

	pub(crate) fn try_get_user_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::user::Id,
	) -> tg::Result<Option<crate::user::User>> {
		let key = Key::User(crate::lmdb::user::Key::User(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the user"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(crate::user::User::deserialize(bytes)?))
	}
}
