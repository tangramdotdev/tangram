use {
	crate::lmdb::{Db, Index, Key, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_users(&self, ids: &[tg::user::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let request = Request::DeleteUsers(ids.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(())
	}

	pub(crate) fn delete_users_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::user::Id],
	) -> tg::Result<()> {
		for id in ids {
			let key = Key::User(crate::lmdb::user::Key::User(id.clone()));
			let key = Self::pack(subspace, &key);
			let user = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the user"))?
				.map(crate::user::User::deserialize)
				.transpose()?;
			if let Some(user) = user {
				let key = Key::Node(crate::lmdb::node::Key::Node(user.specifier));
				let key = Self::pack(subspace, &key);
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete the node"))?;
			}
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the user"))?;
		}
		Ok(())
	}
}
