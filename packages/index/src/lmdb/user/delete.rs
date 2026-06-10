use {
	crate::lmdb::{Db, Index, Key, Request},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_users(&self, ids: &[tg::user::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteUsers(ids.to_vec());
		self.sender_high
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub(crate) fn task_delete_users(
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
