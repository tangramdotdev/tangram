#![allow(clippy::unnecessary_wraps)]

use {
	crate::fdb::{Index, Key, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
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

	pub(crate) async fn delete_users_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		ids: &[tg::user::Id],
	) -> crate::fdb::Result<()> {
		for id in ids {
			let key = Key::User(crate::fdb::user::Key::User(id.clone()));
			let key = Self::pack(subspace, &key);
			let user = txn
				.get(&key, false)
				.await?
				.map(|bytes| crate::user::User::deserialize(&bytes))
				.transpose()
				.map_err(crate::fdb::custom_error)?;
			if let Some(user) = user {
				let node_key = Key::Node(crate::fdb::node::Key::Node(user.specifier));
				let node_key = Self::pack(subspace, &node_key);
				txn.clear(&node_key);
			}
			txn.clear(&key);
		}
		Ok(())
	}
}
