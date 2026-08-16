#![allow(clippy::unnecessary_wraps)]

use {
	crate::fdb::{Index, Key, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
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
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		ids: &[tg::user::Id],
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for id in ids {
			let key = Key::User(crate::fdb::user::Key::User(id.clone()));
			let key = Self::pack(subspace, &key);
			let result = txn.get(&key, false).await;
			let user = crate::fdb::retry!(result)
				.map(|bytes| crate::user::User::deserialize(&bytes))
				.transpose()?;
			if let Some(user) = user {
				let node_key = Key::Node(crate::fdb::node::Key::Node(user.specifier));
				let node_key = Self::pack(subspace, &node_key);
				txn.clear(&node_key);
			}
			txn.clear(&key);
		}
		Ok(ControlFlow::Break(()))
	}
}
