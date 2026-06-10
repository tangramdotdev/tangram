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
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteUsers(ids.to_vec());
		self.sender_high
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected response"));
		};
		Ok(())
	}

	pub(crate) fn task_delete_users(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		ids: &[tg::user::Id],
	) -> tg::Result<()> {
		for id in ids {
			let key = Key::User(crate::fdb::user::Key::User(id.clone()));
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}
		Ok(())
	}
}
