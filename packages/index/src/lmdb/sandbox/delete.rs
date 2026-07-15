use {
	crate::lmdb::{Db, Index, Key, Request},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_sandboxes(&self, ids: &[tg::sandbox::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteSandboxes(ids.to_vec());
		self.sender_high
			.as_ref()
			.unwrap()
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub(crate) fn task_delete_sandboxes(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::sandbox::Id],
	) -> tg::Result<()> {
		for id in ids {
			let key = Key::Sandbox(crate::lmdb::sandbox::Key::Sandbox(id.clone()));
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the sandbox"))?;
		}
		Ok(())
	}
}
