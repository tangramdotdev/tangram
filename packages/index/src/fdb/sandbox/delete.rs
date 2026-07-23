#![allow(clippy::unnecessary_wraps)]

use {
	crate::fdb::{Index, Key, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_sandboxes(&self, ids: &[tg::sandbox::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let request = Request::DeleteSandboxes(ids.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}

	pub(crate) fn delete_sandboxes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		ids: &[tg::sandbox::Id],
	) -> tg::Result<()> {
		for id in ids {
			let key = Key::Sandbox(crate::fdb::sandbox::Key::Sandbox(id.clone()));
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}
		Ok(())
	}
}
