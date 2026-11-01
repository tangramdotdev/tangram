use {
	crate::{Db, Index, Key, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
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
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::sandbox::Id],
	) -> tg::Result<()> {
		for id in ids {
			Self::delete_sandbox_processes_with_transaction(db, subspace, transaction, id)?;

			// Delete the creator and owner keys.
			let sandbox = Self::try_get_sandbox_with_transaction(db, subspace, transaction, id)?;
			if let Some(data) = sandbox.and_then(|sandbox| sandbox.data) {
				for key in crate::sandbox::Key::principals(
					&data.data.creator,
					&data.data.owner,
					data.data.status,
					id,
				) {
					let key = Self::pack(subspace, &Key::Sandbox(key));
					db.delete(transaction, &key).map_err(|error| {
						tg::error!(!error, "failed to delete a sandbox principal key")
					})?;
				}
			}

			// Delete the sandbox.
			let key = Key::Sandbox(crate::sandbox::Key::Sandbox(id.clone()));
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the sandbox"))?;
		}
		Ok(())
	}
}
