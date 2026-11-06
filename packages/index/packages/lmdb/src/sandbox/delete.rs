use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_sandboxes(&self, ids: &[tg::sandbox::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let items = ids
			.iter()
			.map(|id| {
				let arg = tangram_index::sandbox::delete::Arg { id: id.clone() };
				tangram_index::Item::DeleteSandbox(Box::new(arg))
			})
			.collect();
		let arg = tangram_index::write::Arg { items };
		self.write(arg).await??;

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
