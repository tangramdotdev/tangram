use {
	crate::{Index, Key, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
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

	pub(crate) async fn delete_sandboxes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		ids: &[tg::sandbox::Id],
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for id in ids {
			crate::propagate!(
				Self::delete_sandbox_processes_with_transaction(txn, subspace, id, partition_total)
					.await
			);

			// Delete the creator and owner keys.
			let sandbox =
				crate::propagate!(Self::try_get_sandbox_with_transaction(txn, subspace, id).await);
			if let Some(data) = sandbox.and_then(|sandbox| sandbox.data) {
				for key in crate::sandbox::Key::principals(
					&data.data.creator,
					&data.data.owner,
					data.data.status,
					id,
				) {
					let key = Self::pack(subspace, &Key::Sandbox(key));
					txn.clear(&key);
				}
			}

			// Delete the sandbox.
			let key = Key::Sandbox(crate::sandbox::Key::Sandbox(id.clone()));
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}
		Ok(ControlFlow::Break(()))
	}
}
