use {
	crate::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
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
			crate::propagate!(
				Self::delete_sandbox_scheduler_with_transaction(txn, subspace, id).await
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
