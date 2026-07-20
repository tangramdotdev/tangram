use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn get_sandbox_processes(
		&self,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Vec<(tg::process::Id, crate::process::Process)>> {
		let sandbox = sandbox.clone();
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let subspace = self.subspace.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let sandbox = sandbox.to_bytes();
				let prefix = Self::pack(
					&subspace,
					&(Kind::SandboxProcess.to_i32().unwrap(), sandbox.as_ref()),
				);
				let iter = db
					.prefix_iter(&transaction, &prefix)
					.map_err(|error| tg::error!(!error, "failed to get the sandbox processes"))?;
				let mut output = Vec::new();
				for entry in iter {
					let (key, _) = entry
						.map_err(|error| tg::error!(!error, "failed to read a sandbox process"))?;
					let key = Self::unpack(&subspace, key)?;
					let Key::Sandbox(crate::lmdb::sandbox::Key::SandboxProcess { process, .. }) =
						key
					else {
						return Err(tg::error!("unexpected key type"));
					};
					let data = Self::try_get_process_with_transaction(
						&db,
						&subspace,
						&transaction,
						&process,
					)?
					.ok_or_else(|| tg::error!(%process, "failed to find the sandbox process"))?;
					output.push((process, data));
				}
				Ok(output)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	pub async fn try_get_sandboxes(
		&self,
		ids: &[tg::sandbox::Id],
	) -> tg::Result<Vec<Option<crate::sandbox::Sandbox>>> {
		let ids = ids.to_owned();
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let subspace = self.subspace.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				ids.iter()
					.map(|id| {
						Self::try_get_sandbox_with_transaction(&db, &subspace, &transaction, id)
					})
					.collect()
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	pub(crate) fn try_get_sandbox_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<crate::sandbox::Sandbox>> {
		let key = Key::Sandbox(crate::lmdb::sandbox::Key::Sandbox(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox"))?;
		bytes.map(crate::sandbox::Sandbox::deserialize).transpose()
	}
}
