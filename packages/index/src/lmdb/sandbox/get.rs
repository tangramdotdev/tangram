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
		let request = crate::read::Request::GetSandboxProcesses {
			sandbox: sandbox.clone(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::GetSandboxProcesses(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn get_sandbox_processes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Vec<(tg::process::Id, crate::process::Process)>> {
		let sandbox = sandbox.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::SandboxProcess.to_i32().unwrap(), sandbox.as_ref()),
		);
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the sandbox processes"))?;
		let mut output = Vec::new();
		for entry in iter {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read a sandbox process"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Sandbox(crate::lmdb::sandbox::Key::SandboxProcess { process, .. }) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			let data = Self::try_get_process_with_transaction(db, subspace, transaction, &process)?
				.ok_or_else(|| tg::error!(%process, "failed to find the sandbox process"))?;
			output.push((process, data));
		}

		Ok(output)
	}

	pub async fn try_get_sandboxes(
		&self,
		ids: &[tg::sandbox::Id],
	) -> tg::Result<Vec<Option<crate::sandbox::Sandbox>>> {
		let request = crate::read::Request::TryGetSandboxes {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetSandboxes(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn try_get_sandboxes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::sandbox::Id],
	) -> tg::Result<Vec<Option<crate::sandbox::Sandbox>>> {
		ids.iter()
			.map(|id| Self::try_get_sandbox_with_transaction(db, subspace, transaction, id))
			.collect()
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
