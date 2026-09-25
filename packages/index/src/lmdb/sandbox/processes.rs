use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_sandbox_processes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		sandbox: &tg::sandbox::Id,
		processes: &[tg::process::Id],
	) -> tg::Result<()> {
		Self::delete_sandbox_processes_with_transaction(db, subspace, transaction, sandbox)?;
		for (position, process) in processes.iter().enumerate() {
			let position = i64::try_from(position)
				.map_err(|error| tg::error!(!error, "the sandbox has too many processes"))?;
			let key = Key::Sandbox(super::Key::SandboxProcess {
				position,
				process: process.clone(),
				sandbox: sandbox.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the sandbox process entry"))?;
			let key = Key::Process(crate::lmdb::process::Key::ProcessSandbox {
				process: process.clone(),
				sandbox: sandbox.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &position.to_be_bytes())
				.map_err(|error| tg::error!(!error, "failed to put the sandbox process entry"))?;
		}

		Ok(())
	}

	pub(crate) fn delete_sandbox_processes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<()> {
		let prefix = Self::pack(
			subspace,
			&(
				Kind::SandboxProcess.to_i32().unwrap(),
				sandbox.to_bytes().as_ref(),
			),
		);
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the sandbox processes"))?
			.map(|entry| {
				let (key, _) = entry
					.map_err(|error| tg::error!(!error, "failed to read a sandbox process"))?;
				let Key::Sandbox(super::Key::SandboxProcess { process, .. }) =
					Self::unpack(subspace, key)?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((key.to_vec(), process))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		for (key, process) in entries {
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the sandbox process"))?;
			let key = Key::Process(crate::lmdb::process::Key::ProcessSandbox {
				process: process.clone(),
				sandbox: sandbox.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the process sandbox"))?;
			Self::decrement_process_reference_count(db, subspace, transaction, &process)?;
		}

		Ok(())
	}
}
