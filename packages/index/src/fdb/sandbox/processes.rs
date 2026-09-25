use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::TryStreamExt as _,
	num::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_sandbox_processes_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		sandbox: &tg::sandbox::Id,
		processes: &[tg::process::Id],
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		crate::fdb::propagate!(
			Self::delete_sandbox_processes_with_transaction(
				txn,
				subspace,
				sandbox,
				partition_total
			)
			.await
		);
		for (position, process) in processes.iter().enumerate() {
			let position = i64::try_from(position)
				.map_err(|error| tg::error!(!error, "the sandbox has too many processes"))?;
			let key = Key::Sandbox(super::Key::SandboxProcess {
				position,
				process: process.clone(),
				sandbox: sandbox.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
			let key = Key::Process(crate::fdb::process::Key::ProcessSandbox {
				process: process.clone(),
				sandbox: sandbox.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &position.to_be_bytes());
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn delete_sandbox_processes_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		sandbox: &tg::sandbox::Id,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let prefix = Self::pack(
			subspace,
			&(
				Kind::SandboxProcess.to_i32().unwrap(),
				sandbox.to_bytes().as_ref(),
			),
		);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&fdbt::Subspace::from_bytes(prefix))
		};
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::fdb::retry!(result);
		for entry in entries {
			let Key::Sandbox(super::Key::SandboxProcess { process, .. }) =
				Self::unpack(subspace, entry.key())?
			else {
				return Err(tg::error!("unexpected key type"));
			};
			txn.clear(entry.key());
			let key = Key::Process(crate::fdb::process::Key::ProcessSandbox {
				process: process.clone(),
				sandbox: sandbox.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
			crate::fdb::propagate!(
				Self::decrement_process_reference_count(txn, subspace, &process, partition_total)
					.await
			);
		}

		Ok(ControlFlow::Break(()))
	}
}
