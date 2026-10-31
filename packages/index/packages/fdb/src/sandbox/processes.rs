use {
	crate::{Index, Key, Kind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::TryStreamExt as _,
	num::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_sandbox_process_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		sandbox: &tg::sandbox::Id,
		process: &tg::process::Id,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		// A replay must not recreate a deleted sandbox or change the list of a destroyed sandbox.
		let indexed =
			crate::propagate!(Self::try_get_sandbox_with_transaction(txn, subspace, sandbox).await);
		if indexed.is_none_or(|sandbox| {
			sandbox
				.data
				.as_ref()
				.is_some_and(|data| data.data.status.is_destroyed())
		}) {
			return Ok(ControlFlow::Break(()));
		}
		crate::propagate!(
			Self::append_sandbox_process_with_transaction(txn, subspace, sandbox, process).await
		);

		Ok(ControlFlow::Break(()))
	}

	async fn append_sandbox_process_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		sandbox: &tg::sandbox::Id,
		process: &tg::process::Id,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		// Preserve the first indexed position when initialization is replayed.
		let key = Key::Process(crate::process::Key::ProcessSandbox {
			process: process.clone(),
			sandbox: sandbox.clone(),
		});
		let key = Self::pack(subspace, &key);
		if {
			let result = txn.get(&key, false).await;
			crate::retry!(result)
		}
		.is_some()
		{
			return Ok(ControlFlow::Break(()));
		}

		// Append after the last indexed process.
		let prefix = Self::pack(
			subspace,
			&(
				Kind::SandboxProcess.to_i32().unwrap(),
				sandbox.to_bytes().as_ref(),
			),
		);
		let (begin, end) = fdbt::Subspace::from_bytes(prefix).range();
		let selector = fdb::KeySelector::last_less_than(end);
		let result = txn.get_key(&selector, false).await;
		let last = crate::retry!(result);
		let position = if last.as_ref() < begin.as_slice() {
			0
		} else {
			let Key::Sandbox(super::Key::SandboxProcess { position, .. }) =
				Self::unpack(subspace, &last)?
			else {
				return Err(tg::error!("unexpected key type"));
			};
			position
				.checked_add(1)
				.ok_or_else(|| tg::error!("the sandbox has too many processes"))?
		};
		txn.set(&key, &position.to_be_bytes());
		let key = Key::Sandbox(super::Key::SandboxProcess {
			position,
			process: process.clone(),
			sandbox: sandbox.clone(),
		});
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn put_sandbox_processes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		sandbox: &tg::sandbox::Id,
		processes: &[tg::process::Id],
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		// Append the processes that are not indexed, so that the lists reported by each boot merge.
		for process in processes {
			crate::propagate!(
				Self::append_sandbox_process_with_transaction(txn, subspace, sandbox, process)
					.await
			);
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn delete_sandbox_processes_with_transaction(
		txn: &fdb::Transaction,
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
		let entries = crate::retry!(result);
		for entry in entries {
			let Key::Sandbox(super::Key::SandboxProcess { process, .. }) =
				Self::unpack(subspace, entry.key())?
			else {
				return Err(tg::error!("unexpected key type"));
			};
			txn.clear(entry.key());
			let key = Key::Process(crate::process::Key::ProcessSandbox {
				process: process.clone(),
				sandbox: sandbox.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
			crate::propagate!(
				Self::decrement_process_reference_count(txn, subspace, &process, partition_total)
					.await
			);
		}

		Ok(ControlFlow::Break(()))
	}
}
