use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	num::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_sandbox_process_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		sandbox: &tg::sandbox::Id,
		process: &tg::process::Id,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		// Preserve the first indexed position when initialization is replayed.
		let key = Key::Sandbox(super::Key::SandboxProcessPosition {
			process: process.clone(),
			sandbox: sandbox.clone(),
		});
		let key = Self::pack(subspace, &key);
		if {
			let result = txn.get(&key, false).await;
			crate::fdb::retry!(result)
		}
		.is_some()
		{
			return Ok(ControlFlow::Break(()));
		}

		// Append after the last indexed process.
		let prefix = Self::pack(
			subspace,
			&(
				Kind::SandboxProcessEntry.to_i32().unwrap(),
				sandbox.to_bytes().as_ref(),
			),
		);
		let (begin, end) = fdbt::Subspace::from_bytes(prefix).range();
		let selector = fdb::KeySelector::last_less_than(end);
		let result = txn.get_key(&selector, false).await;
		let last = crate::fdb::retry!(result);
		let position = if last.as_ref() < begin.as_slice() {
			0
		} else {
			let Key::Sandbox(super::Key::SandboxProcessEntry { position, .. }) =
				Self::unpack(subspace, &last)?
			else {
				return Err(tg::error!("unexpected key type"));
			};
			position
				.checked_add(1)
				.ok_or_else(|| tg::error!("the sandbox has too many processes"))?
		};
		txn.set(&key, &position.to_be_bytes());
		let key = Key::Sandbox(super::Key::SandboxProcessEntry {
			position,
			process: process.clone(),
			sandbox: sandbox.clone(),
		});
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);

		Ok(ControlFlow::Break(()))
	}

	pub(crate) fn put_sandbox_processes_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		sandbox: &tg::sandbox::Id,
		processes: &[tg::process::Id],
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		crate::fdb::propagate!(Self::delete_sandbox_processes_with_transaction(
			txn, subspace, sandbox
		));
		for (position, process) in processes.iter().enumerate() {
			let position = i64::try_from(position)
				.map_err(|error| tg::error!(!error, "the sandbox has too many processes"))?;
			let key = Key::Sandbox(super::Key::SandboxProcessEntry {
				position,
				process: process.clone(),
				sandbox: sandbox.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
			let key = Key::Sandbox(super::Key::SandboxProcessPosition {
				process: process.clone(),
				sandbox: sandbox.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &position.to_be_bytes());
		}

		Ok(ControlFlow::Break(()))
	}

	#[allow(clippy::unnecessary_wraps)]
	pub(crate) fn delete_sandbox_processes_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for kind in [Kind::SandboxProcessEntry, Kind::SandboxProcessPosition] {
			let prefix = Self::pack(
				subspace,
				&(kind.to_i32().unwrap(), sandbox.to_bytes().as_ref()),
			);
			let (begin, end) = fdbt::Subspace::from_bytes(prefix).range();
			txn.clear_range(&begin, &end);
		}

		Ok(ControlFlow::Break(()))
	}
}
