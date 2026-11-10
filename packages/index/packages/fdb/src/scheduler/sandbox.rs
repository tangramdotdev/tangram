use {
	crate::{Index, Key},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_index::scheduler::sandbox::SandboxScheduler,
};

impl Index {
	pub async fn try_get_sandbox_scheduler(
		&self,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::scheduler::Id>> {
		let request = tangram_index::read::Request::TryGetSandboxScheduler {
			sandbox: sandbox.clone(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetSandboxScheduler(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn try_get_sandbox_scheduler_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<ControlFlow<Option<tg::scheduler::Id>, fdb::FdbError>> {
		let record = crate::propagate!(
			Self::try_get_sandbox_scheduler_record_with_transaction(txn, subspace, sandbox).await
		);
		let scheduler = record.and_then(|record| record.scheduler);

		Ok(ControlFlow::Break(scheduler))
	}

	pub(crate) async fn put_scheduler_sandbox_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		arg: &tangram_index::scheduler::sandbox::put::Arg,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		// Resolve the put against the sandbox's record by boot.
		let existing = crate::propagate!(
			Self::try_get_sandbox_scheduler_record_with_transaction(txn, subspace, &arg.sandbox)
				.await
		);
		let Some(change) = SandboxScheduler::put(existing.as_ref(), arg.boot, &arg.scheduler)
		else {
			return Ok(ControlFlow::Break(()));
		};

		// Write the change, and the request in the scheduler's queue.
		Self::apply_sandbox_scheduler_change(txn, subspace, &arg.sandbox, &change)?;
		let key = Key::Runner(crate::runner::Key::SchedulerSandbox {
			sandbox: arg.sandbox.clone(),
			scheduler: arg.scheduler.clone(),
		});
		txn.set(&Self::pack(subspace, &key), &arg.request);

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn delete_scheduler_sandbox_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		arg: &tangram_index::scheduler::sandbox::delete::Arg,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		// Resolve the delete against the sandbox's record by boot.
		let existing = crate::propagate!(
			Self::try_get_sandbox_scheduler_record_with_transaction(txn, subspace, &arg.sandbox)
				.await
		);
		let Some(change) = SandboxScheduler::delete(existing.as_ref(), arg.boot) else {
			return Ok(ControlFlow::Break(()));
		};

		// Write the change.
		Self::apply_sandbox_scheduler_change(txn, subspace, &arg.sandbox, &change)?;

		Ok(ControlFlow::Break(()))
	}

	/// Remove a deleted sandbox's keys that associate it with a scheduler.
	pub(crate) async fn delete_sandbox_scheduler_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let existing = crate::propagate!(
			Self::try_get_sandbox_scheduler_record_with_transaction(txn, subspace, sandbox).await
		);
		if let Some(scheduler) = existing.and_then(|existing| existing.scheduler) {
			let key = Key::Runner(crate::runner::Key::SchedulerSandbox {
				sandbox: sandbox.clone(),
				scheduler,
			});
			txn.clear(&Self::pack(subspace, &key));
		}
		let key = Key::Runner(crate::runner::Key::SandboxScheduler {
			sandbox: sandbox.clone(),
		});
		txn.clear(&Self::pack(subspace, &key));

		Ok(ControlFlow::Break(()))
	}

	async fn try_get_sandbox_scheduler_record_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<ControlFlow<Option<SandboxScheduler>, fdb::FdbError>> {
		let key = Key::Runner(crate::runner::Key::SandboxScheduler {
			sandbox: sandbox.clone(),
		});
		let result = txn.get(&Self::pack(subspace, &key), false).await;
		let bytes = crate::retry!(result);
		let record = bytes
			.as_deref()
			.map(SandboxScheduler::deserialize)
			.transpose()?;

		Ok(ControlFlow::Break(record))
	}

	fn apply_sandbox_scheduler_change(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		sandbox: &tg::sandbox::Id,
		change: &tangram_index::scheduler::sandbox::Change,
	) -> tg::Result<()> {
		if let Some(scheduler) = &change.removed {
			let key = Key::Runner(crate::runner::Key::SchedulerSandbox {
				sandbox: sandbox.clone(),
				scheduler: scheduler.clone(),
			});
			txn.clear(&Self::pack(subspace, &key));
		}
		let key = Key::Runner(crate::runner::Key::SandboxScheduler {
			sandbox: sandbox.clone(),
		});
		let value = change.stored.serialize()?;
		txn.set(&Self::pack(subspace, &key), &value);

		Ok(())
	}
}
