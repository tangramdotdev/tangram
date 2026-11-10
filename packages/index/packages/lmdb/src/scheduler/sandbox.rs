use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
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

	pub(crate) fn try_get_sandbox_scheduler_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::scheduler::Id>> {
		let record = Self::try_get_sandbox_scheduler_record_with_transaction(
			db,
			subspace,
			transaction,
			sandbox,
		)?;
		let scheduler = record.and_then(|record| record.scheduler);

		Ok(scheduler)
	}

	pub(crate) fn put_scheduler_sandbox_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::scheduler::sandbox::put::Arg,
	) -> tg::Result<()> {
		// Resolve the put against the sandbox's record by boot.
		let existing = Self::try_get_sandbox_scheduler_record_with_transaction(
			db,
			subspace,
			transaction,
			&arg.sandbox,
		)?;
		let Some(change) = SandboxScheduler::put(existing.as_ref(), arg.boot, &arg.scheduler)
		else {
			return Ok(());
		};

		// Write the change, and the request in the scheduler's queue.
		Self::apply_sandbox_scheduler_change(db, subspace, transaction, &arg.sandbox, &change)?;
		let key = Key::Runner(crate::runner::Key::SchedulerSandbox {
			sandbox: arg.sandbox.clone(),
			scheduler: arg.scheduler.clone(),
		});
		db.put(transaction, &Self::pack(subspace, &key), &arg.request)
			.map_err(|error| tg::error!(!error, "failed to put the scheduler sandbox"))?;

		Ok(())
	}

	pub(crate) fn delete_scheduler_sandbox_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::scheduler::sandbox::delete::Arg,
	) -> tg::Result<()> {
		// Resolve the delete against the sandbox's record by boot.
		let existing = Self::try_get_sandbox_scheduler_record_with_transaction(
			db,
			subspace,
			transaction,
			&arg.sandbox,
		)?;
		let Some(change) = SandboxScheduler::delete(existing.as_ref(), arg.boot) else {
			return Ok(());
		};

		// Write the change.
		Self::apply_sandbox_scheduler_change(db, subspace, transaction, &arg.sandbox, &change)?;

		Ok(())
	}

	/// Remove a deleted sandbox's keys that associate it with a scheduler.
	pub(crate) fn delete_sandbox_scheduler_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<()> {
		let existing = Self::try_get_sandbox_scheduler_record_with_transaction(
			db,
			subspace,
			transaction,
			sandbox,
		)?;
		if let Some(scheduler) = existing.and_then(|existing| existing.scheduler) {
			let key = Key::Runner(crate::runner::Key::SchedulerSandbox {
				sandbox: sandbox.clone(),
				scheduler,
			});
			db.delete(transaction, &Self::pack(subspace, &key))
				.map_err(|error| tg::error!(!error, "failed to delete the scheduler sandbox"))?;
		}
		let key = Key::Runner(crate::runner::Key::SandboxScheduler {
			sandbox: sandbox.clone(),
		});
		db.delete(transaction, &Self::pack(subspace, &key))
			.map_err(|error| tg::error!(!error, "failed to delete the sandbox scheduler"))?;

		Ok(())
	}

	fn try_get_sandbox_scheduler_record_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Option<SandboxScheduler>> {
		let key = Key::Runner(crate::runner::Key::SandboxScheduler {
			sandbox: sandbox.clone(),
		});
		let bytes = db
			.get(transaction, &Self::pack(subspace, &key))
			.map_err(|error| tg::error!(!error, "failed to get the sandbox scheduler"))?;
		let record = bytes.map(SandboxScheduler::deserialize).transpose()?;

		Ok(record)
	}

	fn apply_sandbox_scheduler_change(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		sandbox: &tg::sandbox::Id,
		change: &tangram_index::scheduler::sandbox::Change,
	) -> tg::Result<()> {
		if let Some(scheduler) = &change.removed {
			let key = Key::Runner(crate::runner::Key::SchedulerSandbox {
				sandbox: sandbox.clone(),
				scheduler: scheduler.clone(),
			});
			db.delete(transaction, &Self::pack(subspace, &key))
				.map_err(|error| tg::error!(!error, "failed to delete the scheduler sandbox"))?;
		}
		let key = Key::Runner(crate::runner::Key::SandboxScheduler {
			sandbox: sandbox.clone(),
		});
		let value = change.stored.serialize()?;
		db.put(transaction, &Self::pack(subspace, &key), &value)
			.map_err(|error| tg::error!(!error, "failed to put the sandbox scheduler"))?;

		Ok(())
	}
}
