use {
	crate::lmdb::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_runners_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[crate::runner::put::Arg],
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::Runner(crate::lmdb::runner::Key::Runner(arg.id.clone()));
			let key = Self::pack(subspace, &key);
			let existing = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the runner"))?
				.map(crate::runner::Runner::deserialize)
				.transpose()?;

			if let Some(scheduler) = existing.and_then(|runner| runner.scheduler) {
				let key = Key::Runner(crate::lmdb::runner::Key::SchedulerRunner {
					scheduler: scheduler.clone(),
					runner: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete the scheduler runner"))?;

				let key = Key::Runner(crate::lmdb::runner::Key::RunnerScheduler {
					runner: arg.id.clone(),
					scheduler,
				});
				let key = Self::pack(subspace, &key);
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete the runner scheduler"))?;
			}

			let runner = crate::runner::Runner {
				scheduler: arg.scheduler.clone(),
			};
			let value = runner.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|error| tg::error!(!error, "failed to put the runner"))?;

			if let Some(scheduler) = &arg.scheduler {
				let key = Key::Runner(crate::lmdb::runner::Key::SchedulerRunner {
					scheduler: scheduler.clone(),
					runner: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put the scheduler runner"))?;

				let key = Key::Runner(crate::lmdb::runner::Key::RunnerScheduler {
					runner: arg.id.clone(),
					scheduler: scheduler.clone(),
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put the runner scheduler"))?;
			}
		}
		Ok(())
	}
}
