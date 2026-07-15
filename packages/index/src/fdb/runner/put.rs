use {
	crate::fdb::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn task_put_runners(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::runner::put::Arg],
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::Runner(crate::fdb::runner::Key::Runner(arg.id.clone()));
			let key = Self::pack(subspace, &key);
			let existing = txn
				.get(&key, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the runner"))?
				.map(|bytes| crate::runner::Runner::deserialize(&bytes))
				.transpose()?;

			if let Some(scheduler) = existing.and_then(|runner| runner.scheduler) {
				let key = Key::Runner(crate::fdb::runner::Key::SchedulerRunner {
					scheduler: scheduler.clone(),
					runner: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.clear(&key);

				let key = Key::Runner(crate::fdb::runner::Key::RunnerScheduler {
					runner: arg.id.clone(),
					scheduler,
				});
				let key = Self::pack(subspace, &key);
				txn.clear(&key);
			}

			let runner = crate::runner::Runner {
				scheduler: arg.scheduler.clone(),
			};
			let value = runner.serialize()?;
			txn.set(&key, &value);

			if let Some(scheduler) = &arg.scheduler {
				let key = Key::Runner(crate::fdb::runner::Key::SchedulerRunner {
					scheduler: scheduler.clone(),
					runner: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.set(&key, &[]);

				let key = Key::Runner(crate::fdb::runner::Key::RunnerScheduler {
					runner: arg.id.clone(),
					scheduler: scheduler.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.set(&key, &[]);
			}
		}
		Ok(())
	}
}
