use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_runners(
		&self,
		ids: &[tg::runner::Id],
	) -> tg::Result<Vec<Option<crate::runner::Runner>>> {
		let txn = self
			.database
			.create_trx()
			.map_err(|error| tg::error!(!error, "failed to create the transaction"))?;
		futures::future::try_join_all(ids.iter().map(|id| async {
			let key = Key::Runner(crate::fdb::runner::Key::Runner(id.clone()));
			let key = Self::pack(&self.subspace, &key);
			let bytes = txn
				.get(&key, false)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to get the runner"))?;
			bytes
				.map(|bytes| crate::runner::Runner::deserialize(&bytes))
				.transpose()
		}))
		.await
	}

	pub async fn get_runner_sandboxes(
		&self,
		runner: &tg::runner::Id,
	) -> tg::Result<Vec<tg::sandbox::Id>> {
		let runner = runner.to_bytes();
		let prefix = Self::pack(
			&self.subspace,
			&(Kind::RunnerSandbox.to_i32().unwrap(), runner.as_ref()),
		);
		let txn = self
			.database
			.create_trx()
			.map_err(|error| tg::error!(!error, "failed to create the transaction"))?;
		let entries = txn
			.get_range(
				&fdb::RangeOption {
					mode: fdb::options::StreamingMode::WantAll,
					..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
				},
				1,
				false,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the runner sandboxes"))?;
		entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(&self.subspace, entry.key())?;
				let Key::Runner(crate::fdb::runner::Key::RunnerSandbox { sandbox, .. }) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(sandbox)
			})
			.collect()
	}

	pub async fn get_scheduler_runners(
		&self,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<Vec<tg::runner::Id>> {
		let scheduler = scheduler.to_bytes();
		let prefix = Self::pack(
			&self.subspace,
			&(Kind::SchedulerRunner.to_i32().unwrap(), scheduler.as_ref()),
		);
		let txn = self
			.database
			.create_trx()
			.map_err(|error| tg::error!(!error, "failed to create the transaction"))?;
		let entries = txn
			.get_range(
				&fdb::RangeOption {
					mode: fdb::options::StreamingMode::WantAll,
					..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
				},
				1,
				false,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the scheduler runners"))?;
		entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(&self.subspace, entry.key())?;
				let Key::Runner(crate::fdb::runner::Key::SchedulerRunner { runner, .. }) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(runner)
			})
			.collect()
	}
}
