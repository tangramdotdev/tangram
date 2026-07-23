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
		let request = crate::read::Request::TryGetRunners {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetRunners(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn try_get_runners_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::runner::Id],
	) -> tg::Result<Vec<Option<crate::runner::Runner>>> {
		futures::future::try_join_all(ids.iter().map(|id| async {
			let key = Key::Runner(crate::fdb::runner::Key::Runner(id.clone()));
			let key = Self::pack(subspace, &key);
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
		let request = crate::read::Request::GetRunnerSandboxes {
			runner: runner.clone(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::GetRunnerSandboxes(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn get_runner_sandboxes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		runner: &tg::runner::Id,
	) -> tg::Result<Vec<tg::sandbox::Id>> {
		let runner = runner.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::RunnerSandbox.to_i32().unwrap(), runner.as_ref()),
		);
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
				let key = Self::unpack(subspace, entry.key())?;
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
		let request = crate::read::Request::GetSchedulerRunners {
			scheduler: scheduler.clone(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::GetSchedulerRunners(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn get_scheduler_runners_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<Vec<tg::runner::Id>> {
		let scheduler = scheduler.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::SchedulerRunner.to_i32().unwrap(), scheduler.as_ref()),
		);
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
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Runner(crate::fdb::runner::Key::SchedulerRunner { runner, .. }) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(runner)
			})
			.collect()
	}
}
