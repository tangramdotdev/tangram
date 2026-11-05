use {
	crate::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_runner(
		&self,
		runner: &tg::runner::Id,
	) -> tg::Result<Option<tangram_index::runner::Runner>> {
		let request = tangram_index::read::Request::TryGetRunner {
			runner: runner.clone(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetRunner(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn try_get_runner_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		runner: &tg::runner::Id,
	) -> tg::Result<ControlFlow<Option<tangram_index::runner::Runner>, fdb::FdbError>> {
		let key = Key::Runner(crate::runner::Key::Runner(runner.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::retry!(result);
		let output = bytes
			.as_deref()
			.map(tangram_serialize::from_slice::<tangram_index::runner::Runner>)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to decode the runner"))?;

		Ok(ControlFlow::Break(output))
	}

	pub async fn get_scheduler_runners(
		&self,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<Vec<tg::runner::Id>> {
		let request = tangram_index::read::Request::GetSchedulerRunners {
			scheduler: scheduler.clone(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::GetSchedulerRunners(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn get_scheduler_sandboxes(
		&self,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<Vec<tangram_index::runner::SchedulerSandbox>> {
		let request = tangram_index::read::Request::GetSchedulerSandboxes {
			scheduler: scheduler.clone(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::GetSchedulerSandboxes(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn get_scheduler_runners_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<ControlFlow<Vec<tg::runner::Id>, fdb::FdbError>> {
		let scheduler = scheduler.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::SchedulerRunner.to_i32().unwrap(), scheduler.as_ref()),
		);
		let entry = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};
		let result = txn
			.get_ranges_keyvalues(entry, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		let runners = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Runner(crate::runner::Key::SchedulerRunner { runner, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(runner)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(runners))
	}

	pub(crate) async fn get_scheduler_sandboxes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<ControlFlow<Vec<tangram_index::runner::SchedulerSandbox>, fdb::FdbError>> {
		let scheduler = scheduler.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::SchedulerSandbox.to_i32().unwrap(), scheduler.as_ref()),
		);
		let entry = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};
		let result = txn
			.get_ranges_keyvalues(entry, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		let sandboxes = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Runner(crate::runner::Key::SchedulerSandbox { sandbox, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				let sandbox = tangram_index::runner::SchedulerSandbox {
					id: sandbox,
					request: entry.value().to_vec(),
				};
				Ok(sandbox)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(sandboxes))
	}

	pub(crate) async fn get_runner_sandboxes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		runner: &tg::runner::Id,
	) -> tg::Result<ControlFlow<Vec<tg::sandbox::Id>, fdb::FdbError>> {
		let runner = runner.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::RunnerSandbox.to_i32().unwrap(), runner.as_ref()),
		);
		let entry = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};
		let result = txn
			.get_ranges_keyvalues(entry, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		let sandboxes = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Runner(crate::runner::Key::RunnerSandbox { sandbox, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(sandbox)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(sandboxes))
	}
}
