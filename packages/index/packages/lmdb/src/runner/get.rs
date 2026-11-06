use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
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

	pub(crate) fn try_get_runner_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		runner: &tg::runner::Id,
	) -> tg::Result<Option<tangram_index::runner::Runner>> {
		let key = Key::Runner(crate::runner::Key::Runner(runner.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the runner"))?;
		let output = bytes
			.map(tangram_serialize::from_slice::<tangram_index::runner::Runner>)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to decode the runner"))?;

		Ok(output)
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

	pub(crate) fn get_scheduler_runners_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<Vec<tg::runner::Id>> {
		let scheduler = scheduler.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::SchedulerRunner.to_i32().unwrap(), scheduler.as_ref()),
		);
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the scheduler runners"))?;
		iter.map(|entry| {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read a scheduler runner"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Runner(crate::runner::Key::SchedulerRunner { runner, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			Ok(runner)
		})
		.collect()
	}

	pub(crate) fn get_scheduler_sandboxes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<Vec<tangram_index::runner::SchedulerSandbox>> {
		let scheduler = scheduler.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::SchedulerSandbox.to_i32().unwrap(), scheduler.as_ref()),
		);
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the scheduler sandboxes"))?;
		iter.map(|entry| {
			let (key, value) =
				entry.map_err(|error| tg::error!(!error, "failed to read a scheduler sandbox"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Runner(crate::runner::Key::SchedulerSandbox { sandbox, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			let sandbox = tangram_index::runner::SchedulerSandbox {
				id: sandbox,
				request: value.to_vec(),
			};
			Ok(sandbox)
		})
		.collect()
	}

	pub(crate) fn get_runner_sandboxes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		runner: &tg::runner::Id,
	) -> tg::Result<Vec<tg::sandbox::Id>> {
		let runner = runner.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::RunnerSandbox.to_i32().unwrap(), runner.as_ref()),
		);
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the runner sandboxes"))?;
		iter.map(|entry| {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read a runner sandbox"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Runner(crate::runner::Key::RunnerSandbox { sandbox, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			Ok(sandbox)
		})
		.collect()
	}
}
