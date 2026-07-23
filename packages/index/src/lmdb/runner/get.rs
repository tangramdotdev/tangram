use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
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

	pub(crate) fn try_get_runners_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::runner::Id],
	) -> tg::Result<Vec<Option<crate::runner::Runner>>> {
		ids.iter()
			.map(|id| {
				let key = Key::Runner(crate::lmdb::runner::Key::Runner(id.clone()));
				let key = Self::pack(subspace, &key);
				let bytes = db
					.get(transaction, &key)
					.map_err(|error| tg::error!(!error, %id, "failed to get the runner"))?;
				bytes.map(crate::runner::Runner::deserialize).transpose()
			})
			.collect()
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
			let Key::Runner(crate::lmdb::runner::Key::RunnerSandbox { sandbox, .. }) = key else {
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
			let Key::Runner(crate::lmdb::runner::Key::SchedulerRunner { runner, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			Ok(runner)
		})
		.collect()
	}
}
