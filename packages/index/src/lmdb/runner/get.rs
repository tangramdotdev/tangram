use {
	crate::lmdb::{Index, Key, Kind},
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_runners(
		&self,
		ids: &[tg::runner::Id],
	) -> tg::Result<Vec<Option<crate::runner::Runner>>> {
		let ids = ids.to_owned();
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let subspace = self.subspace.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				ids.iter()
					.map(|id| {
						let key = Key::Runner(crate::lmdb::runner::Key::Runner(id.clone()));
						let key = Self::pack(&subspace, &key);
						let bytes = db
							.get(&transaction, &key)
							.map_err(|error| tg::error!(!error, %id, "failed to get the runner"))?;
						bytes.map(crate::runner::Runner::deserialize).transpose()
					})
					.collect()
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	pub async fn get_runner_sandboxes(
		&self,
		runner: &tg::runner::Id,
	) -> tg::Result<Vec<tg::sandbox::Id>> {
		let runner = runner.clone();
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let subspace = self.subspace.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let runner = runner.to_bytes();
				let prefix = Self::pack(
					&subspace,
					&(Kind::RunnerSandbox.to_i32().unwrap(), runner.as_ref()),
				);
				let iter = db
					.prefix_iter(&transaction, &prefix)
					.map_err(|error| tg::error!(!error, "failed to get the runner sandboxes"))?;
				iter.map(|entry| {
					let (key, _) = entry
						.map_err(|error| tg::error!(!error, "failed to read a runner sandbox"))?;
					let key = Self::unpack(&subspace, key)?;
					let Key::Runner(crate::lmdb::runner::Key::RunnerSandbox { sandbox, .. }) = key
					else {
						return Err(tg::error!("unexpected key type"));
					};
					Ok(sandbox)
				})
				.collect()
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	pub async fn get_scheduler_runners(
		&self,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<Vec<tg::runner::Id>> {
		let scheduler = scheduler.clone();
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let subspace = self.subspace.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let scheduler = scheduler.to_bytes();
				let prefix = Self::pack(
					&subspace,
					&(Kind::SchedulerRunner.to_i32().unwrap(), scheduler.as_ref()),
				);
				let iter = db
					.prefix_iter(&transaction, &prefix)
					.map_err(|error| tg::error!(!error, "failed to get the scheduler runners"))?;
				iter.map(|entry| {
					let (key, _) = entry
						.map_err(|error| tg::error!(!error, "failed to read a scheduler runner"))?;
					let key = Self::unpack(&subspace, key)?;
					let Key::Runner(crate::lmdb::runner::Key::SchedulerRunner { runner, .. }) = key
					else {
						return Err(tg::error!("unexpected key type"));
					};
					Ok(runner)
				})
				.collect()
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}
}
