use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_runner_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::runner::put::Arg,
	) -> tg::Result<tg::Result<()>> {
		// Read the runner record in the mutation transaction.
		let key = Key::Runner(crate::runner::Key::Runner(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the runner record"))?;
		let mut existing = bytes
			.map(tangram_serialize::from_slice::<tangram_index::runner::Runner>)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to decode the runner record"))?
			.unwrap_or_default();

		// Reject a registration with another scheduler, because a runner belongs to the scheduler that it first registers with for its whole life.
		if existing
			.scheduler
			.as_ref()
			.is_some_and(|scheduler| *scheduler != arg.scheduler)
		{
			return Ok(Err(tg::error!("the runner belongs to another scheduler")));
		}

		// Record the runner under its scheduler.
		let scheduler_key = Key::Runner(crate::runner::Key::SchedulerRunner {
			runner: arg.id.clone(),
			scheduler: arg.scheduler.clone(),
		});
		let scheduler_key = Self::pack(subspace, &scheduler_key);
		db.put(transaction, &scheduler_key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the scheduler runner"))?;

		// Store the runner's scheduler.
		existing.scheduler = Some(arg.scheduler.clone());
		let bytes = tangram_serialize::to_vec(&existing)
			.map_err(|error| tg::error!(!error, "failed to encode the runner record"))?;
		db.put(transaction, &key, &bytes)
			.map_err(|error| tg::error!(!error, "failed to put the runner record"))?;

		Ok(Ok(()))
	}
}
