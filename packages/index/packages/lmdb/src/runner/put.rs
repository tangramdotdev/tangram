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
		// Read the current registration in the mutation transaction.
		let key = Key::Runner(crate::runner::Key::Runner(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the runner registration"))?;
		let mut existing = bytes
			.map(tangram_serialize::from_slice::<tangram_index::runner::Runner>)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to decode the runner registration"))?
			.unwrap_or_default();

		// Accept an identical retry or a strictly newer ownership claim.
		let accepted = existing.scheduler.as_ref() == Some(&arg.scheduler)
			&& existing.registration == arg.registration
			|| existing.scheduler.is_none()
			|| arg.registration > existing.registration;
		if !accepted {
			return Ok(Err(tg::error!(
				code = tg::error::Code::Cancellation,
				"the runner registration was superseded"
			)));
		}

		// Move the runner to its scheduler.
		if let Some(scheduler) = &existing.scheduler {
			let key = Key::Runner(crate::runner::Key::SchedulerRunner {
				runner: arg.id.clone(),
				scheduler: scheduler.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the scheduler runner"))?;
		}
		let scheduler_key = Key::Runner(crate::runner::Key::SchedulerRunner {
			runner: arg.id.clone(),
			scheduler: arg.scheduler.clone(),
		});
		let scheduler_key = Self::pack(subspace, &scheduler_key);
		db.put(transaction, &scheduler_key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the scheduler runner"))?;

		// Store the accepted registration.
		existing.next = existing.next.max(arg.registration.saturating_add(1));
		existing.registration = arg.registration;
		existing.scheduler = Some(arg.scheduler.clone());
		let bytes = tangram_serialize::to_vec(&existing)
			.map_err(|error| tg::error!(!error, "failed to encode the runner registration"))?;
		db.put(transaction, &key, &bytes)
			.map_err(|error| tg::error!(!error, "failed to put the runner registration"))?;

		Ok(Ok(()))
	}
}
