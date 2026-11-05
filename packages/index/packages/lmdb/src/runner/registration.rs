use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn check_runner_registration_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		registration: &tangram_index::runner::Registration,
	) -> tg::Result<tg::Result<()>> {
		// Read the current registration in the mutation transaction.
		let key = Key::Runner(crate::runner::Key::Runner(registration.runner.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the runner registration"))?;
		let existing = bytes
			.map(tangram_serialize::from_slice::<tangram_index::runner::Runner>)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to decode the runner registration"))?
			.unwrap_or_default();

		// Require the accepted registration to match the cleanup guard.
		let accepted = existing.scheduler.as_ref() == Some(&registration.scheduler)
			&& existing.registration == registration.index;
		if !accepted {
			return Ok(Err(tg::error!(
				code = tg::error::Code::Cancellation,
				"the runner registration was superseded"
			)));
		}

		Ok(Ok(()))
	}
}
