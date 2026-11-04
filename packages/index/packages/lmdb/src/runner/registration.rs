use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn runner_registration_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		registration: &tangram_index::runner::Registration,
		put: bool,
	) -> tg::Result<tg::Result<()>> {
		// Read the current registration in the mutation transaction.
		let key = Key::Runner(crate::runner::Key::Registration(
			registration.runner.clone(),
		));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the runner registration"))?;
		let existing = bytes
			.map(tangram_serialize::from_slice::<tangram_index::runner::Registration>)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to decode the runner registration"))?;

		// Accept an identical retry or a strictly newer ownership claim.
		let accepted = existing.as_ref() == Some(registration)
			|| (put
				&& existing.as_ref().is_none_or(|existing| {
					registration.registration_index > existing.registration_index
				}));
		if !accepted {
			return Ok(Err(tg::error!(
				code = tg::error::Code::Cancellation,
				"the runner registration was superseded"
			)));
		}

		// Store the accepted registration.
		if put {
			let bytes = tangram_serialize::to_vec(registration)
				.map_err(|error| tg::error!(!error, "failed to encode the runner registration"))?;
			db.put(transaction, &key, &bytes)
				.map_err(|error| tg::error!(!error, "failed to put the runner registration"))?;
		}

		Ok(Ok(()))
	}
}
