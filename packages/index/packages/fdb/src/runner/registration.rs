use {
	crate::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn check_runner_registration_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		registration: &tangram_index::runner::Registration,
	) -> tg::Result<ControlFlow<tg::Result<()>, fdb::FdbError>> {
		// Read the current registration in the mutation transaction.
		let key = Key::Runner(crate::runner::Key::Runner(registration.runner.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::retry!(result);
		let existing = bytes
			.as_deref()
			.map(tangram_serialize::from_slice::<tangram_index::runner::Runner>)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to decode the runner registration"))?
			.unwrap_or_default();

		// Require the accepted registration to match the cleanup guard.
		let accepted = existing.scheduler.as_ref() == Some(&registration.scheduler)
			&& existing.registration == registration.index;
		if !accepted {
			return Ok(ControlFlow::Break(Err(tg::error!(
				code = tg::error::Code::Cancellation,
				"the runner registration was superseded"
			))));
		}

		Ok(ControlFlow::Break(Ok(())))
	}
}
