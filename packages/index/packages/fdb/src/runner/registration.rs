use {
	crate::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn runner_registration_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		registration: &tangram_index::runner::Registration,
		put: bool,
	) -> tg::Result<ControlFlow<tg::Result<()>, fdb::FdbError>> {
		// Read the current registration in the mutation transaction.
		let key = Key::Runner(crate::runner::Key::Registration(
			registration.runner.clone(),
		));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::retry!(result);
		let existing = bytes
			.as_deref()
			.map(tangram_serialize::from_slice::<tangram_index::runner::Registration>)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to decode the runner registration"))?;

		// Accept an identical retry or a strictly newer ownership claim.
		let accepted = existing.as_ref() == Some(registration)
			|| (put
				&& existing
					.as_ref()
					.is_none_or(|existing| registration.attempt > existing.attempt));
		if !accepted {
			return Ok(ControlFlow::Break(Err(tg::error!(
				code = tg::error::Code::Cancellation,
				"the runner registration was superseded"
			))));
		}

		// Store the accepted registration.
		if put {
			let bytes = tangram_serialize::to_vec(registration)
				.map_err(|error| tg::error!(!error, "failed to encode the runner registration"))?;
			txn.set(&key, &bytes);
		}

		Ok(ControlFlow::Break(Ok(())))
	}
}
