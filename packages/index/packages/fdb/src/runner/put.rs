use {
	crate::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_runner_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::runner::put::Arg,
	) -> tg::Result<ControlFlow<tg::Result<()>, fdb::FdbError>> {
		// Read the current registration in the mutation transaction.
		let key = Key::Runner(crate::runner::Key::Runner(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::retry!(result);
		let mut existing = bytes
			.as_deref()
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
			return Ok(ControlFlow::Break(Err(tg::error!(
				code = tg::error::Code::Cancellation,
				"the runner registration was superseded"
			))));
		}

		// Store the accepted registration.
		existing.next = existing.next.max(arg.registration.saturating_add(1));
		existing.registration = arg.registration;
		existing.scheduler = Some(arg.scheduler.clone());
		let bytes = tangram_serialize::to_vec(&existing)
			.map_err(|error| tg::error!(!error, "failed to encode the runner registration"))?;
		txn.set(&key, &bytes);

		Ok(ControlFlow::Break(Ok(())))
	}
}
