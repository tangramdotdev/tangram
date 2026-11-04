use {
	crate::{Index, Key, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn next_runner_registration_index(&self, runner: &tg::runner::Id) -> tg::Result<u64> {
		let response = self
			.send_write_request(Request::NextRunnerRegistrationIndex(runner.clone()))
			.await?;
		let Response::RunnerRegistrationIndex(registration_index) = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(registration_index)
	}

	pub(crate) async fn next_runner_registration_index_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		runner: &tg::runner::Id,
	) -> tg::Result<ControlFlow<u64, fdb::FdbError>> {
		// Read the last allocation and the accepted registration in the same transaction.
		let key = Key::Runner(crate::runner::Key::RegistrationIndex(runner.clone()));
		let key = Self::pack(subspace, &key);
		let registration_key = Key::Runner(crate::runner::Key::Registration(runner.clone()));
		let registration_key = Self::pack(subspace, &registration_key);
		let result = txn.get(&key, false).await;
		let previous = crate::retry!(result);
		let previous = previous
			.as_deref()
			.map(tangram_serialize::from_slice::<u64>)
			.transpose()
			.map_err(|error| {
				tg::error!(!error, "failed to decode the runner registration index")
			})?;
		let result = txn.get(&registration_key, false).await;
		let registration = crate::retry!(result);
		let registration = registration
			.as_deref()
			.map(tangram_serialize::from_slice::<tangram_index::runner::Registration>)
			.transpose()
			.map_err(|error| {
				tg::error!(!error, "failed to decode the runner registration index")
			})?;

		// Advance past any accepted registration when the runner and scheduler share an index.
		let previous = previous
			.unwrap_or_default()
			.max(registration.map_or(0, |registration| registration.registration_index));
		let registration_index = previous
			.checked_add(1)
			.ok_or_else(|| tg::error!("the runner registration index overflowed"))?;
		let bytes = tangram_serialize::to_vec(&registration_index).map_err(|error| {
			tg::error!(!error, "failed to encode the runner registration index")
		})?;
		txn.set(&key, &bytes);

		Ok(ControlFlow::Break(registration_index))
	}
}
