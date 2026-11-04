use {
	crate::{Db, Index, Key, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn next_runner_attempt(&self, runner: &tg::runner::Id) -> tg::Result<u64> {
		let response = self
			.send_write_request(Request::NextRunnerAttempt(runner.clone()))
			.await?;
		let Response::RunnerAttempt(attempt) = response else {
			return Err(tg::error!("unexpected write response"));
		};

		// Persist the allocation before the runner publishes it.
		self.sync().await?;

		Ok(attempt)
	}

	pub(crate) fn next_runner_attempt_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		runner: &tg::runner::Id,
	) -> tg::Result<u64> {
		// Read the last allocation and the accepted registration in the same transaction.
		let key = Key::Runner(crate::runner::Key::Attempt(runner.clone()));
		let key = Self::pack(subspace, &key);
		let registration_key = Key::Runner(crate::runner::Key::Registration(runner.clone()));
		let registration_key = Self::pack(subspace, &registration_key);
		let previous = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to read the runner attempt"))?;
		let previous = previous
			.map(tangram_serialize::from_slice::<u64>)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to decode the runner attempt"))?;
		let registration = db
			.get(transaction, &registration_key)
			.map_err(|error| tg::error!(!error, "failed to read the runner attempt"))?;
		let registration = registration
			.map(tangram_serialize::from_slice::<tangram_index::runner::Registration>)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to decode the runner attempt"))?;

		// Advance past any accepted registration when the runner and scheduler share an index.
		let previous = previous
			.unwrap_or_default()
			.max(registration.map_or(0, |registration| registration.attempt));
		let attempt = previous
			.checked_add(1)
			.ok_or_else(|| tg::error!("the runner attempt overflowed"))?;
		let bytes = tangram_serialize::to_vec(&attempt)
			.map_err(|error| tg::error!(!error, "failed to encode the runner attempt"))?;
		db.put(transaction, &key, &bytes)
			.map_err(|error| tg::error!(!error, "failed to store the runner attempt"))?;

		Ok(attempt)
	}
}
