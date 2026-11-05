use {
	crate::{Db, Index, Key, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
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

		// Persist the allocation before the runner publishes it.
		self.sync().await?;

		Ok(registration_index)
	}

	pub(crate) fn next_runner_registration_index_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		runner: &tg::runner::Id,
	) -> tg::Result<u64> {
		// Read the allocation and accepted ownership from the runner's registration record.
		let key = Key::Runner(crate::runner::Key::Runner(runner.clone()));
		let key = Self::pack(subspace, &key);
		let existing = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to read the runner registration"))?;
		let mut existing = existing
			.map(tangram_serialize::from_slice::<tangram_index::runner::Runner>)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to decode the runner registration"))?
			.unwrap_or_default();

		// Advance the allocation without changing the accepted ownership.
		let registration_index = existing.next;
		existing.next = registration_index
			.checked_add(1)
			.ok_or_else(|| tg::error!("the runner registration index overflowed"))?;
		let bytes = tangram_serialize::to_vec(&existing)
			.map_err(|error| tg::error!(!error, "failed to encode the runner registration"))?;
		db.put(transaction, &key, &bytes)
			.map_err(|error| tg::error!(!error, "failed to store the runner registration"))?;

		Ok(registration_index)
	}
}
