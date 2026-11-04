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
		// Read the last allocation and the accepted registration in the same transaction.
		let key = Key::Runner(crate::runner::Key::RegistrationIndex(runner.clone()));
		let key = Self::pack(subspace, &key);
		let registration_key = Key::Runner(crate::runner::Key::Registration(runner.clone()));
		let registration_key = Self::pack(subspace, &registration_key);
		let previous = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to read the runner registration index"))?;
		let previous = previous
			.map(tangram_serialize::from_slice::<u64>)
			.transpose()
			.map_err(|error| {
				tg::error!(!error, "failed to decode the runner registration index")
			})?;
		let registration = db
			.get(transaction, &registration_key)
			.map_err(|error| tg::error!(!error, "failed to read the runner registration index"))?;
		let registration = registration
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
		db.put(transaction, &key, &bytes)
			.map_err(|error| tg::error!(!error, "failed to store the runner registration index"))?;

		Ok(registration_index)
	}
}
