use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_runner(
		&self,
		runner: &tg::runner::Id,
	) -> tg::Result<Option<tangram_index::runner::Runner>> {
		let request = tangram_index::read::Request::TryGetRunner {
			runner: runner.clone(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetRunner(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn try_get_runner_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		runner: &tg::runner::Id,
	) -> tg::Result<Option<tangram_index::runner::Runner>> {
		let key = Key::Runner(crate::runner::Key::Runner(runner.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the runner"))?;
		let output = bytes
			.map(tangram_serialize::from_slice::<tangram_index::runner::Runner>)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to decode the runner"))?;

		Ok(output)
	}

	pub(crate) fn get_runner_sandboxes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		runner: &tg::runner::Id,
	) -> tg::Result<Vec<tg::sandbox::Id>> {
		let runner = runner.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::RunnerSandbox.to_i32().unwrap(), runner.as_ref()),
		);
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the runner sandboxes"))?;
		iter.map(|entry| {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read a runner sandbox"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Runner(crate::runner::Key::RunnerSandbox { sandbox, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			Ok(sandbox)
		})
		.collect()
	}
}
