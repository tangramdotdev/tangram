use {
	crate::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
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

	pub(crate) async fn try_get_runner_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		runner: &tg::runner::Id,
	) -> tg::Result<ControlFlow<Option<tangram_index::runner::Runner>, fdb::FdbError>> {
		let key = Key::Runner(crate::runner::Key::Runner(runner.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::retry!(result);
		let output = bytes
			.as_deref()
			.map(tangram_serialize::from_slice::<tangram_index::runner::Runner>)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to decode the runner"))?;

		Ok(ControlFlow::Break(output))
	}

	pub(crate) async fn get_runner_sandboxes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		runner: &tg::runner::Id,
	) -> tg::Result<ControlFlow<Vec<tg::sandbox::Id>, fdb::FdbError>> {
		let runner = runner.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::RunnerSandbox.to_i32().unwrap(), runner.as_ref()),
		);
		let entry = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};
		let result = txn
			.get_ranges_keyvalues(entry, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		let sandboxes = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Runner(crate::runner::Key::RunnerSandbox { sandbox, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(sandbox)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(sandboxes))
	}
}
