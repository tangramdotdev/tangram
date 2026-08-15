use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn get_runner_sandboxes(
		&self,
		runner: &tg::runner::Id,
	) -> tg::Result<Vec<tg::sandbox::Id>> {
		let request = crate::read::Request::GetRunnerSandboxes {
			runner: runner.clone(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::GetRunnerSandboxes(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
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
		let entries = crate::fdb::retry!(
			txn.get_range(
				&fdb::RangeOption {
					mode: fdb::options::StreamingMode::WantAll,
					..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
				},
				1,
				false,
			)
			.await
		);
		let sandboxes = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Runner(crate::fdb::runner::Key::RunnerSandbox { sandbox, .. }) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(sandbox)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(sandboxes))
	}
}
