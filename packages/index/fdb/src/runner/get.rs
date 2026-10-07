use {
	crate::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn get_runner_sandboxes_with_transaction(
		txn: &crate::Transaction,
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
		let result = txn.get_range(&entry, 1, false).await;
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
