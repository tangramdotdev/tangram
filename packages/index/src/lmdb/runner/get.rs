use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn get_runner_sandboxes(
		&self,
		runner: &tg::runner::Id,
	) -> tg::Result<Vec<crate::runner::Sandbox>> {
		let request = crate::read::Request::GetRunnerSandboxes {
			runner: runner.clone(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::GetRunnerSandboxes(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn get_runner_sandboxes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		runner: &tg::runner::Id,
	) -> tg::Result<Vec<crate::runner::Sandbox>> {
		let runner = runner.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::RunnerSandbox.to_i32().unwrap(), runner.as_ref()),
		);
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the runner sandboxes"))?;
		iter.map(|entry| {
			let (key, value) =
				entry.map_err(|error| tg::error!(!error, "failed to read a runner sandbox"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Runner(crate::lmdb::runner::Key::RunnerSandbox { sandbox, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			let attempt = if value.is_empty() {
				None
			} else {
				let attempt = std::str::from_utf8(value)
					.map_err(|error| tg::error!(!error, "invalid runner sandbox attempt"))?;
				Some(attempt.to_owned())
			};
			let sandbox = crate::runner::Sandbox {
				attempt,
				id: sandbox,
			};
			Ok(sandbox)
		})
		.collect()
	}
}
