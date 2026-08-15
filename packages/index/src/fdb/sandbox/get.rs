use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn get_sandbox_processes(
		&self,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Vec<(tg::process::Id, crate::process::Process)>> {
		let request = crate::read::Request::GetSandboxProcesses {
			sandbox: sandbox.clone(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::GetSandboxProcesses(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn get_sandbox_processes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<ControlFlow<Vec<(tg::process::Id, crate::process::Process)>, fdb::FdbError>> {
		let sandbox = sandbox.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::SandboxProcess.to_i32().unwrap(), sandbox.as_ref()),
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
		let processes = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Sandbox(crate::fdb::sandbox::Key::SandboxProcess { process, .. }) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(process)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		drop(entries);
		let mut output = Vec::with_capacity(processes.len());
		for process in processes {
			let data = crate::fdb::propagate!(
				Self::try_get_process_with_transaction(txn, subspace, &process).await
			)
			.ok_or_else(|| tg::error!(%process, "failed to find the sandbox process"))?;
			output.push((process, data));
		}
		Ok(ControlFlow::Break(output))
	}

	pub async fn try_get_sandboxes(
		&self,
		ids: &[tg::sandbox::Id],
	) -> tg::Result<Vec<Option<crate::sandbox::Sandbox>>> {
		let request = crate::read::Request::TryGetSandboxes {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetSandboxes(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn try_get_sandboxes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::sandbox::Id],
	) -> tg::Result<ControlFlow<Vec<Option<crate::sandbox::Sandbox>>, fdb::FdbError>> {
		let sandboxes = {
			let result = futures::future::try_join_all(
				ids.iter()
					.map(|id| Self::try_get_sandbox_with_transaction(txn, subspace, id)),
			)
			.await;
			let results = result?;
			let mut values = Vec::new();
			for result in results {
				let value = match result {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				values.push(value);
			}
			values
		};

		Ok(ControlFlow::Break(sandboxes))
	}

	pub(crate) async fn try_get_sandbox_with_transaction(
		txn: &fdb::Transaction,
		subspace: &foundationdb_tuple::Subspace,
		id: &tg::sandbox::Id,
	) -> tg::Result<ControlFlow<Option<crate::sandbox::Sandbox>, fdb::FdbError>> {
		let key = Key::Sandbox(crate::fdb::sandbox::Key::Sandbox(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = crate::fdb::retry!(txn.get(&key, false).await);
		let sandbox = bytes
			.map(|bytes| crate::sandbox::Sandbox::deserialize(&bytes))
			.transpose()?;

		Ok(ControlFlow::Break(sandbox))
	}
}
