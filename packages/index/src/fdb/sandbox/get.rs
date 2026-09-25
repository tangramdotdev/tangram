use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_sandbox_processes(
		&self,
		id: &tg::sandbox::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> tg::Result<Option<Vec<tg::process::Id>>> {
		let request = crate::read::Request::TryGetSandboxProcesses {
			id: id.clone(),
			length,
			position,
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetSandboxProcesses(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn try_get_sandbox_processes_count(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<u64>> {
		let request = crate::read::Request::TryGetSandboxProcessesCount { id: id.clone() };
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetSandboxProcessesCount(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};
		Ok(output)
	}

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
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<ControlFlow<Vec<(tg::process::Id, crate::process::Process)>, fdb::FdbError>> {
		let bytes = sandbox.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::SandboxProcess.to_i32().unwrap(), bytes.as_ref()),
		);
		let entry = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};
		let result = txn
			.get_ranges_keyvalues(entry, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::fdb::retry!(result);
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
		let output = {
			let result =
				futures::future::try_join_all(processes.into_iter().map(|process| async move {
					let data = crate::fdb::propagate!(
						Self::try_get_process_with_transaction(txn, subspace, &process).await
					);
					let data = data.filter(|data| data.sandbox.as_ref() == Some(sandbox));

					Ok::<_, tg::Error>(ControlFlow::Break(data.map(|data| (process, data))))
				}))
				.await;
			let results = result?;
			let mut values = Vec::with_capacity(results.len());
			for result in results {
				let value = match result {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				values.extend(value);
			}
			values
		};

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
		txn: &crate::fdb::Transaction,
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
			let mut values = Vec::with_capacity(results.len());
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
		txn: &crate::fdb::Transaction,
		subspace: &foundationdb_tuple::Subspace,
		id: &tg::sandbox::Id,
	) -> tg::Result<ControlFlow<Option<crate::sandbox::Sandbox>, fdb::FdbError>> {
		let key = Key::Sandbox(crate::fdb::sandbox::Key::Sandbox(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::fdb::retry!(result);
		let sandbox = bytes
			.map(|bytes| crate::sandbox::Sandbox::deserialize(&bytes))
			.transpose()?;

		Ok(ControlFlow::Break(sandbox))
	}

	pub(crate) async fn try_get_sandbox_processes_count_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		id: &tg::sandbox::Id,
	) -> tg::Result<ControlFlow<Option<u64>, fdb::FdbError>> {
		let sandbox = Self::try_get_sandbox_with_transaction(txn, subspace, id).await?;
		let sandbox = match sandbox {
			ControlFlow::Break(sandbox) => sandbox,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		if sandbox.is_none() {
			return Ok(ControlFlow::Break(None));
		}
		let bytes = id.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::SandboxProcess.to_i32().unwrap(), bytes.as_ref()),
		);
		let (begin, end) = Subspace::from_bytes(prefix).range();
		let selector = fdb::KeySelector::last_less_than(end);
		let result = txn.get_key(&selector, false).await;
		let key = crate::fdb::retry!(result);
		if key.as_ref() < begin.as_slice() {
			return Ok(ControlFlow::Break(Some(0)));
		}
		let Key::Sandbox(crate::fdb::sandbox::Key::SandboxProcess { position, .. }) =
			Self::unpack(subspace, &key)?
		else {
			return Err(tg::error!("unexpected key type"));
		};
		let count = u64::try_from(position)
			.map_err(|error| tg::error!(!error, "invalid sandbox process position"))?
			.checked_add(1)
			.ok_or_else(|| tg::error!("invalid sandbox process count"))?;
		Ok(ControlFlow::Break(Some(count)))
	}

	pub(crate) async fn try_get_sandbox_processes_page_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		id: &tg::sandbox::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> tg::Result<ControlFlow<Option<Vec<tg::process::Id>>, fdb::FdbError>> {
		let Some(_) =
			crate::fdb::propagate!(Self::try_get_sandbox_with_transaction(txn, subspace, id).await)
		else {
			return Ok(ControlFlow::Break(None));
		};
		if length == 0 {
			return Ok(ControlFlow::Break(Some(Vec::new())));
		}
		let limit = length
			.to_usize()
			.ok_or_else(|| tg::error!("the sandbox process length is too large"))?;
		let bytes = id.to_bytes();
		let key = (Kind::SandboxProcess.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let (begin, end) = range_subspace.range();
		let position = match position {
			std::io::SeekFrom::Start(position) => position
				.to_i64()
				.ok_or_else(|| tg::error!("the sandbox process position is too large"))?,
			std::io::SeekFrom::End(position) => {
				if position >= 0 {
					return Ok(ControlFlow::Break(Some(Vec::new())));
				}
				let selector = fdb::KeySelector::last_less_than(end.clone());
				let result = txn.get_key(&selector, false).await;
				let key = crate::fdb::retry!(result);
				if key.as_ref() < begin.as_slice() {
					return Err(tg::error!("invalid sandbox process position"));
				}
				let key = Self::unpack(subspace, &key)?;
				let Key::Sandbox(crate::fdb::sandbox::Key::SandboxProcess {
					position: last_position,
					..
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				last_position
					.checked_add(1)
					.and_then(|processes_length| processes_length.checked_add(position))
					.filter(|position| *position >= 0)
					.ok_or_else(|| tg::error!("invalid sandbox process position"))?
			},
			std::io::SeekFrom::Current(_) => {
				return Err(tg::error!(
					"a current sandbox process position is not supported"
				));
			},
		};
		let begin = Self::pack(
			subspace,
			&(
				Kind::SandboxProcess.to_i32().unwrap(),
				bytes.as_ref(),
				position,
			),
		);
		let range = fdb::RangeOption {
			begin: fdb::KeySelector::first_greater_or_equal(begin),
			end: fdb::KeySelector::first_greater_or_equal(end),
			limit: Some(limit),
			mode: fdb::options::StreamingMode::WantAll,
			..Default::default()
		};
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::fdb::retry!(result);
		let processes = entries
			.iter()
			.enumerate()
			.map(|(index, entry)| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Sandbox(crate::fdb::sandbox::Key::SandboxProcess {
					process: process_id,
					position: process_position,
					..
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				let expected_position = position
					.checked_add(index.to_i64().unwrap())
					.ok_or_else(|| tg::error!("invalid sandbox process position"))?;
				if process_position != expected_position {
					return Err(tg::error!("the sandbox process position is invalid"));
				}
				Ok(process_id)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(Some(processes)))
	}
}
