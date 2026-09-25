use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
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

	pub(crate) fn get_sandbox_processes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Vec<(tg::process::Id, crate::process::Process)>> {
		let sandbox = sandbox.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::SandboxProcess.to_i32().unwrap(), sandbox.as_ref()),
		);
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the sandbox processes"))?;
		let mut output = Vec::new();
		for entry in iter {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read a sandbox process"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Sandbox(crate::lmdb::sandbox::Key::SandboxProcess { process, .. }) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			let data = Self::try_get_process_with_transaction(db, subspace, transaction, &process)?
				.ok_or_else(|| tg::error!(%process, "failed to find the sandbox process"))?;
			output.push((process, data));
		}

		Ok(output)
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

	pub(crate) fn try_get_sandboxes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::sandbox::Id],
	) -> tg::Result<Vec<Option<crate::sandbox::Sandbox>>> {
		ids.iter()
			.map(|id| Self::try_get_sandbox_with_transaction(db, subspace, transaction, id))
			.collect()
	}

	pub(crate) fn try_get_sandbox_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<crate::sandbox::Sandbox>> {
		let key = Key::Sandbox(crate::lmdb::sandbox::Key::Sandbox(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox"))?;
		bytes.map(crate::sandbox::Sandbox::deserialize).transpose()
	}

	pub(crate) fn try_get_sandbox_processes_count_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<u64>> {
		if Self::try_get_sandbox_with_transaction(db, subspace, transaction, id)?.is_none() {
			return Ok(None);
		}
		let bytes = id.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::SandboxProcessEntry.to_i32().unwrap(), bytes.as_ref()),
		);
		let entry = db
			.rev_prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the sandbox processes"))?
			.next()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to read the sandbox process"))?;
		let Some((key, _)) = entry else {
			return Ok(Some(0));
		};
		let Key::Sandbox(crate::lmdb::sandbox::Key::SandboxProcessEntry { position, .. }) =
			Self::unpack(subspace, key)?
		else {
			return Err(tg::error!("unexpected key type"));
		};
		let count = u64::try_from(position)
			.map_err(|error| tg::error!(!error, "invalid sandbox process position"))?
			.checked_add(1)
			.ok_or_else(|| tg::error!("invalid sandbox process count"))?;
		Ok(Some(count))
	}

	pub(crate) fn try_get_sandbox_processes_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::sandbox::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> tg::Result<Option<Vec<tg::process::Id>>> {
		let Some(_) = Self::try_get_sandbox_with_transaction(db, subspace, transaction, id)? else {
			return Ok(None);
		};
		if length == 0 {
			return Ok(Some(Vec::new()));
		}
		let length = length
			.to_usize()
			.ok_or_else(|| tg::error!("the sandbox process length is too large"))?;
		let id_bytes = id.to_bytes();
		let prefix = &(
			Kind::SandboxProcessEntry.to_i32().unwrap(),
			id_bytes.as_ref(),
		);
		let prefix = Self::pack(subspace, prefix);
		let position = match position {
			std::io::SeekFrom::Start(position) => position
				.to_i64()
				.ok_or_else(|| tg::error!("the sandbox process position is too large"))?,
			std::io::SeekFrom::End(position) => {
				if position >= 0 {
					return Ok(Some(Vec::new()));
				}
				let entry = db
					.rev_prefix_iter(transaction, &prefix)
					.map_err(|error| tg::error!(!error, "failed to get sandbox processes"))?
					.next()
					.transpose()
					.map_err(|error| tg::error!(!error, "failed to read a sandbox process entry"))?
					.ok_or_else(|| tg::error!("invalid sandbox process position"))?;
				let key = Self::unpack(subspace, entry.0)?;
				let Key::Sandbox(crate::lmdb::sandbox::Key::SandboxProcessEntry {
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
		let start = Self::pack(
			subspace,
			&(
				Kind::SandboxProcessEntry.to_i32().unwrap(),
				id_bytes.as_ref(),
				position,
			),
		);
		let range = (
			std::ops::Bound::Included(start.as_slice()),
			std::ops::Bound::Unbounded,
		);
		let entries = db
			.range(transaction, &range)
			.map_err(|error| tg::error!(!error, "failed to get sandbox processes"))?;
		let mut processes = Vec::with_capacity(length);
		for entry in entries.take(length) {
			let (key, _) = entry
				.map_err(|error| tg::error!(!error, "failed to read a sandbox process entry"))?;
			if !key.starts_with(&prefix) {
				break;
			}
			let key = Self::unpack(subspace, key)?;
			let Key::Sandbox(crate::lmdb::sandbox::Key::SandboxProcessEntry {
				process: process_id,
				position: process_position,
				..
			}) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			let expected_position = position
				.checked_add(processes.len().to_i64().unwrap())
				.ok_or_else(|| tg::error!("invalid sandbox process position"))?;
			if process_position != expected_position {
				return Err(tg::error!("the sandbox process position is invalid"));
			}
			processes.push(process_id);
		}

		Ok(Some(processes))
	}
}
