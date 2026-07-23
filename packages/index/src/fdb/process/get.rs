use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_cached_processes(
		&self,
		command: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, crate::process::Process)>> {
		let request = crate::read::Request::TryGetCachedProcesses {
			command: command.clone(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetCachedProcesses(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn try_get_cached_processes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		command: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, crate::process::Process)>> {
		let command_bytes = command.to_bytes();
		let prefix = (
			Kind::CommandCacheableProcess.to_i32().unwrap(),
			command_bytes.as_ref(),
		);
		let prefix = Self::pack(subspace, &prefix);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the cached processes"))?;
		let processes = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Process(crate::fdb::process::Key::CommandCacheableProcess {
					process, ..
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(process)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		drop(entries);
		let mut output = Vec::new();
		for process in processes {
			let Some(data) =
				Self::try_get_process_with_transaction(txn, subspace, &process).await?
			else {
				continue;
			};
			if data.data.is_none() {
				continue;
			}
			output.push((process, data));
		}
		output.sort_unstable_by(|(a_id, a), (b_id, b)| {
			let a_created_at = a.data.as_ref().unwrap().created_at;
			let b_created_at = b.data.as_ref().unwrap().created_at;
			a_created_at.cmp(&b_created_at).then_with(|| a_id.cmp(b_id))
		});
		Ok(output)
	}

	pub async fn process_has_ancestor(
		&self,
		process: &tg::process::Id,
		ancestor: &tg::process::Id,
	) -> tg::Result<bool> {
		if process == ancestor {
			return Ok(true);
		}
		let request = crate::read::Request::ProcessHasAncestor {
			ancestor: ancestor.clone(),
			process: process.clone(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::ProcessHasAncestor(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn process_has_ancestor_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		process: &tg::process::Id,
		ancestor: &tg::process::Id,
	) -> tg::Result<bool> {
		let mut seen = BTreeSet::from([process.clone()]);
		let mut frontier = vec![process.clone()];
		while !frontier.is_empty() {
			let parents =
				futures::future::try_join_all(frontier.iter().map(|process| {
					Self::get_process_parents_with_transaction(txn, subspace, process)
				}))
				.await?;
			frontier = Vec::new();
			for parent in parents.into_iter().flatten() {
				if &parent == ancestor {
					return Ok(true);
				}
				if seen.insert(parent.clone()) {
					frontier.push(parent);
				}
			}
		}
		Ok(false)
	}

	pub async fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = crate::read::Request::TryGetProcesses {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetProcesses(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn try_get_processes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		futures::future::try_join_all(
			ids.iter()
				.map(|id| Self::try_get_process_with_transaction(txn, subspace, id)),
		)
		.await
	}

	pub(crate) async fn try_get_process_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<Option<crate::process::Process>> {
		let key = Key::Process(crate::fdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(crate::process::Process::deserialize(&bytes)?))
	}

	pub(crate) async fn get_process_children_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::process::Id>> {
		let bytes = id.to_bytes();
		let key = (Kind::ProcessChild.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get process children"))?;

		let children = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Process(crate::fdb::process::Key::ProcessChild { child, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(children)
	}

	pub(crate) async fn get_process_parents_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::process::Id>> {
		let bytes = id.to_bytes();
		let key = (Kind::ChildProcess.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get process parents"))?;

		let parents = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Process(crate::fdb::process::Key::ChildProcess { parent, .. }) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(parent)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(parents)
	}

	pub(crate) async fn get_process_objects_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<Vec<(tg::object::Id, crate::process::object::Kind)>> {
		let bytes = id.to_bytes();
		let key = (Kind::ProcessObject.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get process objects"))?;

		let objects = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Process(crate::fdb::process::Key::ProcessObject { kind, object, .. }) =
					key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((object, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(objects)
	}
}
