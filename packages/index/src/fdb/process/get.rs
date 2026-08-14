use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_process_children(
		&self,
		id: &tg::process::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> tg::Result<Option<Vec<tg::process::data::Child>>> {
		let request = crate::read::Request::TryGetProcessChildren {
			id: id.clone(),
			length,
			position,
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetProcessChildren(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

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
	) -> crate::fdb::Result<Vec<(tg::process::Id, crate::process::Process)>> {
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
		let entries = txn.get_range(&range, 1, false).await?;
		let processes = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Process(crate::fdb::process::Key::CommandCacheableProcess {
					process, ..
				}) = key
				else {
					return Err(crate::fdb::error!("unexpected key type"));
				};
				Ok(process)
			})
			.collect::<crate::fdb::Result<Vec<_>>>()?;
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
	) -> crate::fdb::Result<bool> {
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
	) -> crate::fdb::Result<Vec<Option<crate::process::Process>>> {
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
	) -> crate::fdb::Result<Option<crate::process::Process>> {
		let key = Key::Process(crate::fdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = txn.get(&key, false).await?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(
			crate::process::Process::deserialize(&bytes).map_err(crate::fdb::custom_error)?,
		))
	}

	pub(crate) async fn get_process_children_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> crate::fdb::Result<Vec<tg::process::Id>> {
		let bytes = id.to_bytes();
		let key = (Kind::ProcessChild.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await?;

		let children = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Process(crate::fdb::process::Key::ProcessChild { child, .. }) = key else {
					return Err(crate::fdb::error!("unexpected key type"));
				};
				Ok(child)
			})
			.collect::<crate::fdb::Result<Vec<_>>>()?;

		Ok(children)
	}

	pub(crate) async fn try_get_process_children_page_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> crate::fdb::Result<Option<Vec<tg::process::data::Child>>> {
		let Some(_) = Self::try_get_process_with_transaction(txn, subspace, id).await? else {
			return Ok(None);
		};
		if length == 0 {
			return Ok(Some(Vec::new()));
		}
		let limit = length
			.to_usize()
			.ok_or_else(|| crate::fdb::error!("the process child length is too large"))?;
		let bytes = id.to_bytes();
		let key = (Kind::ProcessChild.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let (begin, end) = range_subspace.range();
		let position = match position {
			std::io::SeekFrom::Start(position) => position
				.to_i64()
				.ok_or_else(|| crate::fdb::error!("the process child position is too large"))?,
			std::io::SeekFrom::End(position) => {
				if position >= 0 {
					return Ok(Some(Vec::new()));
				}
				let selector = fdb::KeySelector::last_less_than(end.clone());
				let key = txn.get_key(&selector, false).await?;
				if key.as_ref() < begin.as_slice() {
					return Err(crate::fdb::error!("invalid process child position"));
				}
				let key = Self::unpack(subspace, &key)?;
				let Key::Process(crate::fdb::process::Key::ProcessChild {
					position: last_position,
					..
				}) = key
				else {
					return Err(crate::fdb::error!("unexpected key type"));
				};
				last_position
					.checked_add(1)
					.and_then(|children_length| children_length.checked_add(position))
					.filter(|position| *position >= 0)
					.ok_or_else(|| crate::fdb::error!("invalid process child position"))?
			},
			std::io::SeekFrom::Current(_) => {
				return Err(crate::fdb::error!(
					"a current process child position is not supported"
				));
			},
		};
		let begin = Self::pack(
			subspace,
			&(
				Kind::ProcessChild.to_i32().unwrap(),
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
		let entries = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await?;
		let children = entries
			.iter()
			.enumerate()
			.map(|(index, entry)| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Process(crate::fdb::process::Key::ProcessChild {
					child: child_id,
					position: child_position,
					..
				}) = key
				else {
					return Err(crate::fdb::error!("unexpected key type"));
				};
				let expected_position = position
					.checked_add(index.to_i64().unwrap())
					.ok_or_else(|| crate::fdb::error!("invalid process child position"))?;
				if child_position != expected_position {
					return Err(crate::fdb::error!("the process child position is invalid"));
				}
				let child: tg::process::data::Child = tangram_serialize::from_slice(entry.value())
					.map_err(|error| {
						crate::fdb::error!(!error, "failed to deserialize the process child")
					})?;
				if child.process.node != child_id {
					return Err(crate::fdb::error!(
						"the process child value does not match its key"
					));
				}

				Ok(child)
			})
			.collect::<crate::fdb::Result<Vec<_>>>()?;
		Ok(Some(children))
	}

	pub(crate) async fn get_process_parents_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> crate::fdb::Result<Vec<tg::process::Id>> {
		let bytes = id.to_bytes();
		let key = (Kind::ChildProcess.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn.get_range(&range, 1, false).await?;

		let parents = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Process(crate::fdb::process::Key::ChildProcess { parent, .. }) = key
				else {
					return Err(crate::fdb::error!("unexpected key type"));
				};
				Ok(parent)
			})
			.collect::<crate::fdb::Result<Vec<_>>>()?;

		Ok(parents)
	}

	pub(crate) async fn get_process_objects_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> crate::fdb::Result<Vec<(tg::object::Id, crate::process::object::Kind)>> {
		let bytes = id.to_bytes();
		let key = (Kind::ProcessObject.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn.get_range(&range, 1, false).await?;

		let objects = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Process(crate::fdb::process::Key::ProcessObject { kind, object, .. }) =
					key
				else {
					return Err(crate::fdb::error!("unexpected key type"));
				};
				Ok((object, kind))
			})
			.collect::<crate::fdb::Result<Vec<_>>>()?;

		Ok(objects)
	}
}
