use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::{
		collections::{BTreeMap, BTreeSet},
		ops::ControlFlow,
	},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_process_node_children(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<crate::process::NodeChildren>> {
		let request = crate::read::Request::TryGetProcessNodeChildren { id: id.clone() };
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetProcessNodeChildren(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

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
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		command: &tg::object::Id,
	) -> tg::Result<ControlFlow<Vec<(tg::process::Id, crate::process::Process)>, fdb::FdbError>> {
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
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);
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
		let processes = {
			let result =
				futures::future::try_join_all(processes.into_iter().map(|process| async move {
					let data = crate::fdb::propagate!(
						Self::try_get_process_with_transaction(txn, subspace, &process).await
					);

					Ok::<_, tg::Error>(ControlFlow::Break((process, data)))
				}))
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
		let mut output = processes
			.into_iter()
			.filter_map(|(process, data)| {
				data.filter(|data| data.data.is_some())
					.map(|data| (process, data))
			})
			.collect::<Vec<_>>();
		output.sort_unstable_by(|(a_id, a), (b_id, b)| {
			let a_created_at = a.data.as_ref().unwrap().created_at;
			let b_created_at = b.data.as_ref().unwrap().created_at;
			a_created_at.cmp(&b_created_at).then_with(|| a_id.cmp(b_id))
		});
		Ok(ControlFlow::Break(output))
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
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		process: &tg::process::Id,
		ancestor: &tg::process::Id,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let mut seen = BTreeSet::from([process.clone()]);
		let mut processes = vec![process.clone()];
		while !processes.is_empty() {
			let parents = {
				let result = futures::future::try_join_all(processes.iter().map(|process| {
					Self::get_process_parents_with_transaction(txn, subspace, process)
				}))
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
			processes = Vec::new();
			for parent in parents.into_iter().flatten() {
				if &parent == ancestor {
					return Ok(ControlFlow::Break(true));
				}
				if seen.insert(parent.clone()) {
					processes.push(parent);
				}
			}
		}
		Ok(ControlFlow::Break(false))
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
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::process::Id],
	) -> tg::Result<ControlFlow<Vec<Option<crate::process::Process>>, fdb::FdbError>> {
		let processes = {
			let result = futures::future::try_join_all(
				ids.iter()
					.map(|id| Self::try_get_process_with_transaction(txn, subspace, id)),
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

		Ok(ControlFlow::Break(processes))
	}

	pub(crate) async fn try_get_process_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<ControlFlow<Option<crate::process::Process>, fdb::FdbError>> {
		let key = Key::Process(crate::fdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::fdb::retry!(result);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(None));
		};
		let process = Some(crate::process::Process::deserialize(&bytes)?);

		Ok(ControlFlow::Break(process))
	}

	pub(crate) async fn get_process_children_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<ControlFlow<Vec<tg::process::Id>, fdb::FdbError>> {
		let bytes = id.to_bytes();
		let key = (Kind::ProcessChild.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::fdb::retry!(result);

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

		Ok(ControlFlow::Break(children))
	}

	pub(crate) async fn try_get_process_children_page_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> tg::Result<ControlFlow<Option<Vec<tg::process::data::Child>>, fdb::FdbError>> {
		let Some(_) =
			crate::fdb::propagate!(Self::try_get_process_with_transaction(txn, subspace, id).await)
		else {
			return Ok(ControlFlow::Break(None));
		};
		if length == 0 {
			return Ok(ControlFlow::Break(Some(Vec::new())));
		}
		let limit = length
			.to_usize()
			.ok_or_else(|| tg::error!("the process child length is too large"))?;
		let bytes = id.to_bytes();
		let key = (Kind::ProcessChild.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let (begin, end) = range_subspace.range();
		let position = match position {
			std::io::SeekFrom::Start(position) => position
				.to_i64()
				.ok_or_else(|| tg::error!("the process child position is too large"))?,
			std::io::SeekFrom::End(position) => {
				if position >= 0 {
					return Ok(ControlFlow::Break(Some(Vec::new())));
				}
				let selector = fdb::KeySelector::last_less_than(end.clone());
				let result = txn.get_key(&selector, false).await;
				let key = crate::fdb::retry!(result);
				if key.as_ref() < begin.as_slice() {
					return Err(tg::error!("invalid process child position"));
				}
				let key = Self::unpack(subspace, &key)?;
				let Key::Process(crate::fdb::process::Key::ProcessChild {
					position: last_position,
					..
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				last_position
					.checked_add(1)
					.and_then(|children_length| children_length.checked_add(position))
					.filter(|position| *position >= 0)
					.ok_or_else(|| tg::error!("invalid process child position"))?
			},
			std::io::SeekFrom::Current(_) => {
				return Err(tg::error!(
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
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::fdb::retry!(result);
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
					return Err(tg::error!("unexpected key type"));
				};
				let expected_position = position
					.checked_add(index.to_i64().unwrap())
					.ok_or_else(|| tg::error!("invalid process child position"))?;
				if child_position != expected_position {
					return Err(tg::error!("the process child position is invalid"));
				}
				let child: tg::process::data::Child = tangram_serialize::from_slice(entry.value())
					.map_err(|error| {
						tg::error!(!error, "failed to deserialize the process child")
					})?;
				if child.process.node != child_id {
					return Err(tg::error!("the process child value does not match its key"));
				}

				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(Some(children)))
	}

	pub(crate) async fn get_process_parents_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<ControlFlow<Vec<tg::process::Id>, fdb::FdbError>> {
		let bytes = id.to_bytes();
		let key = (Kind::ChildProcess.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);

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

		Ok(ControlFlow::Break(parents))
	}

	pub(crate) async fn get_process_objects_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<ControlFlow<Vec<(tg::object::Id, crate::process::object::Kind)>, fdb::FdbError>>
	{
		let bytes = id.to_bytes();
		let key = (Kind::ProcessObject.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);

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

		Ok(ControlFlow::Break(objects))
	}

	pub(crate) async fn try_get_process_node_children_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<ControlFlow<Option<crate::process::NodeChildren>, fdb::FdbError>> {
		let Some(process) =
			crate::fdb::propagate!(Self::try_get_process_with_transaction(txn, subspace, id).await)
		else {
			return Ok(ControlFlow::Break(None));
		};

		let bytes = id.to_bytes();
		let key = (Kind::ProcessChild.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::fdb::retry!(result);
		let mut children = BTreeMap::new();
		for entry in &entries {
			let key = Self::unpack(subspace, entry.key())?;
			let Key::Process(crate::fdb::process::Key::ProcessChild {
				child: child_id, ..
			}) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			let child: tg::process::data::Child = tangram_serialize::from_slice(entry.value())
				.map_err(|error| tg::error!(!error, "failed to deserialize the process child"))?;
			if child.process.node != child_id {
				return Err(tg::error!("the process child value does not match its key"));
			}
			let child: tg::Referent<tg::Id> = child.process.map(Into::into);
			children
				.entry(child.node.clone())
				.and_modify(|existing: &mut tg::Referent<tg::Id>| {
					existing.options.inherit(&child.options);
				})
				.or_insert(child);
		}

		let objects = crate::fdb::propagate!(
			Self::get_process_objects_with_transaction(txn, subspace, id).await
		);
		for (object, _) in objects {
			let id: tg::Id = object.into();
			children
				.entry(id.clone())
				.or_insert_with(|| tg::Referent::with_node(id));
		}

		let complete = process.set.complete();
		let nodes = children.into_values().collect();
		let output = crate::process::NodeChildren { complete, nodes };

		Ok(ControlFlow::Break(Some(output)))
	}
}
