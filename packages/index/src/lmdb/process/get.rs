use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	std::collections::{BTreeMap, BTreeSet, VecDeque},
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

	pub(crate) fn try_get_cached_processes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		command: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, crate::process::Process)>> {
		let command_bytes = command.to_bytes();
		let prefix = &(
			Kind::CommandCacheableProcess.to_i32().unwrap(),
			command_bytes.as_ref(),
		);
		let prefix = Self::pack(subspace, prefix);
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the cached processes"))?;
		let mut output = Vec::new();
		for entry in iter {
			let (key, _) = entry
				.map_err(|error| tg::error!(!error, "failed to read a cached process entry"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Process(crate::lmdb::process::Key::CommandCacheableProcess {
				process, ..
			}) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			let Some(data) =
				Self::try_get_process_with_transaction(db, subspace, transaction, &process)?
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

	pub(crate) fn process_has_ancestor_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		process: &tg::process::Id,
		ancestor: &tg::process::Id,
	) -> tg::Result<bool> {
		let mut seen = BTreeSet::from([process.clone()]);
		let mut queue = VecDeque::from([process.clone()]);
		while let Some(process) = queue.pop_front() {
			for parent in
				Self::get_process_parents_with_transaction(db, subspace, transaction, &process)?
			{
				if &parent == ancestor {
					return Ok(true);
				}
				if seen.insert(parent.clone()) {
					queue.push_back(parent);
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

	pub(crate) fn try_get_processes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		ids.iter()
			.map(|id| Self::try_get_process_with_transaction(db, subspace, transaction, id))
			.collect()
	}

	pub(crate) fn try_get_process_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Option<crate::process::Process>> {
		let key = Key::Process(crate::lmdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(crate::process::Process::deserialize(bytes)?))
	}

	pub(crate) fn get_process_children_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::process::Id>> {
		let id_bytes = id.to_bytes();
		let prefix = &(Kind::ProcessChild.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, prefix);
		let mut children = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get process children"))?;
		for entry in iter {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read process child entry"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Process(crate::lmdb::process::Key::ProcessChild { child, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			children.push(child);
		}
		Ok(children)
	}

	pub(crate) fn try_get_process_children_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> tg::Result<Option<Vec<tg::process::data::Child>>> {
		let Some(_) = Self::try_get_process_with_transaction(db, subspace, transaction, id)? else {
			return Ok(None);
		};
		if length == 0 {
			return Ok(Some(Vec::new()));
		}
		let length = length
			.to_usize()
			.ok_or_else(|| tg::error!("the process child length is too large"))?;
		let id_bytes = id.to_bytes();
		let prefix = &(Kind::ProcessChild.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, prefix);
		let position = match position {
			std::io::SeekFrom::Start(position) => position
				.to_i64()
				.ok_or_else(|| tg::error!("the process child position is too large"))?,
			std::io::SeekFrom::End(position) => {
				if position >= 0 {
					return Ok(Some(Vec::new()));
				}
				let entry = db
					.rev_prefix_iter(transaction, &prefix)
					.map_err(|error| tg::error!(!error, "failed to get process children"))?
					.next()
					.transpose()
					.map_err(|error| tg::error!(!error, "failed to read a process child entry"))?
					.ok_or_else(|| tg::error!("invalid process child position"))?;
				let key = Self::unpack(subspace, entry.0)?;
				let Key::Process(crate::lmdb::process::Key::ProcessChild {
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
		let start = Self::pack(
			subspace,
			&(
				Kind::ProcessChild.to_i32().unwrap(),
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
			.map_err(|error| tg::error!(!error, "failed to get process children"))?;
		let mut children = Vec::with_capacity(length);
		for entry in entries.take(length) {
			let (key, value) = entry
				.map_err(|error| tg::error!(!error, "failed to read a process child entry"))?;
			if !key.starts_with(&prefix) {
				break;
			}
			let key = Self::unpack(subspace, key)?;
			let Key::Process(crate::lmdb::process::Key::ProcessChild {
				child: child_id,
				position: child_position,
				..
			}) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			let expected_position = position
				.checked_add(children.len().to_i64().unwrap())
				.ok_or_else(|| tg::error!("invalid process child position"))?;
			if child_position != expected_position {
				return Err(tg::error!("the process child position is invalid"));
			}
			let child: tg::process::data::Child = tangram_serialize::from_slice(value)
				.map_err(|error| tg::error!(!error, "failed to deserialize the process child"))?;
			if child.process.node != child_id {
				return Err(tg::error!("the process child value does not match its key"));
			}
			children.push(child);
		}

		Ok(Some(children))
	}

	pub(crate) fn get_process_parents_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::process::Id>> {
		let id_bytes = id.to_bytes();
		let prefix = &(Kind::ChildProcess.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, prefix);
		let mut parents = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get process parents"))?;
		for entry in iter {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read child process entry"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Process(crate::lmdb::process::Key::ChildProcess { parent, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			parents.push(parent);
		}
		Ok(parents)
	}

	pub(crate) fn get_process_objects_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Vec<(tg::object::Id, crate::process::object::Kind)>> {
		let id_bytes = id.to_bytes();
		let prefix = &(Kind::ProcessObject.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, prefix);
		let mut objects = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get process objects"))?;
		for entry in iter {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read process object entry"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Process(crate::lmdb::process::Key::ProcessObject { kind, object, .. }) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			objects.push((object, kind));
		}
		Ok(objects)
	}

	pub(crate) fn try_get_process_node_children_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Option<crate::process::NodeChildren>> {
		let Some(process) = Self::try_get_process_with_transaction(db, subspace, transaction, id)?
		else {
			return Ok(None);
		};

		let id_bytes = id.to_bytes();
		let prefix = &(Kind::ProcessChild.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, prefix);
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get process children"))?;
		let mut children = BTreeMap::new();
		for entry in iter {
			let (key, value) = entry
				.map_err(|error| tg::error!(!error, "failed to read a process child entry"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Process(crate::lmdb::process::Key::ProcessChild {
				child: child_id, ..
			}) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			let child: tg::process::data::Child = tangram_serialize::from_slice(value)
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

		let objects = Self::get_process_objects_with_transaction(db, subspace, transaction, id)?;
		for (object, _) in objects {
			let id: tg::Id = object.into();
			children
				.entry(id.clone())
				.or_insert_with(|| tg::Referent::with_node(id));
		}

		let complete = process.set.complete();
		let nodes = children.into_values().collect();
		let output = crate::process::NodeChildren { complete, nodes };

		Ok(Some(output))
	}
}
