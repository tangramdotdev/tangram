use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	std::collections::{BTreeMap, BTreeSet, VecDeque},
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn try_get_cached_processes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		command: &tg::object::Id,
	) -> tg::Result<
		Vec<(
			tg::process::Id,
			tangram_index::process::Process,
			tg::process::Data,
		)>,
	> {
		let processes = crate::page::collect(|after| {
			Self::get_command_cacheable_processes_page_with_transaction(
				db,
				subspace,
				transaction,
				command,
				after,
				crate::page::PAGE_SIZE,
			)
		})?;
		let mut output = Vec::new();
		for process in processes {
			let Some((record, Some(data))) = Self::try_get_process_with_data_with_transaction(
				db,
				subspace,
				transaction,
				&process,
			)?
			else {
				continue;
			};
			output.push((process, record, data));
		}
		output.sort_unstable_by(|(a_id, _, a), (b_id, _, b)| {
			a.created_at.cmp(&b.created_at).then_with(|| a_id.cmp(b_id))
		});

		Ok(output)
	}

	pub(crate) fn get_command_cacheable_processes_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		command: &tg::object::Id,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<crate::page::Page<tg::process::Id>> {
		let bytes = command.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				Kind::CommandCacheableProcess.to_i32().unwrap(),
				bytes.as_ref(),
			),
		);
		let (entries, cursor) = Self::get_entry_page_with_transaction(
			db,
			subspace,
			transaction,
			&prefix,
			after,
			limit,
		)?;
		let processes = entries
			.into_iter()
			.map(|(key, _)| {
				let Key::Process(crate::process::Key::CommandCacheableProcess { process, .. }) =
					key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(process)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok((processes, cursor))
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
			for parent in crate::page::collect(|after| {
				Self::get_process_parents_page_with_transaction(
					db,
					subspace,
					transaction,
					&process,
					after,
					crate::page::PAGE_SIZE,
				)
			})? {
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

	pub(crate) fn try_get_processes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tangram_index::process::Process>>> {
		ids.iter()
			.map(|id| Self::try_get_process_with_transaction(db, subspace, transaction, id))
			.collect()
	}

	pub(crate) fn try_get_processes_with_data_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(tangram_index::process::Process, Option<tg::process::Data>)>>> {
		ids.iter()
			.map(|id| {
				Self::try_get_process_with_data_with_transaction(db, subspace, transaction, id)
			})
			.collect()
	}

	pub(crate) fn try_get_process_with_data_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Option<(tangram_index::process::Process, Option<tg::process::Data>)>> {
		let Some(process) = Self::try_get_process_with_transaction(db, subspace, transaction, id)?
		else {
			return Ok(None);
		};
		let data = Self::try_get_process_data_with_transaction(db, subspace, transaction, id)?;
		Ok(Some((process, data)))
	}

	pub(crate) fn try_get_process_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Option<tangram_index::process::Process>> {
		let key = Key::Process(crate::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(tangram_index::process::Process::deserialize(bytes)?))
	}

	pub(crate) fn try_get_process_data_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Data>> {
		let id_bytes = id.to_bytes();
		let prefix = (Kind::ProcessData.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, &prefix);
		let fragments = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, %id, "failed to get the process data"))?
			.map(|entry| {
				let (_, value) = entry.map_err(
					|error| tg::error!(!error, %id, "failed to read a process data fragment"),
				)?;
				Ok(value)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		if fragments.is_empty() {
			return Ok(None);
		}
		let data = tangram_index::process::data::deserialize(&fragments)?;

		Ok(Some(data))
	}

	pub(crate) fn get_process_children_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<crate::page::Page<tg::process::Id>> {
		let bytes = id.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::ProcessChild.to_i32().unwrap(), bytes.as_ref()),
		);
		let (entries, cursor) = Self::get_entry_page_with_transaction(
			db,
			subspace,
			transaction,
			&prefix,
			after,
			limit,
		)?;
		let items = entries
			.into_iter()
			.map(|(key, _)| {
				let Key::Process(crate::process::Key::ProcessChild { child, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok((items, cursor))
	}

	pub(crate) fn try_get_process_children_count_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Option<u64>> {
		if Self::try_get_process_with_transaction(db, subspace, transaction, id)?.is_none() {
			return Ok(None);
		}
		let bytes = id.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::ProcessChild.to_i32().unwrap(), bytes.as_ref()),
		);
		let entry = db
			.rev_prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the process children"))?
			.next()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to read the process child"))?;
		let Some((key, _)) = entry else {
			return Ok(Some(0));
		};
		let Key::Process(crate::process::Key::ProcessChild { position, .. }) =
			Self::unpack(subspace, key)?
		else {
			return Err(tg::error!("unexpected key type"));
		};
		let count = u64::try_from(position)
			.map_err(|error| tg::error!(!error, "invalid process child position"))?
			.checked_add(1)
			.ok_or_else(|| tg::error!("invalid process child count"))?;
		Ok(Some(count))
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
				let Key::Process(crate::process::Key::ProcessChild {
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
			let Key::Process(crate::process::Key::ProcessChild {
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

	pub(crate) fn get_process_parents_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<crate::page::Page<tg::process::Id>> {
		let bytes = id.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::ChildProcess.to_i32().unwrap(), bytes.as_ref()),
		);
		let (entries, cursor) = Self::get_entry_page_with_transaction(
			db,
			subspace,
			transaction,
			&prefix,
			after,
			limit,
		)?;
		let items = entries
			.into_iter()
			.map(|(key, _)| {
				let Key::Process(crate::process::Key::ChildProcess { parent, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(parent)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok((items, cursor))
	}

	pub(crate) fn get_process_objects_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
		kind: Option<tangram_index::process::object::Kind>,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<crate::page::Page<(tg::object::Id, tangram_index::process::object::Kind)>> {
		let bytes = id.to_bytes();
		let prefix = match kind {
			Some(kind) => Self::pack(
				subspace,
				&(
					Kind::ProcessObject.to_i32().unwrap(),
					bytes.as_ref(),
					kind.to_i32().unwrap(),
				),
			),
			None => Self::pack(
				subspace,
				&(Kind::ProcessObject.to_i32().unwrap(), bytes.as_ref()),
			),
		};
		let (entries, cursor) = Self::get_entry_page_with_transaction(
			db,
			subspace,
			transaction,
			&prefix,
			after,
			limit,
		)?;
		let items = entries
			.into_iter()
			.map(|(key, _)| {
				let Key::Process(crate::process::Key::ProcessObject { kind, object, .. }) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((object, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok((items, cursor))
	}

	pub(crate) fn try_get_process_children_and_objects_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Option<tangram_index::process::NodeChildren>> {
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
			let Key::Process(crate::process::Key::ProcessChild {
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

		let objects = crate::page::collect(|after| {
			Self::get_process_objects_page_with_transaction(
				db,
				subspace,
				transaction,
				id,
				None,
				after,
				crate::page::PAGE_SIZE,
			)
		})?;
		for (object, _) in objects {
			let id: tg::Id = object.into();
			children
				.entry(id.clone())
				.or_insert_with(|| tg::Referent::with_node(id));
		}

		let complete = process.set.complete();
		let nodes = children.into_values().collect();
		let output = tangram_index::process::NodeChildren { complete, nodes };

		Ok(Some(output))
	}
}
