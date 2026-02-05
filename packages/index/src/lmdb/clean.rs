use {
	super::{Db, Index, ItemKind, Key, KeyKind, Request, Response},
	crate::{CacheEntry, CleanOutput, Object, Process, ProcessObjectKind},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

struct Candidate {
	touched_at: i64,
	item: Item,
}

#[derive(Clone)]
enum Item {
	CacheEntry(tg::artifact::Id),
	Object(tg::object::Id),
	Process(tg::process::Id),
}

impl Index {
	pub async fn clean(
		&self,
		max_touched_at: i64,
		batch_size: usize,
		_partition_start: u64,
		_partition_count: u64,
	) -> tg::Result<CleanOutput> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Clean {
			max_touched_at,
			batch_size,
		};
		self.sender_low
			.send((request, sender))
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		match response {
			Response::CleanOutput(output) => Ok(output),
			_ => Err(tg::error!("unexpected response")),
		}
	}

	pub(super) fn task_clean(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		max_touched_at: i64,
		batch_size: usize,
	) -> tg::Result<CleanOutput> {
		let mut output = CleanOutput::default();

		let prefix = (KeyKind::Clean.to_i32().unwrap(),).pack_to_vec();
		let mut candidates: Vec<Candidate> = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to iterate clean keys"))?;
		for result in iter {
			if candidates.len() >= batch_size {
				break;
			}
			let (key, _) =
				result.map_err(|source| tg::error!(!source, "failed to read clean key"))?;
			let key =
				fdbt::unpack(key).map_err(|source| tg::error!(!source, "failed to unpack key"))?;
			let Key::Clean {
				touched_at,
				kind,
				id,
			} = key
			else {
				return Err(tg::error!("expected clean key"));
			};
			if touched_at > max_touched_at {
				continue;
			}
			let item = match kind {
				ItemKind::CacheEntry => {
					let tg::Either::Left(id) = id else {
						return Err(tg::error!("expected object id for cache entry"));
					};
					let id = tg::artifact::Id::try_from(id)
						.map_err(|source| tg::error!(!source, "invalid artifact id"))?;
					Item::CacheEntry(id)
				},
				ItemKind::Object => {
					let tg::Either::Left(id) = id else {
						return Err(tg::error!("expected object id for object"));
					};
					Item::Object(id)
				},
				ItemKind::Process => {
					let tg::Either::Right(id) = id else {
						return Err(tg::error!("expected process id for process"));
					};
					Item::Process(id)
				},
			};
			candidates.push(Candidate { touched_at, item });
		}

		for candidate in &candidates {
			let reference_count = match &candidate.item {
				Item::CacheEntry(id) => {
					Self::compute_cache_entry_reference_count(db, transaction, id)?
				},
				Item::Object(id) => Self::compute_object_reference_count(db, transaction, id)?,
				Item::Process(id) => Self::compute_process_reference_count(db, transaction, id)?,
			};

			let item = if reference_count > 0 {
				Self::set_reference_count(db, transaction, &candidate.item, reference_count)?;
				None
			} else {
				Self::delete_item(db, transaction, &candidate.item)?;
				Some(candidate.item.clone())
			};

			let (kind, id) = match &candidate.item {
				Item::CacheEntry(id) => (ItemKind::CacheEntry, tg::Either::Left(id.clone().into())),
				Item::Object(id) => (ItemKind::Object, tg::Either::Left(id.clone())),
				Item::Process(id) => (ItemKind::Process, tg::Either::Right(id.clone())),
			};
			let key = Key::Clean {
				touched_at: candidate.touched_at,
				kind,
				id,
			}
			.pack_to_vec();
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete clean key"))?;

			if let Some(item) = item {
				match item {
					Item::CacheEntry(id) => output.cache_entries.push(id),
					Item::Object(id) => output.objects.push(id),
					Item::Process(id) => output.processes.push(id),
				}
			}
		}

		output.done = candidates.is_empty();

		Ok(output)
	}

	fn compute_cache_entry_reference_count(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::artifact::Id,
	) -> tg::Result<u64> {
		let cache_entry_object_prefix = (
			KeyKind::CacheEntryObject.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let cache_entry_object_count =
			Self::count_keys_with_prefix(db, transaction, &cache_entry_object_prefix)?;

		let dependency_cache_entry_prefix = (
			KeyKind::DependencyCacheEntry.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let dependency_cache_entry_count =
			Self::count_keys_with_prefix(db, transaction, &dependency_cache_entry_prefix)?;

		Ok(cache_entry_object_count + dependency_cache_entry_count)
	}

	fn compute_object_reference_count(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<u64> {
		let child_object_prefix = (
			KeyKind::ChildObject.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let child_object_count =
			Self::count_keys_with_prefix(db, transaction, &child_object_prefix)?;

		let object_process_prefix = (
			KeyKind::ObjectProcess.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let object_process_count =
			Self::count_keys_with_prefix(db, transaction, &object_process_prefix)?;

		// Count tags referencing this object.
		let item_tag_prefix =
			(KeyKind::ItemTag.to_i32().unwrap(), id.to_bytes().as_ref()).pack_to_vec();
		let item_tag_count = Self::count_keys_with_prefix(db, transaction, &item_tag_prefix)?;

		Ok(child_object_count + object_process_count + item_tag_count)
	}

	fn compute_process_reference_count(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<u64> {
		let child_process_prefix = (
			KeyKind::ChildProcess.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let child_process_count =
			Self::count_keys_with_prefix(db, transaction, &child_process_prefix)?;

		// Count tags referencing this process.
		let item_tag_prefix =
			(KeyKind::ItemTag.to_i32().unwrap(), id.to_bytes().as_ref()).pack_to_vec();
		let item_tag_count = Self::count_keys_with_prefix(db, transaction, &item_tag_prefix)?;

		Ok(child_process_count + item_tag_count)
	}

	fn count_keys_with_prefix(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		prefix: &[u8],
	) -> tg::Result<u64> {
		let mut count = 0u64;
		let iter = db
			.prefix_iter(transaction, prefix)
			.map_err(|source| tg::error!(!source, "failed to iterate keys with prefix"))?;
		for result in iter {
			result.map_err(|source| tg::error!(!source, "failed to read key"))?;
			count += 1;
		}
		Ok(count)
	}

	fn set_reference_count(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		item: &Item,
		reference_count: u64,
	) -> tg::Result<()> {
		match item {
			Item::CacheEntry(id) => {
				let key = Key::CacheEntry(id.clone()).pack_to_vec();
				if let Some(bytes) = db
					.get(transaction, &key)
					.map_err(|source| tg::error!(!source, "failed to get cache entry"))?
				{
					let mut entry = CacheEntry::deserialize(bytes)?;
					entry.reference_count = reference_count;
					let bytes = entry.serialize()?;
					db.put(transaction, &key, &bytes)
						.map_err(|source| tg::error!(!source, "failed to put cache entry"))?;
				}
			},
			Item::Object(id) => {
				let key = Key::Object(id.clone()).pack_to_vec();
				if let Some(bytes) = db
					.get(transaction, &key)
					.map_err(|source| tg::error!(!source, "failed to get object"))?
				{
					let mut object = Object::deserialize(bytes)?;
					object.reference_count = reference_count;
					let bytes = object.serialize()?;
					db.put(transaction, &key, &bytes)
						.map_err(|source| tg::error!(!source, "failed to put object"))?;
				}
			},
			Item::Process(id) => {
				let key = Key::Process(id.clone()).pack_to_vec();
				if let Some(bytes) = db
					.get(transaction, &key)
					.map_err(|source| tg::error!(!source, "failed to get process"))?
				{
					let mut process = Process::deserialize(bytes)?;
					process.reference_count = reference_count;
					let bytes = process.serialize()?;
					db.put(transaction, &key, &bytes)
						.map_err(|source| tg::error!(!source, "failed to put process"))?;
				}
			},
		}
		Ok(())
	}

	fn delete_item(db: &Db, transaction: &mut lmdb::RwTxn<'_>, item: &Item) -> tg::Result<()> {
		match item {
			Item::CacheEntry(id) => Self::delete_cache_entry(db, transaction, id),
			Item::Object(id) => Self::delete_object(db, transaction, id),
			Item::Process(id) => Self::delete_process(db, transaction, id),
		}
	}

	fn delete_cache_entry(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::artifact::Id,
	) -> tg::Result<()> {
		let key = Key::CacheEntry(id.clone()).pack_to_vec();
		db.delete(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to delete cache entry"))?;

		let prefix = (
			KeyKind::CacheEntryDependency.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let iter = db.prefix_iter(transaction, &prefix).map_err(|source| {
			tg::error!(!source, "failed to iterate cache entry dependency keys")
		})?;
		let mut entries = Vec::new();
		for result in iter {
			let (key, _) = result.map_err(|source| {
				tg::error!(!source, "failed to read cache entry dependency key")
			})?;
			let key = fdbt::unpack(key).map_err(|source| {
				tg::error!(!source, "failed to unpack cache entry dependency key")
			})?;
			let Key::CacheEntryDependency { dependency, .. } = &key else {
				return Err(tg::error!("expected cache entry dependency key"));
			};
			entries.push((key.pack_to_vec(), dependency.clone()));
		}

		for (key, _) in &entries {
			db.delete(transaction, key).map_err(|source| {
				tg::error!(!source, "failed to delete cache entry dependency key")
			})?;
		}

		for (_, dependency) in entries {
			let key = Key::DependencyCacheEntry {
				dependency: dependency.clone(),
				cache_entry: id.clone(),
			}
			.pack_to_vec();
			db.delete(transaction, &key).map_err(|source| {
				tg::error!(!source, "failed to delete dependency cache entry key")
			})?;

			Self::decrement_cache_entry_reference_count(db, transaction, &dependency)?;
		}

		Ok(())
	}

	fn delete_object(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<()> {
		let key = Key::Object(id.clone()).pack_to_vec();
		let cache_entry = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to get object"))?
			.and_then(|bytes| Object::deserialize(bytes).ok())
			.and_then(|obj| obj.cache_entry);

		db.delete(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to delete object"))?;

		let prefix = (
			KeyKind::ObjectChild.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to iterate object child keys"))?;
		let mut entries = Vec::new();
		for result in iter {
			let (key, _) =
				result.map_err(|source| tg::error!(!source, "failed to read object child key"))?;
			let key = fdbt::unpack(key)
				.map_err(|source| tg::error!(!source, "failed to unpack object child key"))?;
			let Key::ObjectChild { child, .. } = &key else {
				return Err(tg::error!("expected object child key"));
			};
			entries.push((key.pack_to_vec(), child.clone()));
		}
		for (key, _) in &entries {
			db.delete(transaction, key)
				.map_err(|source| tg::error!(!source, "failed to delete object child key"))?;
		}

		for (_, child) in &entries {
			let key = Key::ChildObject {
				child: child.clone(),
				object: id.clone(),
			}
			.pack_to_vec();
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete child object key"))?;
		}
		for (_, child) in entries {
			Self::decrement_object_reference_count(db, transaction, &child)?;
		}

		if let Some(cache_entry) = &cache_entry {
			let key = Key::ObjectCacheEntry {
				object: id.clone(),
				cache_entry: cache_entry.clone(),
			}
			.pack_to_vec();
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete object cache entry"))?;

			let key = Key::CacheEntryObject {
				cache_entry: cache_entry.clone(),
				object: id.clone(),
			}
			.pack_to_vec();
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete cache entry object"))?;

			Self::decrement_cache_entry_reference_count(db, transaction, cache_entry)?;
		}

		Ok(())
	}

	fn delete_process(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<()> {
		let key = Key::Process(id.clone()).pack_to_vec();
		db.delete(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to delete process"))?;

		let prefix = (
			KeyKind::ProcessChild.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to iterate process child keys"))?;
		let mut entries = Vec::new();
		for result in iter {
			let (key, _) =
				result.map_err(|source| tg::error!(!source, "failed to read process child key"))?;
			let key = fdbt::unpack(key)
				.map_err(|source| tg::error!(!source, "failed to unpack process child key"))?;
			let Key::ProcessChild { child, .. } = &key else {
				return Err(tg::error!("expected process child key"));
			};
			entries.push((key.pack_to_vec(), child.clone()));
		}
		for (key, _) in &entries {
			db.delete(transaction, key)
				.map_err(|source| tg::error!(!source, "failed to delete process child key"))?;
		}

		for (_, child) in &entries {
			let key = Key::ChildProcess {
				child: child.clone(),
				parent: id.clone(),
			}
			.pack_to_vec();
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete child process key"))?;
		}
		for (_, child) in entries {
			Self::decrement_process_reference_count(db, transaction, &child)?;
		}

		let prefix = (
			KeyKind::ProcessObject.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to iterate process object keys"))?;
		let mut object_entries: Vec<(Vec<u8>, tg::object::Id, ProcessObjectKind)> = Vec::new();
		for result in iter {
			let (key, _) = result
				.map_err(|source| tg::error!(!source, "failed to read process object key"))?;
			let key = fdbt::unpack(key)
				.map_err(|source| tg::error!(!source, "failed to unpack process object key"))?;
			let Key::ProcessObject { kind, object, .. } = &key else {
				return Err(tg::error!("expected process object key"));
			};
			object_entries.push((key.pack_to_vec(), object.clone(), *kind));
		}
		for (key, _, _) in &object_entries {
			db.delete(transaction, key)
				.map_err(|source| tg::error!(!source, "failed to delete process object key"))?;
		}

		for (_, object, kind) in &object_entries {
			let key = Key::ObjectProcess {
				object: object.clone(),
				kind: *kind,
				process: id.clone(),
			}
			.pack_to_vec();
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete object process key"))?;
		}
		for (_, object, _) in object_entries {
			Self::decrement_object_reference_count(db, transaction, &object)?;
		}

		Ok(())
	}

	fn decrement_cache_entry_reference_count(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::artifact::Id,
	) -> tg::Result<()> {
		let key = Key::CacheEntry(id.clone()).pack_to_vec();
		if let Some(bytes) = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to get cache entry"))?
		{
			let mut entry = CacheEntry::deserialize(bytes)?;
			let reference_count = entry.reference_count;
			if reference_count > 1 {
				entry.reference_count = reference_count - 1;
				let bytes = entry.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|source| tg::error!(!source, "failed to put cache entry"))?;
			} else {
				entry.reference_count = 0;
				let bytes = entry.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|source| tg::error!(!source, "failed to put cache entry"))?;

				let key = Key::Clean {
					touched_at: entry.touched_at,
					kind: ItemKind::CacheEntry,
					id: tg::Either::Left(id.clone().into()),
				}
				.pack_to_vec();
				db.put(transaction, &key, &[])
					.map_err(|source| tg::error!(!source, "failed to put clean key"))?;
			}
		}
		Ok(())
	}

	pub(super) fn decrement_object_reference_count(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<()> {
		let key = Key::Object(id.clone()).pack_to_vec();
		if let Some(bytes) = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to get object"))?
		{
			let mut object = Object::deserialize(bytes)?;
			let reference_count = object.reference_count;
			if reference_count > 1 {
				object.reference_count = reference_count - 1;
				let bytes = object.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|source| tg::error!(!source, "failed to put object"))?;
			} else {
				object.reference_count = 0;
				let bytes = object.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|source| tg::error!(!source, "failed to put object"))?;

				let key = Key::Clean {
					touched_at: object.touched_at,
					kind: ItemKind::Object,
					id: tg::Either::Left(id.clone()),
				}
				.pack_to_vec();
				db.put(transaction, &key, &[])
					.map_err(|source| tg::error!(!source, "failed to put clean key"))?;
			}
		}
		Ok(())
	}

	pub(super) fn decrement_process_reference_count(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<()> {
		let key = Key::Process(id.clone()).pack_to_vec();
		if let Some(bytes) = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to get process"))?
		{
			let mut process = Process::deserialize(bytes)?;
			let reference_count = process.reference_count;
			if reference_count > 1 {
				process.reference_count = reference_count - 1;
				let bytes = process.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|source| tg::error!(!source, "failed to put process"))?;
			} else {
				process.reference_count = 0;
				let bytes = process.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|source| tg::error!(!source, "failed to put process"))?;

				let key = Key::Clean {
					touched_at: process.touched_at,
					kind: ItemKind::Process,
					id: tg::Either::Right(id.clone()),
				}
				.pack_to_vec();
				db.put(transaction, &key, &[])
					.map_err(|source| tg::error!(!source, "failed to put clean key"))?;
			}
		}
		Ok(())
	}
}
