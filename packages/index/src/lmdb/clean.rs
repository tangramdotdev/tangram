use {
	super::{Db, Index, ItemKind, Key, Kind, Request, Response},
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
	pub async fn clean(&self, max_touched_at: i64, batch_size: usize) -> tg::Result<CleanOutput> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Clean {
			max_touched_at,
			batch_size,
		};
		self.sender
			.send((request, sender))
			.await
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

		// Collect candidates from Clean keys.
		let prefix = (Kind::Clean.to_i32().unwrap(),).pack_to_vec();
		let mut candidates: Vec<Candidate> = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to iterate clean keys"))?;
		for result in iter {
			if candidates.len() >= batch_size {
				break;
			}
			let (key_bytes, _) =
				result.map_err(|source| tg::error!(!source, "failed to read clean key"))?;
			let key: Key = fdbt::unpack(key_bytes)
				.map_err(|source| tg::error!(!source, "failed to unpack key"))?;
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
					let id = tg::artifact::Id::try_from(id)
						.map_err(|source| tg::error!(!source, "invalid artifact id"))?;
					Item::CacheEntry(id)
				},
				ItemKind::Object => {
					let id = tg::object::Id::try_from(id)
						.map_err(|source| tg::error!(!source, "invalid object id"))?;
					Item::Object(id)
				},
				ItemKind::Process => {
					let id = tg::process::Id::try_from(id)
						.map_err(|source| tg::error!(!source, "invalid process id"))?;
					Item::Process(id)
				},
			};
			candidates.push(Candidate { touched_at, item });
		}

		// Process each candidate.
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
				Item::CacheEntry(id) => (ItemKind::CacheEntry, id.clone().into()),
				Item::Object(id) => (ItemKind::Object, id.clone().into()),
				Item::Process(id) => (ItemKind::Process, id.clone().into()),
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

		Ok(output)
	}

	fn compute_cache_entry_reference_count(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::artifact::Id,
	) -> tg::Result<u64> {
		let prefix = (
			Kind::CacheEntryObject.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		Self::count_keys_with_prefix(db, transaction, &prefix)
	}

	fn compute_object_reference_count(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<u64> {
		let child_object_prefix =
			(Kind::ChildObject.to_i32().unwrap(), id.to_bytes().as_ref()).pack_to_vec();
		let child_object_count =
			Self::count_keys_with_prefix(db, transaction, &child_object_prefix)?;

		let object_process_prefix = (
			Kind::ObjectProcess.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let object_process_count =
			Self::count_keys_with_prefix(db, transaction, &object_process_prefix)?;

		Ok(child_object_count + object_process_count)
	}

	fn compute_process_reference_count(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<u64> {
		let prefix = (Kind::ChildProcess.to_i32().unwrap(), id.to_bytes().as_ref()).pack_to_vec();
		Self::count_keys_with_prefix(db, transaction, &prefix)
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
		Ok(())
	}

	fn delete_object(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<()> {
		// Get cache_entry from Object value.
		let key = Key::Object(id.clone()).pack_to_vec();
		let cache_entry = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to get object"))?
			.and_then(|bytes| Object::deserialize(bytes).ok())
			.and_then(|obj| obj.cache_entry);

		// Delete main Object key.
		db.delete(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to delete object"))?;

		// Get ObjectChild keys, extract children, delete keys.
		let prefix = (Kind::ObjectChild.to_i32().unwrap(), id.to_bytes().as_ref()).pack_to_vec();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to iterate object child keys"))?;
		let mut entries = Vec::new();
		for result in iter {
			let (key_bytes, _) =
				result.map_err(|source| tg::error!(!source, "failed to read object child key"))?;
			let key: Key = fdbt::unpack(key_bytes)
				.map_err(|source| tg::error!(!source, "failed to unpack object child key"))?;
			let Key::ObjectChild { child, .. } = key else {
				return Err(tg::error!("expected object child key"));
			};
			entries.push((key_bytes.to_vec(), child));
		}
		for (key_bytes, _) in &entries {
			db.delete(transaction, key_bytes)
				.map_err(|source| tg::error!(!source, "failed to delete object child key"))?;
		}

		// For each child: delete reverse ChildObject key, decrement child ref count.
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

		// If cache_entry exists: delete ObjectCacheEntry + CacheEntryObject, decrement cache_entry ref count.
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
		// Delete main Process key.
		let key = Key::Process(id.clone()).pack_to_vec();
		db.delete(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to delete process"))?;

		// Get ProcessChild keys, extract children, delete keys.
		let prefix = (Kind::ProcessChild.to_i32().unwrap(), id.to_bytes().as_ref()).pack_to_vec();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to iterate process child keys"))?;
		let mut entries = Vec::new();
		for result in iter {
			let (key_bytes, _) =
				result.map_err(|source| tg::error!(!source, "failed to read process child key"))?;
			let key: Key = fdbt::unpack(key_bytes)
				.map_err(|source| tg::error!(!source, "failed to unpack process child key"))?;
			let Key::ProcessChild { child, .. } = key else {
				return Err(tg::error!("expected process child key"));
			};
			entries.push((key_bytes.to_vec(), child));
		}
		for (key_bytes, _) in &entries {
			db.delete(transaction, key_bytes)
				.map_err(|source| tg::error!(!source, "failed to delete process child key"))?;
		}

		// For each child: delete reverse ChildProcess key, decrement child process ref count.
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

		// Get ProcessObject keys, extract (object, kind) pairs, delete keys.
		let prefix = (
			Kind::ProcessObject.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to iterate process object keys"))?;
		let mut object_entries: Vec<(Vec<u8>, tg::object::Id, ProcessObjectKind)> = Vec::new();
		for result in iter {
			let (key_bytes, _) = result
				.map_err(|source| tg::error!(!source, "failed to read process object key"))?;
			let key: Key = fdbt::unpack(key_bytes)
				.map_err(|source| tg::error!(!source, "failed to unpack process object key"))?;
			let Key::ProcessObject { object, kind, .. } = key else {
				return Err(tg::error!("expected process object key"));
			};
			object_entries.push((key_bytes.to_vec(), object, kind));
		}
		for (key_bytes, _, _) in &object_entries {
			db.delete(transaction, key_bytes)
				.map_err(|source| tg::error!(!source, "failed to delete process object key"))?;
		}

		// For each object: delete reverse ObjectProcess key, decrement object ref count.
		for (_, object, kind) in &object_entries {
			let key = Key::ObjectProcess {
				object: object.clone(),
				process: id.clone(),
				kind: *kind,
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

				let clean_key = Key::Clean {
					touched_at: entry.touched_at,
					kind: ItemKind::CacheEntry,
					id: tg::Id::from(id.clone()),
				}
				.pack_to_vec();
				db.put(transaction, &clean_key, &[])
					.map_err(|source| tg::error!(!source, "failed to put clean key"))?;
			}
		}
		Ok(())
	}

	fn decrement_object_reference_count(
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

				let clean_key = Key::Clean {
					touched_at: object.touched_at,
					kind: ItemKind::Object,
					id: tg::Id::from(id.clone()),
				}
				.pack_to_vec();
				db.put(transaction, &clean_key, &[])
					.map_err(|source| tg::error!(!source, "failed to put clean key"))?;
			}
		}
		Ok(())
	}

	fn decrement_process_reference_count(
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

				let clean_key = Key::Clean {
					touched_at: process.touched_at,
					kind: ItemKind::Process,
					id: tg::Id::from(id.clone()),
				}
				.pack_to_vec();
				db.put(transaction, &clean_key, &[])
					.map_err(|source| tg::error!(!source, "failed to put clean key"))?;
			}
		}
		Ok(())
	}
}
