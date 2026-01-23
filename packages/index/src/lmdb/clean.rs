use {
	super::{Db, Index, ItemKind, Key, Kind, Request, Response},
	crate::{CacheEntry, CleanOutput, Object, Process},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

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

		let prefix = (Kind::Clean.to_i32().unwrap(),).pack_to_vec();

		let mut items: Vec<(i64, ItemKind, tg::Id)> = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|source| tg::error!(!source, "failed to iterate clean keys"))?;
		for result in iter {
			if items.len() >= batch_size {
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
			items.push((touched_at, kind, id));
		}

		for (touched_at, kind, id) in items {
			let reference_count = match kind {
				ItemKind::CacheEntry => {
					let artifact_id = tg::artifact::Id::try_from(id.clone())
						.map_err(|source| tg::error!(!source, "invalid artifact id"))?;
					Self::compute_cache_entry_reference_count(db, transaction, &artifact_id)?
				},
				ItemKind::Object => {
					let object_id = tg::object::Id::try_from(id.clone())
						.map_err(|source| tg::error!(!source, "invalid object id"))?;
					Self::compute_object_reference_count(db, transaction, &object_id)?
				},
				ItemKind::Process => {
					let process_id = tg::process::Id::try_from(id.clone())
						.map_err(|source| tg::error!(!source, "invalid process id"))?;
					Self::compute_process_reference_count(db, transaction, &process_id)?
				},
			};

			if reference_count > 0 {
				Self::set_reference_count(db, transaction, kind, &id, reference_count)?;
			} else {
				Self::delete_item(db, transaction, kind, &id, touched_at, &mut output)?;
			}

			let clean_key = Key::Clean {
				touched_at,
				kind,
				id: id.clone(),
			}
			.pack_to_vec();
			db.delete(transaction, &clean_key)
				.map_err(|source| tg::error!(!source, "failed to delete clean key"))?;
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
		kind: ItemKind,
		id: &tg::Id,
		reference_count: u64,
	) -> tg::Result<()> {
		match kind {
			ItemKind::CacheEntry => {
				let artifact_id = tg::artifact::Id::try_from(id.clone())
					.map_err(|source| tg::error!(!source, "invalid artifact id"))?;
				let key = Key::CacheEntry(artifact_id.clone()).pack_to_vec();
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
			ItemKind::Object => {
				let object_id = tg::object::Id::try_from(id.clone())
					.map_err(|source| tg::error!(!source, "invalid object id"))?;
				let key = Key::Object(object_id.clone()).pack_to_vec();
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
			ItemKind::Process => {
				let process_id = tg::process::Id::try_from(id.clone())
					.map_err(|source| tg::error!(!source, "invalid process id"))?;
				let key = Key::Process(process_id.clone()).pack_to_vec();
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

	fn delete_item(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		kind: ItemKind,
		id: &tg::Id,
		_touched_at: i64,
		output: &mut CleanOutput,
	) -> tg::Result<()> {
		match kind {
			ItemKind::CacheEntry => {
				let artifact_id = tg::artifact::Id::try_from(id.clone())
					.map_err(|source| tg::error!(!source, "invalid artifact id"))?;
				Self::delete_cache_entry(db, transaction, &artifact_id, output)?;
			},
			ItemKind::Object => {
				let object_id = tg::object::Id::try_from(id.clone())
					.map_err(|source| tg::error!(!source, "invalid object id"))?;
				Self::delete_object(db, transaction, &object_id, output)?;
			},
			ItemKind::Process => {
				let process_id = tg::process::Id::try_from(id.clone())
					.map_err(|source| tg::error!(!source, "invalid process id"))?;
				Self::delete_process(db, transaction, &process_id, output)?;
			},
		}
		Ok(())
	}

	fn delete_cache_entry(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::artifact::Id,
		output: &mut CleanOutput,
	) -> tg::Result<()> {
		let key = Key::CacheEntry(id.clone()).pack_to_vec();
		db.delete(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to delete cache entry"))?;

		let prefix = (
			Kind::CacheEntryObject.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		Self::delete_keys_with_prefix(db, transaction, &prefix)?;

		output.cache_entries.push(id.clone());

		Ok(())
	}

	fn delete_object(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::object::Id,
		output: &mut CleanOutput,
	) -> tg::Result<()> {
		let key = Key::Object(id.clone()).pack_to_vec();
		let object = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to get object"))?
			.and_then(|bytes| Object::deserialize(bytes).ok());

		db.delete(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to delete object"))?;

		let prefix = (Kind::ObjectChild.to_i32().unwrap(), id.to_bytes().as_ref()).pack_to_vec();
		let children = Self::collect_child_ids_and_delete(db, transaction, &prefix)?;
		for child_id in children {
			Self::decrement_object_reference_count(db, transaction, &child_id)?;
		}

		let prefix = (Kind::ChildObject.to_i32().unwrap(), id.to_bytes().as_ref()).pack_to_vec();
		Self::delete_keys_with_prefix(db, transaction, &prefix)?;

		if let Some(ref obj) = object
			&& let Some(cache_entry) = &obj.cache_entry
		{
			let key = Key::ObjectCacheEntry {
				object: id.clone(),
				cache_entry: cache_entry.clone(),
			}
			.pack_to_vec();
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete object cache entry"))?;
		}

		if let Some(ref obj) = object
			&& let Some(cache_entry) = &obj.cache_entry
		{
			let key = Key::CacheEntryObject {
				cache_entry: cache_entry.clone(),
				object: id.clone(),
			}
			.pack_to_vec();
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete cache entry object"))?;
		}

		let prefix = (
			Kind::ObjectProcess.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		Self::delete_keys_with_prefix(db, transaction, &prefix)?;

		output.objects.push(id.clone());

		Ok(())
	}

	fn delete_process(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::process::Id,
		output: &mut CleanOutput,
	) -> tg::Result<()> {
		let key = Key::Process(id.clone()).pack_to_vec();
		db.delete(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to delete process"))?;

		let prefix = (Kind::ProcessChild.to_i32().unwrap(), id.to_bytes().as_ref()).pack_to_vec();
		let children = Self::collect_process_child_ids_and_delete(db, transaction, &prefix)?;
		for child_id in children {
			Self::decrement_process_reference_count(db, transaction, &child_id)?;
		}

		let prefix = (Kind::ChildProcess.to_i32().unwrap(), id.to_bytes().as_ref()).pack_to_vec();
		Self::delete_keys_with_prefix(db, transaction, &prefix)?;

		let prefix = (
			Kind::ProcessObject.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		)
			.pack_to_vec();
		let objects = Self::collect_process_object_ids_and_delete(db, transaction, &prefix)?;
		for object_id in objects {
			Self::decrement_object_reference_count(db, transaction, &object_id)?;
		}

		output.processes.push(id.clone());

		Ok(())
	}

	fn collect_child_ids_and_delete(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		prefix: &[u8],
	) -> tg::Result<Vec<tg::object::Id>> {
		let entries: Vec<(Vec<u8>, tg::object::Id)> = {
			let iter = db
				.prefix_iter(transaction, prefix)
				.map_err(|source| tg::error!(!source, "failed to iterate object child keys"))?;
			iter.filter_map(|result| {
				result.ok().and_then(|(key, _)| {
					let (_, _, child_bytes): (i32, Vec<u8>, Vec<u8>) =
						foundationdb_tuple::unpack(key).ok()?;
					let child_id = tg::object::Id::from_slice(&child_bytes).ok()?;
					Some((key.to_vec(), child_id))
				})
			})
			.collect()
		};

		let mut child_ids = Vec::new();
		for (key, child_id) in entries {
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete object child key"))?;
			child_ids.push(child_id);
		}

		Ok(child_ids)
	}

	fn collect_process_child_ids_and_delete(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		prefix: &[u8],
	) -> tg::Result<Vec<tg::process::Id>> {
		let entries: Vec<(Vec<u8>, tg::process::Id)> = {
			let iter = db
				.prefix_iter(transaction, prefix)
				.map_err(|source| tg::error!(!source, "failed to iterate process child keys"))?;
			iter.filter_map(|result| {
				result.ok().and_then(|(key, _)| {
					let (_, _, child_bytes): (i32, Vec<u8>, Vec<u8>) =
						foundationdb_tuple::unpack(key).ok()?;
					let child_id = tg::process::Id::from_slice(&child_bytes).ok()?;
					Some((key.to_vec(), child_id))
				})
			})
			.collect()
		};

		let mut child_ids = Vec::new();
		for (key, child_id) in entries {
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete process child key"))?;
			child_ids.push(child_id);
		}

		Ok(child_ids)
	}

	fn collect_process_object_ids_and_delete(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		prefix: &[u8],
	) -> tg::Result<Vec<tg::object::Id>> {
		let entries: Vec<(Vec<u8>, tg::object::Id)> = {
			let iter = db
				.prefix_iter(transaction, prefix)
				.map_err(|source| tg::error!(!source, "failed to iterate process object keys"))?;
			iter.filter_map(|result| {
				result.ok().and_then(|(key, _)| {
					let (_, _, object_bytes, _): (i32, Vec<u8>, Vec<u8>, i32) =
						foundationdb_tuple::unpack(key).ok()?;
					let object_id = tg::object::Id::from_slice(&object_bytes).ok()?;
					Some((key.to_vec(), object_id))
				})
			})
			.collect()
		};

		let mut object_ids = Vec::new();
		for (key, object_id) in entries {
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete process object key"))?;
			object_ids.push(object_id);
		}

		Ok(object_ids)
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
			} else if reference_count == 1 {
				object.reference_count = 0;
				let key = Key::Clean {
					touched_at: object.touched_at,
					kind: ItemKind::Object,
					id: tg::Id::from(id.clone()),
				}
				.pack_to_vec();
				db.put(transaction, &key, &[])
					.map_err(|source| tg::error!(!source, "failed to put clean key"))?;
				let bytes = object.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|source| tg::error!(!source, "failed to put object"))?;
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
			} else if reference_count == 1 {
				process.reference_count = 0;
				let clean_key = Key::Clean {
					touched_at: process.touched_at,
					kind: ItemKind::Process,
					id: tg::Id::from(id.clone()),
				}
				.pack_to_vec();
				db.put(transaction, &clean_key, &[])
					.map_err(|source| tg::error!(!source, "failed to put clean key"))?;
				let bytes = process.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|source| tg::error!(!source, "failed to put process"))?;
			}
		}
		Ok(())
	}

	fn delete_keys_with_prefix(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		prefix: &[u8],
	) -> tg::Result<()> {
		let keys: Vec<Vec<u8>> = {
			let iter = db
				.prefix_iter(transaction, prefix)
				.map_err(|source| tg::error!(!source, "failed to iterate keys"))?;
			iter.filter_map(|result| result.ok().map(|(k, _)| k.to_vec()))
				.collect()
		};
		for key in keys {
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete key"))?;
		}
		Ok(())
	}
}
