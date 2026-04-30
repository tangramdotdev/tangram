use {
	super::{Index, ItemKind, Key, Request, Response},
	crate::{CacheEntry, Object, Process},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::future,
	std::time::Duration,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn touch_cache_entries(
		&self,
		ids: &[tg::artifact::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<CacheEntry>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::TouchCacheEntries {
			ids: ids.to_vec(),
			time_to_touch,
			touched_at,
		};
		self.sender_high
			.send((request, sender))
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::CacheEntries(cache_entries) = response else {
			return Err(tg::error!("unexpected response"));
		};
		Ok(cache_entries)
	}

	pub async fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<Object>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::TouchObjects {
			ids: ids.to_vec(),
			time_to_touch,
			touched_at,
		};
		self.sender_high
			.send((request, sender))
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::Objects(objects) = response else {
			return Err(tg::error!("unexpected response"));
		};
		Ok(objects)
	}

	pub async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<Process>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::TouchProcesses {
			ids: ids.to_vec(),
			time_to_touch,
			touched_at,
		};
		self.sender_high
			.send((request, sender))
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::Processes(processes) = response else {
			return Err(tg::error!("unexpected response"));
		};
		Ok(processes)
	}

	pub(super) async fn task_touch_cache_entries(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::artifact::Id],
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<Vec<Option<CacheEntry>>> {
		future::try_join_all(ids.iter().map(|id| {
			let subspace = subspace.clone();
			async move {
				Self::touch_cache_entry_with_transaction(
					txn,
					&subspace,
					id,
					touched_at,
					time_to_touch,
					partition_total,
				)
				.await
			}
		}))
		.await
	}

	pub(super) async fn task_touch_objects(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<Vec<Option<Object>>> {
		future::try_join_all(ids.iter().map(|id| {
			let subspace = subspace.clone();
			async move {
				Self::touch_object_with_transaction(
					txn,
					&subspace,
					id,
					touched_at,
					time_to_touch,
					partition_total,
				)
				.await
			}
		}))
		.await
	}

	pub(super) async fn task_touch_processes(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<Vec<Option<Process>>> {
		future::try_join_all(ids.iter().map(|id| {
			let subspace = subspace.clone();
			async move {
				Self::touch_process_with_transaction(
					txn,
					&subspace,
					id,
					touched_at,
					time_to_touch,
					partition_total,
				)
				.await
			}
		}))
		.await
	}

	async fn touch_cache_entry_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::artifact::Id,
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<Option<CacheEntry>> {
		let key = Key::CacheEntry(id.clone());
		let key = Self::pack(subspace, &key);
		let existing = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the cache entry"))?;
		let existing = existing
			.as_ref()
			.map(|bytes| CacheEntry::deserialize(bytes))
			.transpose()?;
		let Some(mut cache_entry) = existing else {
			return Ok(None);
		};
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if touched_at - cache_entry.touched_at < time_to_touch {
			return Ok(Some(cache_entry));
		}

		let mut key_end = key.clone();
		key_end.push(0x00);
		txn.add_conflict_range(&key, &key_end, fdb::options::ConflictRangeType::Read)
			.map_err(|source| tg::error!(!source, "failed to add read conflict range"))?;

		cache_entry.touched_at = cache_entry.touched_at.max(touched_at);
		let value = cache_entry
			.serialize()
			.map_err(|source| tg::error!(!source, "failed to serialize the cache entry"))?;
		txn.set(&key, &value);
		if cache_entry.reference_count == 0 {
			let id_bytes = id.to_bytes();
			let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
			let key = Key::Clean {
				partition,
				touched_at: cache_entry.touched_at,
				kind: ItemKind::CacheEntry,
				id: tg::Either::Left(id.clone().into()),
			};
			let key = Self::pack(subspace, &key);
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			txn.set(&key, &[]);
		}

		Ok(Some(cache_entry))
	}

	async fn touch_object_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<Option<Object>> {
		let key = Key::Object(id.clone());
		let key = Self::pack(subspace, &key);
		let existing = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?;
		let existing = existing
			.as_ref()
			.map(|bytes| Object::deserialize(bytes))
			.transpose()?;
		let Some(mut object) = existing else {
			return Ok(None);
		};
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if touched_at - object.touched_at < time_to_touch {
			return Ok(Some(object));
		}

		let mut key_end = key.clone();
		key_end.push(0x00);
		txn.add_conflict_range(&key, &key_end, fdb::options::ConflictRangeType::Read)
			.map_err(|source| tg::error!(!source, "failed to add read conflict range"))?;

		object.touched_at = object.touched_at.max(touched_at);
		let value = object
			.serialize()
			.map_err(|source| tg::error!(!source, "failed to serialize the object"))?;
		txn.set(&key, &value);
		if object.reference_count == 0 {
			let id_bytes = id.to_bytes();
			let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
			let key = Key::Clean {
				partition,
				touched_at: object.touched_at,
				kind: ItemKind::Object,
				id: tg::Either::Left(id.clone()),
			};
			let key = Self::pack(subspace, &key);
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			txn.set(&key, &[]);
		}

		Ok(Some(object))
	}

	async fn touch_process_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<Option<Process>> {
		let key = Key::Process(id.clone());
		let key = Self::pack(subspace, &key);
		let existing = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?;
		let existing = existing
			.as_ref()
			.map(|bytes| Process::deserialize(bytes))
			.transpose()?;
		let Some(mut process) = existing else {
			return Ok(None);
		};
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if touched_at - process.touched_at < time_to_touch {
			return Ok(Some(process));
		}

		let mut key_end = key.clone();
		key_end.push(0x00);
		txn.add_conflict_range(&key, &key_end, fdb::options::ConflictRangeType::Read)
			.map_err(|source| tg::error!(!source, "failed to add read conflict range"))?;

		process.touched_at = process.touched_at.max(touched_at);
		let value = process
			.serialize()
			.map_err(|source| tg::error!(!source, "failed to serialize the process"))?;
		txn.set(&key, &value);
		if process.reference_count == 0 {
			let id_bytes = id.to_bytes();
			let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
			let key = Key::Clean {
				partition,
				touched_at: process.touched_at,
				kind: ItemKind::Process,
				id: tg::Either::Right(id.clone()),
			};
			let key = Self::pack(subspace, &key);
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			txn.set(&key, &[]);
		}

		Ok(Some(process))
	}
}
