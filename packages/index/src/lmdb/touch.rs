use {
	super::{Db, Index, Key, Request, Response},
	crate::{CacheEntry, Object, Process},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn touch_cache_entries(
		&self,
		ids: &[tg::artifact::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<CacheEntry>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::TouchCacheEntries {
			ids: ids.to_vec(),
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
	) -> tg::Result<Vec<Option<Object>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::TouchObjects {
			ids: ids.to_vec(),
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
	) -> tg::Result<Vec<Option<Process>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::TouchProcesses {
			ids: ids.to_vec(),
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

	pub(super) fn task_touch_cache_entries(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::artifact::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<CacheEntry>>> {
		let mut outputs = Vec::with_capacity(ids.len());
		for id in ids {
			let key = Key::CacheEntry(id.clone());
			let key = Self::pack(subspace, &key);
			let existing = db
				.get(transaction, &key)
				.map_err(|source| tg::error!(!source, %id, "failed to get the cache entry"))?;
			let existing = existing.map(CacheEntry::deserialize).transpose()?;
			let Some(mut cache_entry) = existing else {
				outputs.push(None);
				continue;
			};
			cache_entry.touched_at = cache_entry.touched_at.max(touched_at);
			let value = cache_entry.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|source| tg::error!(!source, %id, "failed to put the cache entry"))?;
			outputs.push(Some(cache_entry));
		}
		Ok(outputs)
	}

	pub(super) fn task_touch_objects(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<Object>>> {
		let mut outputs = Vec::with_capacity(ids.len());
		for id in ids {
			let key = Key::Object(id.clone());
			let key = Self::pack(subspace, &key);
			let existing = db
				.get(transaction, &key)
				.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?;
			let existing = existing.map(Object::deserialize).transpose()?;
			let Some(mut object) = existing else {
				outputs.push(None);
				continue;
			};
			object.touched_at = object.touched_at.max(touched_at);
			let value = object.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|source| tg::error!(!source, %id, "failed to put the object"))?;
			outputs.push(Some(object));
		}
		Ok(outputs)
	}

	pub(super) fn task_touch_processes(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<Process>>> {
		let mut outputs = Vec::with_capacity(ids.len());
		for id in ids {
			let key = Key::Process(id.clone());
			let key = Self::pack(subspace, &key);
			let existing = db
				.get(transaction, &key)
				.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?;
			let existing = existing.map(Process::deserialize).transpose()?;
			let Some(mut process) = existing else {
				outputs.push(None);
				continue;
			};
			process.touched_at = process.touched_at.max(touched_at);
			let value = process.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|source| tg::error!(!source, %id, "failed to put the process"))?;

			outputs.push(Some(process));
		}
		Ok(outputs)
	}
}
