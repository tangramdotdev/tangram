use {
	super::{Index, Key},
	crate::{CacheEntry, Object, Process},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::future,
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
		self.database
			.run(|txn, _| {
				let subspace = self.subspace.clone();
				let ids = ids.to_vec();
				async move {
					let txn = &txn;
					future::try_join_all(ids.iter().map(|id| {
						let subspace = subspace.clone();
						async move {
							Self::touch_cache_entry_with_transaction(txn, &subspace, id, touched_at)
								.await
						}
					}))
					.await
					.map_err(|source| fdb::FdbBindingError::CustomError(source.into()))
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to touch cache entries"))
	}

	async fn touch_cache_entry_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::artifact::Id,
		touched_at: i64,
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

		let mut key_end = key.clone();
		key_end.push(0x00);
		txn.add_conflict_range(&key, &key_end, fdb::options::ConflictRangeType::Read)
			.map_err(|source| tg::error!(!source, "failed to add read conflict range"))?;

		cache_entry.touched_at = cache_entry.touched_at.max(touched_at);
		let value = cache_entry
			.serialize()
			.map_err(|source| tg::error!(!source, "failed to serialize the cache entry"))?;
		txn.set(&key, &value);

		Ok(Some(cache_entry))
	}

	pub async fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<Object>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		self.database
			.run(|txn, _| {
				let subspace = self.subspace.clone();
				let ids = ids.to_vec();
				async move {
					let txn = &txn;
					future::try_join_all(ids.iter().map(|id| {
						let subspace = subspace.clone();
						async move {
							Self::touch_object_with_transaction(txn, &subspace, id, touched_at)
								.await
						}
					}))
					.await
					.map_err(|source| fdb::FdbBindingError::CustomError(source.into()))
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to touch objects"))
	}

	pub async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<Process>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		self.database
			.run(|txn, _| {
				let subspace = self.subspace.clone();
				let ids = ids.to_vec();
				async move {
					let txn = &txn;
					future::try_join_all(ids.iter().map(|id| {
						let subspace = subspace.clone();
						async move {
							Self::touch_process_with_transaction(txn, &subspace, id, touched_at)
								.await
						}
					}))
					.await
					.map_err(|source| fdb::FdbBindingError::CustomError(source.into()))
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to touch processes"))
	}

	async fn touch_object_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		touched_at: i64,
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

		let mut key_end = key.clone();
		key_end.push(0x00);
		txn.add_conflict_range(&key, &key_end, fdb::options::ConflictRangeType::Read)
			.map_err(|source| tg::error!(!source, "failed to add read conflict range"))?;

		object.touched_at = object.touched_at.max(touched_at);
		let value = object
			.serialize()
			.map_err(|source| tg::error!(!source, "failed to serialize the object"))?;
		txn.set(&key, &value);

		Ok(Some(object))
	}

	async fn touch_process_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		touched_at: i64,
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

		let mut key_end = key.clone();
		key_end.push(0x00);
		txn.add_conflict_range(&key, &key_end, fdb::options::ConflictRangeType::Read)
			.map_err(|source| tg::error!(!source, "failed to add read conflict range"))?;

		process.touched_at = process.touched_at.max(touched_at);
		let value = process
			.serialize()
			.map_err(|source| tg::error!(!source, "failed to serialize the process"))?;
		txn.set(&key, &value);

		Ok(Some(process))
	}
}
