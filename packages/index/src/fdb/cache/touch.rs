use {
	crate::fdb::{Index, ItemKind, Key, Request, Response},
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
	) -> tg::Result<Vec<Option<crate::cache::Entry>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = Request::TouchCacheEntries(crate::fdb::TouchCacheEntries {
			ids: ids.to_vec(),
			time_to_touch,
			touched_at,
		});
		let response = self.send_write_request(request).await?;
		let Response::CacheEntries(cache_entries) = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(cache_entries)
	}

	pub(crate) async fn touch_cache_entries_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::artifact::Id],
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<Vec<Option<crate::cache::Entry>>> {
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

	async fn touch_cache_entry_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::artifact::Id,
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<Option<crate::cache::Entry>> {
		let key = Key::Cache(crate::fdb::cache::Key::CacheEntry(id.clone()));
		let key = Self::pack(subspace, &key);
		let existing = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the cache entry"))?;
		let existing = existing
			.as_ref()
			.map(|bytes| crate::cache::Entry::deserialize(bytes))
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
			.map_err(|error| tg::error!(!error, "failed to add read conflict range"))?;

		cache_entry.touched_at = cache_entry.touched_at.max(touched_at);
		let value = cache_entry
			.serialize()
			.map_err(|error| tg::error!(!error, "failed to serialize the cache entry"))?;
		txn.set(&key, &value);
		if cache_entry.reference_count == 0 {
			let id_bytes = id.to_bytes();
			let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
			let key = crate::fdb::Key::Clean(crate::fdb::clean::Key::Clean {
				partition,
				touched_at: cache_entry.touched_at,
				kind: ItemKind::CacheEntry,
				id: tg::object::Id::from(id.clone()).into(),
			});
			let key = Self::pack(subspace, &key);
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			txn.set(&key, &[]);
		}

		Ok(Some(cache_entry))
	}
}
