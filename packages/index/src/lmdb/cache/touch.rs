use {
	crate::lmdb::{Db, Index, ItemKind, Key, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
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
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::TouchCacheEntries(crate::lmdb::TouchCacheEntries {
			ids: ids.to_vec(),
			time_to_touch,
			touched_at,
		});
		self.sender_high
			.as_ref()
			.unwrap()
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::CacheEntries(cache_entries) = response else {
			return Err(tg::error!("unexpected response"));
		};
		Ok(cache_entries)
	}

	pub(crate) fn task_touch_cache_entries(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::artifact::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<crate::cache::Entry>>> {
		let mut outputs = Vec::with_capacity(ids.len());
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		for id in ids {
			let key = Key::Cache(crate::lmdb::cache::Key::CacheEntry(id.clone()));
			let key = Self::pack(subspace, &key);
			let existing = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, %id, "failed to get the cache entry"))?;
			let existing = existing.map(crate::cache::Entry::deserialize).transpose()?;
			let Some(mut cache_entry) = existing else {
				outputs.push(None);
				continue;
			};
			if touched_at - cache_entry.touched_at < time_to_touch {
				outputs.push(Some(cache_entry));
				continue;
			}
			cache_entry.touched_at = cache_entry.touched_at.max(touched_at);
			let value = cache_entry.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|error| tg::error!(!error, %id, "failed to put the cache entry"))?;
			if cache_entry.reference_count == 0 {
				let key = crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Clean {
					touched_at: cache_entry.touched_at,
					kind: ItemKind::CacheEntry,
					id: tg::object::Id::from(id.clone()).into(),
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put the clean key"))?;
			}
			outputs.push(Some(cache_entry));
		}
		Ok(outputs)
	}
}
