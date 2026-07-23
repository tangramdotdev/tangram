use {
	crate::lmdb::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_cache_entries(
		&self,
		ids: &[tg::artifact::Id],
	) -> tg::Result<Vec<Option<crate::cache::Entry>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = crate::read::Request::TryGetCacheEntries {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetCacheEntries(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn try_get_cache_entries_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::artifact::Id],
	) -> tg::Result<Vec<Option<crate::cache::Entry>>> {
		ids.iter()
			.map(|id| Self::try_get_cache_entry_with_transaction(db, subspace, transaction, id))
			.collect()
	}

	pub(crate) fn try_get_cache_entry_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::artifact::Id,
	) -> tg::Result<Option<crate::cache::Entry>> {
		let key = Key::Cache(crate::lmdb::cache::Key::CacheEntry(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the cache entry"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(crate::cache::Entry::deserialize(bytes)?))
	}
}
