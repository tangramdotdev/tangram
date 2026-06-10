use {
	crate::fdb::{Index, Key},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
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

		let txn = self
			.database
			.create_trx()
			.map_err(|error| tg::error!(!error, "failed to create the transaction"))?;

		let outputs = futures::future::try_join_all(
			ids.iter()
				.map(|id| Self::try_get_cache_entry_with_transaction(&txn, &self.subspace, id)),
		)
		.await?;

		Ok(outputs)
	}

	pub(crate) async fn try_get_cache_entry_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::artifact::Id,
	) -> tg::Result<Option<crate::cache::Entry>> {
		let key = Key::Cache(crate::fdb::cache::Key::CacheEntry(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the cache entry"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(crate::cache::Entry::deserialize(&bytes)?))
	}
}
