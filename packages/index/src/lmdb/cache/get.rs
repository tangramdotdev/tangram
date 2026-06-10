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
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let subspace = self.subspace.clone();
			let ids = ids.to_owned();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let mut outputs = Vec::with_capacity(ids.len());
				for id in &ids {
					let option = Self::try_get_cache_entry_with_transaction(
						&db,
						&subspace,
						&transaction,
						id,
					)?;
					outputs.push(option);
				}
				Ok(outputs)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
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
