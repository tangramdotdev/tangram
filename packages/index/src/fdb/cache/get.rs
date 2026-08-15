use {
	crate::fdb::{Index, Key},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	std::ops::ControlFlow,
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

	pub(crate) async fn try_get_cache_entries_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::artifact::Id],
	) -> tg::Result<ControlFlow<Vec<Option<crate::cache::Entry>>, fdb::FdbError>> {
		let entries = {
			let result = futures::future::try_join_all(
				ids.iter()
					.map(|id| Self::try_get_cache_entry_with_transaction(txn, subspace, id)),
			)
			.await;
			let results = result?;
			let mut values = Vec::with_capacity(results.len());
			for result in results {
				let value = match result {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				values.push(value);
			}
			values
		};

		Ok(ControlFlow::Break(entries))
	}

	pub(crate) async fn try_get_cache_entry_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::artifact::Id,
	) -> tg::Result<ControlFlow<Option<crate::cache::Entry>, fdb::FdbError>> {
		let key = Key::Cache(crate::fdb::cache::Key::CacheEntry(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::fdb::retry!(result);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(None));
		};
		let entry = Some(crate::cache::Entry::deserialize(&bytes)?);

		Ok(ControlFlow::Break(entry))
	}
}
