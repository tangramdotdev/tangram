use {
	crate::lmdb::{Db, Index, ItemKind, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_cache_entry(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::cache::put::Arg,
	) -> tg::Result<()> {
		let key = Key::Cache(crate::lmdb::cache::Key::CacheEntry(arg.id.clone()));
		let key = Self::pack(subspace, &key);

		let existing = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the cache entry"))?
			.and_then(|bytes| crate::cache::Entry::deserialize(bytes).ok());

		let touched_at = existing.as_ref().map_or(arg.touched_at, |existing| {
			existing.touched_at.max(arg.touched_at)
		});

		let value = crate::cache::Entry {
			reference_count: 0,
			touched_at,
		}
		.serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to put the cache entry"))?;

		for dependency in &arg.dependencies {
			let key = Key::Cache(crate::lmdb::cache::Key::CacheEntryDependency {
				cache_entry: arg.id.clone(),
				dependency: dependency.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the cache entry dependency"))?;

			let key = Key::Cache(crate::lmdb::cache::Key::DependencyCacheEntry {
				dependency: dependency.clone(),
				cache_entry: arg.id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the dependency cache entry"))?;
		}

		let key = crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Clean {
			touched_at,
			kind: ItemKind::CacheEntry,
			id: tg::object::Id::from(arg.id.clone()).into(),
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the clean key"))?;

		Ok(())
	}

	pub(crate) fn put_cache_entries_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[crate::cache::put::Arg],
	) -> tg::Result<()> {
		for cache_entry in args {
			Self::put_cache_entry(db, subspace, transaction, cache_entry)?;
		}
		Ok(())
	}
}
