use {
	crate::fdb::{Index, ItemKind, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_cache_entry(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::cache::put::Arg,
		partition_total: u64,
	) -> Result<(), fdb::FdbBindingError> {
		let id = &arg.id;

		let key = Key::Cache(crate::fdb::cache::Key::CacheEntry(id.clone()));
		let key = Self::pack(subspace, &key);
		let value = crate::cache::Entry {
			reference_count: 0,
			touched_at: arg.touched_at,
		}
		.serialize()
		.map_err(|error| fdb::FdbBindingError::CustomError(error.into()))?;
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		txn.set(&key, &value);

		for dependency in &arg.dependencies {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Cache(crate::fdb::cache::Key::CacheEntryDependency {
				cache_entry: id.clone(),
				dependency: dependency.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Cache(crate::fdb::cache::Key::DependencyCacheEntry {
				dependency: dependency.clone(),
				cache_entry: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		let id_bytes = id.to_bytes();
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		let key = crate::fdb::Key::Clean(crate::fdb::clean::Key::Clean {
			partition,
			touched_at: arg.touched_at,
			kind: ItemKind::CacheEntry,
			id: tg::object::Id::from(arg.id.clone()).into(),
		});
		let key = Self::pack(subspace, &key);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		txn.set(&key, &[]);

		Ok(())
	}

	pub(crate) fn task_put_cache_entries(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::cache::put::Arg],
		partition_total: u64,
	) -> tg::Result<()> {
		for cache_entry in args {
			Self::put_cache_entry(txn, subspace, cache_entry, partition_total)
				.map_err(|error| tg::error!(!error, "failed to put the cache entry"))?;
		}
		Ok(())
	}
}
