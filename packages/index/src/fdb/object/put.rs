use {
	crate::fdb::{Index, ItemKind, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_object(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::object::put::Arg,
		partition_total: u64,
	) -> Result<(), fdb::FdbBindingError> {
		let id = &arg.id;
		let key = Key::Object(crate::fdb::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);

		let existing = if arg.complete() {
			None
		} else {
			txn.get(&key, false)
				.await?
				.and_then(|bytes| crate::object::Object::deserialize(&bytes).ok())
		};

		let time_to_touch = i64::try_from(arg.time_to_touch.as_secs()).unwrap();
		let touch = existing.as_ref().is_none_or(|existing| {
			arg.touched_at.saturating_sub(existing.touched_at) >= time_to_touch
		});
		let touched_at = existing.as_ref().map_or(arg.touched_at, |existing| {
			if touch {
				existing.touched_at.max(arg.touched_at)
			} else {
				existing.touched_at
			}
		});

		let cache_entry = arg.cache_entry.clone().or_else(|| {
			existing
				.as_ref()
				.and_then(|existing| existing.cache_entry.clone())
		});

		let stored = crate::object::Stored {
			subtree: arg.stored.subtree
				|| existing
					.as_ref()
					.is_some_and(|existing| existing.stored.subtree),
		};

		let mut metadata = arg.metadata.clone();
		if let Some(ref existing) = existing {
			metadata.merge(&existing.metadata);
		}
		let changed = existing.as_ref().is_none_or(|existing| {
			existing.cache_entry != cache_entry
				|| existing.metadata != metadata
				|| existing.stored != stored
		});
		if !changed && !touch {
			return Ok(());
		}

		let value = crate::object::Object {
			cache_entry,
			metadata,
			reference_count: 0,
			stored,
			touched_at,
		}
		.serialize()
		.map_err(|error| fdb::FdbBindingError::CustomError(error.into()))?;

		if existing.is_none() {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
		}
		txn.set(&key, &value);

		for child in arg.children.iter().filter(|_| changed) {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Object(crate::fdb::object::Key::ObjectChild {
				object: id.clone(),
				child: child.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Object(crate::fdb::object::Key::ChildObject {
				child: child.clone(),
				object: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		if changed && let Some(cache_entry) = &arg.cache_entry {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Object(crate::fdb::object::Key::ObjectCacheEntry {
				object: id.clone(),
				cache_entry: cache_entry.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Object(crate::fdb::object::Key::CacheEntryObject {
				cache_entry: cache_entry.clone(),
				object: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		let id_bytes = id.to_bytes();
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = crate::fdb::Key::Clean(crate::fdb::clean::Key::Clean {
			partition,
			touched_at,
			kind: ItemKind::Object,
			id: id.clone().into(),
		});
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);

		if changed {
			Self::enqueue_update(
				txn,
				subspace,
				&tg::Either::Left(id.clone()),
				partition_total,
			);
		}

		Ok(())
	}

	pub(crate) async fn put_objects_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::object::put::Arg],
		partition_total: u64,
	) -> tg::Result<()> {
		for object in args {
			Self::put_object(txn, subspace, object, partition_total)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the object"))?;
		}
		Ok(())
	}
}
