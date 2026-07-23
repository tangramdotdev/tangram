use {
	crate::lmdb::{Db, Index, ItemKind, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_object(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::object::put::Arg,
	) -> tg::Result<()> {
		let id = &arg.id;
		let key = Key::Object(crate::lmdb::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);

		let merge = !arg.complete();
		let existing = if merge {
			db.get(transaction, &key)
				.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
				.and_then(|bytes| crate::object::Object::deserialize(bytes).ok())
		} else {
			None
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
		.serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, %id, "failed to put the object"))?;

		for child in arg.children.iter().filter(|_| changed) {
			let key = Key::Object(crate::lmdb::object::Key::ObjectChild {
				object: id.clone(),
				child: child.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the object child"))?;

			let key = Key::Object(crate::lmdb::object::Key::ChildObject {
				child: child.clone(),
				object: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the child object"))?;
		}

		if changed && let Some(cache_entry) = &arg.cache_entry {
			let key = Key::Object(crate::lmdb::object::Key::ObjectCacheEntry {
				object: id.clone(),
				cache_entry: cache_entry.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the object cache entry"))?;

			let key = Key::Object(crate::lmdb::object::Key::CacheEntryObject {
				cache_entry: cache_entry.clone(),
				object: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the cache entry object"))?;
		}

		let key = crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Clean {
			touched_at,
			kind: ItemKind::Object,
			id: id.clone().into(),
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the clean key"))?;

		if changed {
			Self::enqueue_update(
				db,
				subspace,
				transaction,
				tg::Either::Left(id.clone()),
				crate::lmdb::update::Source::Put,
				None,
			)?;
		}

		Ok(())
	}

	pub(crate) fn put_objects_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[crate::object::put::Arg],
	) -> tg::Result<()> {
		for object in args {
			Self::put_object(db, subspace, transaction, object)?;
		}
		Ok(())
	}
}
