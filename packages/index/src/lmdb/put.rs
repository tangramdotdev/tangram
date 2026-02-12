use {
	super::{Db, Index, ItemKind, Key, Request},
	crate::{
		CacheEntry, Object, ObjectStored, Process, PutArg, PutCacheEntryArg, PutObjectArg,
		PutProcessArg,
	},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put(&self, arg: PutArg) -> tg::Result<()> {
		if arg.cache_entries.is_empty() && arg.objects.is_empty() && arg.processes.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Put(arg);
		self.sender_medium
			.send((request, sender))
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub(super) fn task_put(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: PutArg,
	) -> tg::Result<()> {
		for cache_entry in arg.cache_entries {
			Self::put_cache_entry(db, subspace, transaction, &cache_entry)?;
		}
		for object in arg.objects {
			Self::put_object(db, subspace, transaction, &object)?;
		}
		for process in arg.processes {
			Self::put_process(db, subspace, transaction, &process)?;
		}
		Ok(())
	}

	fn put_cache_entry(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &PutCacheEntryArg,
	) -> tg::Result<()> {
		let key = Key::CacheEntry(arg.id.clone());
		let key = Self::pack(subspace, &key);

		let existing = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to get the cache entry"))?
			.and_then(|bytes| CacheEntry::deserialize(bytes).ok());

		let touched_at = existing.as_ref().map_or(arg.touched_at, |existing| {
			existing.touched_at.max(arg.touched_at)
		});

		let value = CacheEntry {
			reference_count: 0,
			touched_at,
		}
		.serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|source| tg::error!(!source, "failed to put the cache entry"))?;

		for dependency in &arg.dependencies {
			let key = Key::CacheEntryDependency {
				cache_entry: arg.id.clone(),
				dependency: dependency.clone(),
			};
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[]).map_err(|source| {
				tg::error!(!source, "failed to put the cache entry dependency")
			})?;

			let key = Key::DependencyCacheEntry {
				dependency: dependency.clone(),
				cache_entry: arg.id.clone(),
			};
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[]).map_err(|source| {
				tg::error!(!source, "failed to put the dependency cache entry")
			})?;
		}

		let key = Key::Clean {
			touched_at,
			kind: ItemKind::CacheEntry,
			id: tg::Either::Left(arg.id.clone().into()),
		};
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|source| tg::error!(!source, "failed to put the clean key"))?;

		Ok(())
	}

	fn put_object(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &PutObjectArg,
	) -> tg::Result<()> {
		let id = &arg.id;
		let key = Key::Object(id.clone());
		let key = Self::pack(subspace, &key);

		let merge = !arg.complete();
		let existing = if merge {
			db.get(transaction, &key)
				.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
				.and_then(|bytes| Object::deserialize(bytes).ok())
		} else {
			None
		};

		let touched_at = existing.as_ref().map_or(arg.touched_at, |existing| {
			existing.touched_at.max(arg.touched_at)
		});

		let cache_entry = arg.cache_entry.clone().or_else(|| {
			existing
				.as_ref()
				.and_then(|existing| existing.cache_entry.clone())
		});

		let stored = ObjectStored {
			subtree: arg.stored.subtree
				|| existing
					.as_ref()
					.is_some_and(|existing| existing.stored.subtree),
		};

		let mut metadata = arg.metadata.clone();
		if let Some(ref existing) = existing {
			metadata.merge(&existing.metadata);
		}

		let value = Object {
			cache_entry,
			metadata,
			reference_count: 0,
			stored,
			touched_at,
		}
		.serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|source| tg::error!(!source, %id, "failed to put the object"))?;

		for child in &arg.children {
			let key = Key::ObjectChild {
				object: id.clone(),
				child: child.clone(),
			};
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the object child"))?;

			let key = Key::ChildObject {
				child: child.clone(),
				object: id.clone(),
			};
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the child object"))?;
		}

		if let Some(cache_entry) = &arg.cache_entry {
			let key = Key::ObjectCacheEntry {
				object: id.clone(),
				cache_entry: cache_entry.clone(),
			};
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the object cache entry"))?;

			let key = Key::CacheEntryObject {
				cache_entry: cache_entry.clone(),
				object: id.clone(),
			};
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the cache entry object"))?;
		}

		let key = Key::Clean {
			touched_at,
			kind: ItemKind::Object,
			id: tg::Either::Left(id.clone()),
		};
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|source| tg::error!(!source, "failed to put the clean key"))?;

		Self::enqueue_update(
			db,
			subspace,
			transaction,
			tg::Either::Left(id.clone()),
			super::Update::Put,
			None,
		)?;

		Ok(())
	}

	fn put_process(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &PutProcessArg,
	) -> tg::Result<()> {
		let id = &arg.id;
		let key = Key::Process(id.clone());
		let key = Self::pack(subspace, &key);

		let merge = !arg.complete();
		let existing = if merge {
			db.get(transaction, &key)
				.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?
				.and_then(|bytes| Process::deserialize(bytes).ok())
		} else {
			None
		};

		let touched_at = existing.as_ref().map_or(arg.touched_at, |existing| {
			existing.touched_at.max(arg.touched_at)
		});

		let mut stored = arg.stored.clone();
		if let Some(ref existing) = existing {
			stored.merge(&existing.stored);
		}

		let mut metadata = arg.metadata.clone();
		if let Some(ref existing) = existing {
			metadata.merge(&existing.metadata);
		}

		let value = Process {
			metadata,
			reference_count: 0,
			stored,
			touched_at,
		}
		.serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|source| tg::error!(!source, %id, "failed to put the process"))?;

		for child in &arg.children {
			let key = Key::ProcessChild {
				process: id.clone(),
				child: child.clone(),
			};
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the process child"))?;

			let key = Key::ChildProcess {
				child: child.clone(),
				parent: id.clone(),
			};
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the child process"))?;
		}

		for (object, kind) in &arg.objects {
			let key = Key::ProcessObject {
				process: id.clone(),
				kind: *kind,
				object: object.clone(),
			};
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the process object"))?;

			let key = Key::ObjectProcess {
				object: object.clone(),
				kind: *kind,
				process: id.clone(),
			};
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the object process"))?;
		}

		let key = Key::Clean {
			touched_at,
			kind: ItemKind::Process,
			id: tg::Either::Right(id.clone()),
		};
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|source| tg::error!(!source, "failed to put the clean key"))?;

		Self::enqueue_update(
			db,
			subspace,
			transaction,
			tg::Either::Right(id.clone()),
			super::Update::Put,
			None,
		)?;

		Ok(())
	}
}
