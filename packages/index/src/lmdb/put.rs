use {
	super::{Db, Index, ItemKind, Key, Request},
	crate::{
		CacheEntry, Object, ObjectStored, Process, PutArg, PutCacheEntryArg, PutObjectArg,
		PutProcessArg, PutTagArg, Tag,
	},
	foundationdb_tuple::TuplePack as _,
	heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put(&self, arg: PutArg) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Put(arg);
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	#[expect(clippy::needless_pass_by_value)]
	pub(super) fn task_put(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: PutArg,
	) -> tg::Result<()> {
		for cache_entry in &arg.cache_entries {
			Self::put_cache_entry(db, transaction, cache_entry)?;
		}
		for object in &arg.objects {
			Self::put_object(db, transaction, object)?;
		}
		for process in &arg.processes {
			Self::put_process(db, transaction, process)?;
		}
		for tag in &arg.tags {
			Self::put_tag(db, transaction, tag)?;
		}
		Ok(())
	}

	fn put_cache_entry(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &PutCacheEntryArg,
	) -> tg::Result<()> {
		let key = Key::CacheEntry(arg.id.clone()).pack_to_vec();

		let existing = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to get the cache entry"))?
			.and_then(|bytes| CacheEntry::deserialize(bytes).ok());

		let touched_at = existing
			.as_ref()
			.map_or(arg.touched_at, |e| e.touched_at.max(arg.touched_at));

		let value = CacheEntry {
			reference_count: 0,
			touched_at,
		}
		.serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|source| tg::error!(!source, "failed to put the cache entry"))?;

		let clean_key = Key::Clean {
			touched_at,
			kind: ItemKind::CacheEntry,
			id: tg::Id::from(arg.id.clone()),
		}
		.pack_to_vec();
		db.put(transaction, &clean_key, &[])
			.map_err(|source| tg::error!(!source, "failed to put the clean key"))?;

		Ok(())
	}

	fn put_object(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &PutObjectArg,
	) -> tg::Result<()> {
		let id = &arg.id;
		let key = Key::Object(id.clone()).pack_to_vec();

		let existing = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
			.and_then(|bytes| Object::deserialize(bytes).ok());

		let touched_at = existing
			.as_ref()
			.map_or(arg.touched_at, |e| e.touched_at.max(arg.touched_at));

		let cache_entry = arg
			.cache_entry
			.clone()
			.or_else(|| existing.as_ref().and_then(|e| e.cache_entry.clone()));

		let stored = ObjectStored {
			subtree: arg.stored.subtree || existing.as_ref().is_some_and(|e| e.stored.subtree),
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
			}
			.pack_to_vec();
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the object child"))?;

			let key = Key::ChildObject {
				child: child.clone(),
				object: id.clone(),
			}
			.pack_to_vec();
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the child object"))?;
		}

		if let Some(cache_entry) = &arg.cache_entry {
			let key = Key::ObjectCacheEntry {
				object: id.clone(),
				cache_entry: cache_entry.clone(),
			}
			.pack_to_vec();
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the object cache entry"))?;

			let key = Key::CacheEntryObject {
				cache_entry: cache_entry.clone(),
				object: id.clone(),
			}
			.pack_to_vec();
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the cache entry object"))?;
		}

		let clean_key = Key::Clean {
			touched_at,
			kind: ItemKind::Object,
			id: tg::Id::from(id.clone()),
		}
		.pack_to_vec();
		db.put(transaction, &clean_key, &[])
			.map_err(|source| tg::error!(!source, "failed to put the clean key"))?;

		Ok(())
	}

	fn put_process(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &PutProcessArg,
	) -> tg::Result<()> {
		let id = &arg.id;
		let key = Key::Process(id.clone()).pack_to_vec();

		let existing = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?
			.and_then(|bytes| Process::deserialize(bytes).ok());

		let touched_at = existing
			.as_ref()
			.map_or(arg.touched_at, |e| e.touched_at.max(arg.touched_at));

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
			}
			.pack_to_vec();
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the process child"))?;

			let key = Key::ChildProcess {
				child: child.clone(),
				parent: id.clone(),
			}
			.pack_to_vec();
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the child process"))?;
		}

		for (object, kind) in &arg.objects {
			let key = Key::ProcessObject {
				process: id.clone(),
				object: object.clone(),
				kind: *kind,
			}
			.pack_to_vec();
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the process object"))?;

			let key = Key::ObjectProcess {
				object: object.clone(),
				process: id.clone(),
				kind: *kind,
			}
			.pack_to_vec();
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the object process"))?;
		}

		let clean_key = Key::Clean {
			touched_at,
			kind: ItemKind::Process,
			id: tg::Id::from(id.clone()),
		}
		.pack_to_vec();
		db.put(transaction, &clean_key, &[])
			.map_err(|source| tg::error!(!source, "failed to put the clean key"))?;

		Ok(())
	}

	fn put_tag(db: &Db, transaction: &mut lmdb::RwTxn<'_>, arg: &PutTagArg) -> tg::Result<()> {
		let key = Key::Tag(arg.tag.clone()).pack_to_vec();
		let value = Tag {
			item: arg.item.clone(),
		}
		.serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|source| tg::error!(!source, "failed to put the tag"))?;

		let item_bytes: Vec<u8> = match &arg.item {
			tg::Either::Left(object_id) => object_id.to_bytes().to_vec(),
			tg::Either::Right(process_id) => process_id.to_bytes().to_vec(),
		};
		let key = Key::ItemTag {
			item: item_bytes,
			tag: arg.tag.clone(),
		}
		.pack_to_vec();
		db.put(transaction, &key, &[])
			.map_err(|source| tg::error!(!source, "failed to put the item tag"))?;

		Ok(())
	}
}
