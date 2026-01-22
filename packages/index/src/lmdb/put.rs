use {
	super::{Db, Index, Key, Request},
	crate::{CacheEntry, PutArg, PutCacheEntryArg, PutObjectArg, PutProcessArg, PutTagArg, Tag},
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
		let key = Key::CacheEntry(&arg.id).pack_to_vec();

		// Read existing value if any.
		let existing = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to get the cache entry"))?
			.and_then(|bytes| tangram_serialize::from_slice::<CacheEntry>(bytes).ok());

		// Merge touched_at (use max).
		let touched_at = existing
			.as_ref()
			.map_or(arg.touched_at, |e| e.touched_at.max(arg.touched_at));

		// Keep reference count from existing entry.
		let reference_count = existing.map_or(0, |e| e.reference_count);

		let value = CacheEntry {
			reference_count,
			touched_at,
		};
		let value_bytes = tangram_serialize::to_vec(&value)
			.map_err(|source| tg::error!(!source, "failed to serialize the cache entry"))?;
		db.put(transaction, &key, &value_bytes)
			.map_err(|source| tg::error!(!source, "failed to put the cache entry"))?;

		Ok(())
	}

	fn put_object(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &PutObjectArg,
	) -> tg::Result<()> {
		let id = &arg.id;
		let key = Key::Object(id).pack_to_vec();

		// Read existing value if any.
		let existing = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
			.and_then(|bytes| tangram_serialize::from_slice::<crate::Object>(bytes).ok());

		// Merge touched_at (use max).
		let touched_at = existing
			.as_ref()
			.map_or(arg.touched_at, |e| e.touched_at.max(arg.touched_at));

		// Keep reference count from existing entry.
		let reference_count = existing.as_ref().map_or(0, |e| e.reference_count);

		// Use cache_entry from arg if provided, otherwise keep existing.
		let cache_entry = arg
			.cache_entry
			.clone()
			.or_else(|| existing.as_ref().and_then(|e| e.cache_entry.clone()));

		// Merge stored flags.
		let stored = crate::ObjectStored {
			subtree: arg.stored.subtree || existing.as_ref().is_some_and(|e| e.stored.subtree),
		};

		// Merge metadata (keep existing if arg values are default).
		let metadata = if let Some(ref existing) = existing {
			merge_object_metadata(&arg.metadata, &existing.metadata)
		} else {
			arg.metadata.clone()
		};

		let value = crate::Object {
			cache_entry,
			metadata,
			reference_count,
			stored,
			touched_at,
		};
		let value_bytes = tangram_serialize::to_vec(&value)
			.map_err(|source| tg::error!(!source, "failed to serialize the object"))?;
		db.put(transaction, &key, &value_bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to put the object"))?;

		// Put child relationships.
		for child in &arg.children {
			let key = Key::ObjectChild { object: id, child }.pack_to_vec();
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the object child"))?;

			let key = Key::ChildObject { child, object: id }.pack_to_vec();
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the child object"))?;
		}

		// Put cache entry relationship.
		if let Some(cache_entry) = &arg.cache_entry {
			let key = Key::ObjectCacheEntry {
				cache_entry,
				object: id,
			}
			.pack_to_vec();
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the object cache entry"))?;
		}

		Ok(())
	}

	fn put_process(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &PutProcessArg,
	) -> tg::Result<()> {
		let id = &arg.id;
		let key = Key::Process(id).pack_to_vec();

		// Read existing value if any.
		let existing = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?
			.and_then(|bytes| tangram_serialize::from_slice::<crate::Process>(bytes).ok());

		// Merge touched_at (use max).
		let touched_at = existing
			.as_ref()
			.map_or(arg.touched_at, |e| e.touched_at.max(arg.touched_at));

		// Keep reference count from existing entry.
		let reference_count = existing.as_ref().map_or(0, |e| e.reference_count);

		// Merge stored flags.
		let stored = merge_process_stored(&arg.stored, existing.as_ref().map(|e| &e.stored));

		// Merge metadata.
		let metadata = if let Some(ref existing) = existing {
			merge_process_metadata(&arg.metadata, &existing.metadata)
		} else {
			arg.metadata.clone()
		};

		let value = crate::Process {
			metadata,
			reference_count,
			stored,
			touched_at,
		};
		let value_bytes = tangram_serialize::to_vec(&value)
			.map_err(|source| tg::error!(!source, "failed to serialize the process"))?;
		db.put(transaction, &key, &value_bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to put the process"))?;

		// Put child relationships.
		for child in &arg.children {
			let key = Key::ProcessChild { process: id, child }.pack_to_vec();
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the process child"))?;

			let key = Key::ChildProcess { child, parent: id }.pack_to_vec();
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the child process"))?;
		}

		// Put object relationships.
		for (object, kind) in &arg.objects {
			let key = Key::ProcessObject {
				process: id,
				object,
				kind: *kind,
			}
			.pack_to_vec();
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the process object"))?;

			let key = Key::ObjectProcess {
				object,
				process: id,
				kind: *kind,
			}
			.pack_to_vec();
			db.put(transaction, &key, &[])
				.map_err(|source| tg::error!(!source, "failed to put the object process"))?;
		}

		Ok(())
	}

	fn put_tag(db: &Db, transaction: &mut lmdb::RwTxn<'_>, arg: &PutTagArg) -> tg::Result<()> {
		let key = Key::Tag(&arg.tag).pack_to_vec();
		let value = Tag {
			item: arg.item.clone(),
		};
		let value_bytes = tangram_serialize::to_vec(&value)
			.map_err(|source| tg::error!(!source, "failed to serialize the tag"))?;
		db.put(transaction, &key, &value_bytes)
			.map_err(|source| tg::error!(!source, "failed to put the tag"))?;

		// Put item tag relationship.
		let item_bytes: Vec<u8> = match &arg.item {
			tg::Either::Left(object_id) => object_id.to_bytes().to_vec(),
			tg::Either::Right(process_id) => process_id.to_bytes().to_vec(),
		};
		let key = Key::ItemTag {
			item: &item_bytes,
			tag: &arg.tag,
		}
		.pack_to_vec();
		db.put(transaction, &key, &[])
			.map_err(|source| tg::error!(!source, "failed to put the item tag"))?;

		Ok(())
	}
}

fn merge_object_metadata(
	new: &tg::object::Metadata,
	existing: &tg::object::Metadata,
) -> tg::object::Metadata {
	tg::object::Metadata {
		node: tg::object::metadata::Node {
			size: if new.node.size != 0 {
				new.node.size
			} else {
				existing.node.size
			},
			solvable: new.node.solvable || existing.node.solvable,
			solved: new.node.solved || existing.node.solved,
		},
		subtree: merge_subtree_metadata(&new.subtree, &existing.subtree),
	}
}

fn merge_subtree_metadata(
	new: &tg::object::metadata::Subtree,
	existing: &tg::object::metadata::Subtree,
) -> tg::object::metadata::Subtree {
	tg::object::metadata::Subtree {
		count: new.count.or(existing.count),
		depth: new.depth.or(existing.depth),
		size: new.size.or(existing.size),
		solvable: new.solvable.or(existing.solvable),
		solved: new.solved.or(existing.solved),
	}
}

fn merge_process_stored(
	new: &crate::ProcessStored,
	existing: Option<&crate::ProcessStored>,
) -> crate::ProcessStored {
	let existing = existing.cloned().unwrap_or_default();
	crate::ProcessStored {
		node_command: new.node_command || existing.node_command,
		node_error: new.node_error || existing.node_error,
		node_log: new.node_log || existing.node_log,
		node_output: new.node_output || existing.node_output,
		subtree: new.subtree || existing.subtree,
		subtree_command: new.subtree_command || existing.subtree_command,
		subtree_error: new.subtree_error || existing.subtree_error,
		subtree_log: new.subtree_log || existing.subtree_log,
		subtree_output: new.subtree_output || existing.subtree_output,
	}
}

fn merge_process_metadata(
	new: &tg::process::Metadata,
	existing: &tg::process::Metadata,
) -> tg::process::Metadata {
	tg::process::Metadata {
		node: tg::process::metadata::Node {
			command: merge_subtree_metadata(&new.node.command, &existing.node.command),
			error: merge_subtree_metadata(&new.node.error, &existing.node.error),
			log: merge_subtree_metadata(&new.node.log, &existing.node.log),
			output: merge_subtree_metadata(&new.node.output, &existing.node.output),
		},
		subtree: tg::process::metadata::Subtree {
			count: new.subtree.count.or(existing.subtree.count),
			command: merge_subtree_metadata(&new.subtree.command, &existing.subtree.command),
			error: merge_subtree_metadata(&new.subtree.error, &existing.subtree.error),
			log: merge_subtree_metadata(&new.subtree.log, &existing.subtree.log),
			output: merge_subtree_metadata(&new.subtree.output, &existing.subtree.output),
		},
	}
}
