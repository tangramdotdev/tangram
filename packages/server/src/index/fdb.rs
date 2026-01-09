use {
	super::message::{
		DeleteTag, ProcessObjectKind, PutCacheEntry, PutObject, PutProcess, PutTagMessage,
		TouchObject, TouchProcess,
	},
	crate::Server,
	foundationdb::{self as fdb, RangeOption, options::MutationType},
	foundationdb_tuple::{TuplePack as _, Versionstamp},
	num::ToPrimitive as _,
	std::sync::Arc,
	tangram_client as tg,
};

#[allow(clippy::doc_markdown)]
/// All keys for the FDB index, implementing TuplePack for consistent key construction.
///
/// Key prefixes:
/// - 1: CacheEntry primary
/// - 2: CacheEntryByTouchedAt secondary index
/// - 3: CacheEntryQueue (versionstamped keys for FIFO ordering)
/// - 4: Object primary
/// - 5: ObjectByTouchedAt secondary index
/// - 6: ObjectChildren (parent -> child)
/// - 7: ObjectChildrenReverse (child -> parent)
/// - 8: ObjectQueue (versionstamped keys for FIFO ordering)
/// - 9: Process primary
/// - 10: ProcessByTouchedAt secondary index
/// - 11: ProcessChildren (parent -> position -> child)
/// - 12: ProcessChildrenReverse (child -> parent)
/// - 13: ProcessObjects (process -> object -> kind)
/// - 14: ProcessObjectsReverse (object -> kind -> process)
/// - 15: ProcessQueue (versionstamped keys for FIFO ordering)
/// - 16: Tag primary
/// - 17: TagByItem secondary index
pub enum Key<'a> {
	// CacheEntry: (1, id) -> CacheEntryValue
	CacheEntry(&'a tg::artifact::Id),

	// CacheEntryByTouchedAt: (2, touched_at, id) -> ()
	// Secondary index for cleaning by touched_at.
	CacheEntryByTouchedAt {
		touched_at: i64,
		id: &'a tg::artifact::Id,
	},

	// CacheEntryQueue: (3, versionstamp, id) -> ()
	// Uses FDB versionstamp for FIFO ordering. Written with atomic_op + SetVersionstampedKey.
	#[allow(dead_code)]
	CacheEntryQueue {
		versionstamp: &'a [u8],
		id: &'a tg::artifact::Id,
	},

	// Object: (4, id) -> ObjectValue
	Object(&'a tg::object::Id),

	// ObjectByTouchedAt: (5, touched_at, reference_count_zero, id) -> ()
	// Secondary index for cleaning. reference_count_zero enables efficient range queries.
	ObjectByTouchedAt {
		touched_at: i64,
		reference_count_zero: bool,
		id: &'a tg::object::Id,
	},

	// ObjectChildren: (6, parent, child) -> ()
	ObjectChildren {
		parent: &'a tg::object::Id,
		child: &'a tg::object::Id,
	},

	// ObjectChildrenReverse: (7, child, parent) -> ()
	// Reverse index to find all parents of a child.
	ObjectChildrenReverse {
		child: &'a tg::object::Id,
		parent: &'a tg::object::Id,
	},

	// ObjectQueue: (8, kind, versionstamp, id) -> ()
	// Uses FDB versionstamp for FIFO ordering. Written with atomic_op + SetVersionstampedKey.
	#[allow(dead_code)]
	ObjectQueue {
		kind: i64,
		versionstamp: &'a [u8],
		id: &'a tg::object::Id,
	},

	// Process: (9, id) -> ProcessValue
	Process(&'a tg::process::Id),

	// ProcessByTouchedAt: (10, touched_at, reference_count_zero, id) -> ()
	ProcessByTouchedAt {
		touched_at: i64,
		reference_count_zero: bool,
		id: &'a tg::process::Id,
	},

	// ProcessChildren: (11, parent, position) -> child_id bytes
	ProcessChildren {
		parent: &'a tg::process::Id,
		position: u64,
	},

	// ProcessChildrenReverse: (12, child, parent) -> ()
	ProcessChildrenReverse {
		child: &'a tg::process::Id,
		parent: &'a tg::process::Id,
	},

	// ProcessObjects: (13, process, object, kind) -> ()
	ProcessObjects {
		process: &'a tg::process::Id,
		object: &'a tg::object::Id,
		kind: i64,
	},

	// ProcessObjectsReverse: (14, object, kind, process) -> ()
	// Reverse index to find processes referencing an object.
	ProcessObjectsReverse {
		object: &'a tg::object::Id,
		kind: i64,
		process: &'a tg::process::Id,
	},

	// ProcessQueue: (15, kind, versionstamp, id) -> ()
	// Uses FDB versionstamp for FIFO ordering. Written with atomic_op + SetVersionstampedKey.
	#[allow(dead_code)]
	ProcessQueue {
		kind: i64,
		versionstamp: &'a [u8],
		id: &'a tg::process::Id,
	},

	// Tag: (16, tag) -> item_id bytes
	Tag(&'a str),

	// TagByItem: (17, item_id) -> tag bytes
	TagByItem(&'a [u8]),
}

impl foundationdb_tuple::TuplePack for Key<'_> {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: foundationdb_tuple::TupleDepth,
	) -> std::io::Result<foundationdb_tuple::VersionstampOffset> {
		match self {
			Self::CacheEntry(id) => (1, id.to_bytes().as_ref()).pack(w, tuple_depth),

			Self::CacheEntryByTouchedAt { touched_at, id } => {
				(2, *touched_at, id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Self::CacheEntryQueue { versionstamp, id } => {
				(3i64, *versionstamp, id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Self::Object(id) => (4, id.to_bytes().as_ref()).pack(w, tuple_depth),

			Self::ObjectByTouchedAt {
				touched_at,
				reference_count_zero,
				id,
			} => (
				5,
				*touched_at,
				*reference_count_zero,
				id.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Self::ObjectChildren { parent, child } => {
				(6, parent.to_bytes().as_ref(), child.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Self::ObjectChildrenReverse { child, parent } => {
				(7, child.to_bytes().as_ref(), parent.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Self::ObjectQueue {
				kind,
				versionstamp,
				id,
			} => (8i64, *kind, *versionstamp, id.to_bytes().as_ref()).pack(w, tuple_depth),

			Self::Process(id) => (9, id.to_bytes().as_ref()).pack(w, tuple_depth),

			Self::ProcessByTouchedAt {
				touched_at,
				reference_count_zero,
				id,
			} => (
				10,
				*touched_at,
				*reference_count_zero,
				id.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Self::ProcessChildren { parent, position } => {
				(11, parent.to_bytes().as_ref(), *position).pack(w, tuple_depth)
			},

			Self::ProcessChildrenReverse { child, parent } => {
				(12, child.to_bytes().as_ref(), parent.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Self::ProcessObjects {
				process,
				object,
				kind,
			} => (
				13,
				process.to_bytes().as_ref(),
				object.to_bytes().as_ref(),
				*kind,
			)
				.pack(w, tuple_depth),

			Self::ProcessObjectsReverse {
				object,
				kind,
				process,
			} => (
				14,
				object.to_bytes().as_ref(),
				*kind,
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Self::ProcessQueue {
				kind,
				versionstamp,
				id,
			} => (15i64, *kind, *versionstamp, id.to_bytes().as_ref()).pack(w, tuple_depth),

			Self::Tag(tag) => (16, *tag).pack(w, tuple_depth),

			Self::TagByItem(item) => (17, *item).pack(w, tuple_depth),
		}
	}
}

/// Cache entry value stored in FDB.
#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct CacheEntryValue {
	#[tangram_serialize(id = 0)]
	pub reference_count: Option<i64>,
	#[tangram_serialize(id = 1)]
	pub reference_count_versionstamp: Option<Vec<u8>>,
	#[tangram_serialize(id = 2)]
	pub touched_at: i64,
}

/// Object value stored in FDB.
#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ObjectValue {
	#[tangram_serialize(id = 0)]
	pub cache_entry: Option<tg::artifact::Id>,
	#[tangram_serialize(id = 1)]
	pub node_size: u64,
	#[tangram_serialize(id = 2)]
	pub node_solvable: bool,
	#[tangram_serialize(id = 3)]
	pub node_solved: bool,
	#[tangram_serialize(id = 4)]
	pub reference_count: Option<i64>,
	#[tangram_serialize(id = 5)]
	pub reference_count_versionstamp: Option<Vec<u8>>,
	#[tangram_serialize(id = 6)]
	pub subtree_count: Option<u64>,
	#[tangram_serialize(id = 7)]
	pub subtree_depth: Option<u64>,
	#[tangram_serialize(id = 8)]
	pub subtree_size: Option<u64>,
	#[tangram_serialize(id = 9)]
	pub subtree_solvable: Option<bool>,
	#[tangram_serialize(id = 10)]
	pub subtree_solved: Option<bool>,
	#[tangram_serialize(id = 11)]
	pub subtree_stored: bool,
	#[tangram_serialize(id = 12)]
	pub touched_at: i64,
}

/// Process value stored in FDB.
#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ProcessValue {
	#[tangram_serialize(id = 0)]
	pub node_command_count: Option<u64>,
	#[tangram_serialize(id = 1)]
	pub node_command_depth: Option<u64>,
	#[tangram_serialize(id = 2)]
	pub node_command_size: Option<u64>,
	#[tangram_serialize(id = 3)]
	pub node_command_stored: bool,
	#[tangram_serialize(id = 4)]
	pub node_error_count: Option<u64>,
	#[tangram_serialize(id = 5)]
	pub node_error_depth: Option<u64>,
	#[tangram_serialize(id = 6)]
	pub node_error_size: Option<u64>,
	#[tangram_serialize(id = 7)]
	pub node_error_stored: bool,
	#[tangram_serialize(id = 8)]
	pub node_log_count: Option<u64>,
	#[tangram_serialize(id = 9)]
	pub node_log_depth: Option<u64>,
	#[tangram_serialize(id = 10)]
	pub node_log_size: Option<u64>,
	#[tangram_serialize(id = 11)]
	pub node_log_stored: bool,
	#[tangram_serialize(id = 12)]
	pub node_output_count: Option<u64>,
	#[tangram_serialize(id = 13)]
	pub node_output_depth: Option<u64>,
	#[tangram_serialize(id = 14)]
	pub node_output_size: Option<u64>,
	#[tangram_serialize(id = 15)]
	pub node_output_stored: bool,
	#[tangram_serialize(id = 16)]
	pub reference_count: Option<i64>,
	#[tangram_serialize(id = 17)]
	pub reference_count_versionstamp: Option<Vec<u8>>,
	#[tangram_serialize(id = 18)]
	pub subtree_command_count: Option<u64>,
	#[tangram_serialize(id = 19)]
	pub subtree_command_depth: Option<u64>,
	#[tangram_serialize(id = 20)]
	pub subtree_command_size: Option<u64>,
	#[tangram_serialize(id = 21)]
	pub subtree_command_stored: bool,
	#[tangram_serialize(id = 22)]
	pub subtree_count: Option<u64>,
	#[tangram_serialize(id = 23)]
	pub subtree_error_count: Option<u64>,
	#[tangram_serialize(id = 24)]
	pub subtree_error_depth: Option<u64>,
	#[tangram_serialize(id = 25)]
	pub subtree_error_size: Option<u64>,
	#[tangram_serialize(id = 26)]
	pub subtree_error_stored: bool,
	#[tangram_serialize(id = 27)]
	pub subtree_log_count: Option<u64>,
	#[tangram_serialize(id = 28)]
	pub subtree_log_depth: Option<u64>,
	#[tangram_serialize(id = 29)]
	pub subtree_log_size: Option<u64>,
	#[tangram_serialize(id = 30)]
	pub subtree_log_stored: bool,
	#[tangram_serialize(id = 31)]
	pub subtree_output_count: Option<u64>,
	#[tangram_serialize(id = 32)]
	pub subtree_output_depth: Option<u64>,
	#[tangram_serialize(id = 33)]
	pub subtree_output_size: Option<u64>,
	#[tangram_serialize(id = 34)]
	pub subtree_output_stored: bool,
	#[tangram_serialize(id = 35)]
	pub subtree_stored: bool,
	#[tangram_serialize(id = 36)]
	pub touched_at: i64,
}

impl Server {
	#[expect(clippy::too_many_arguments)]
	pub(super) async fn indexer_task_handle_messages_fdb(
		&self,
		database: &Arc<fdb::Database>,
		put_cache_entry_messages: Vec<PutCacheEntry>,
		put_object_messages: Vec<PutObject>,
		touch_object_messages: Vec<TouchObject>,
		put_process_messages: Vec<PutProcess>,
		touch_process_messages: Vec<TouchProcess>,
		put_tag_messages: Vec<PutTagMessage>,
		delete_tag_messages: Vec<DeleteTag>,
	) -> tg::Result<()> {
		let txn = database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		// Handle cache entries.
		for message in put_cache_entry_messages {
			self.put_cache_entry_fdb(&txn, &message).await?;
		}

		// Handle put objects.
		for message in put_object_messages {
			self.put_object_fdb(&txn, &message).await?;
		}

		// Handle touch objects.
		for message in touch_object_messages {
			self.touch_object_fdb_inner(&txn, &message).await?;
		}

		// Handle put processes.
		for message in put_process_messages {
			self.put_process_fdb(&txn, &message).await?;
		}

		// Handle touch processes.
		for message in touch_process_messages {
			self.touch_process_fdb_inner(&txn, &message).await?;
		}

		// Handle put tags.
		for message in put_tag_messages {
			self.put_tag_fdb(&txn, &message).await?;
		}

		// Handle delete tags.
		for message in delete_tag_messages {
			self.delete_tag_fdb(&txn, &message).await?;
		}

		// Commit the transaction.
		txn.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;

		Ok(())
	}

	async fn put_cache_entry_fdb(
		&self,
		txn: &fdb::Transaction,
		message: &PutCacheEntry,
	) -> tg::Result<()> {
		let key = Key::CacheEntry(&message.id).pack_to_vec();

		// Try to get existing value.
		let existing = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get cache entry"))?;

		let (old_touched_at, inserted) = if let Some(bytes) = existing {
			let value: CacheEntryValue = tangram_serialize::from_slice(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize cache entry"))?;
			(Some(value.touched_at), false)
		} else {
			(None, true)
		};

		// Create or update the value.
		let value = CacheEntryValue {
			reference_count: None,
			reference_count_versionstamp: None,
			touched_at: old_touched_at
				.map_or(message.touched_at, |old| old.max(message.touched_at)),
		};

		let value_bytes = tangram_serialize::to_vec(&value)
			.map_err(|source| tg::error!(!source, "failed to serialize cache entry"))?;
		txn.set(&key, &value_bytes);

		// Update secondary index.
		if let Some(old) = old_touched_at.filter(|&old| old != value.touched_at) {
			let old_key = Key::CacheEntryByTouchedAt {
				touched_at: old,
				id: &message.id,
			}
			.pack_to_vec();
			txn.clear(&old_key);
		}
		let new_key = Key::CacheEntryByTouchedAt {
			touched_at: value.touched_at,
			id: &message.id,
		}
		.pack_to_vec();
		txn.set(&new_key, &[]);

		// Enqueue with versionstamped key if inserted.
		if inserted {
			// Build key with versionstamp placeholder: (3, <versionstamp>, id)
			let id_bytes = message.id.to_bytes();
			let queue_key = (3i64, Versionstamp::incomplete(0), id_bytes.as_ref())
				.pack_to_vec_with_versionstamp();
			txn.atomic_op(&queue_key, &[], MutationType::SetVersionstampedKey);
		}

		Ok(())
	}

	async fn put_object_fdb(&self, txn: &fdb::Transaction, message: &PutObject) -> tg::Result<()> {
		let key = Key::Object(&message.id).pack_to_vec();

		// Try to get existing value.
		let existing = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object"))?;

		let (old_value, inserted) = if let Some(bytes) = existing {
			let value: ObjectValue = tangram_serialize::from_slice(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize object"))?;
			(Some(value), false)
		} else {
			(None, true)
		};

		// Insert children.
		for child in &message.children {
			let child_key = Key::ObjectChildren {
				parent: &message.id,
				child,
			}
			.pack_to_vec();
			txn.set(&child_key, &[]);

			let reverse_key = Key::ObjectChildrenReverse {
				child,
				parent: &message.id,
			}
			.pack_to_vec();
			txn.set(&reverse_key, &[]);
		}

		// Create or update the value.
		let new_value = ObjectValue {
			cache_entry: message.cache_entry.clone(),
			node_size: message.metadata.node.size,
			node_solvable: message.metadata.node.solvable,
			node_solved: message.metadata.node.solved,
			reference_count: old_value.as_ref().and_then(|v| v.reference_count),
			reference_count_versionstamp: old_value
				.as_ref()
				.and_then(|v| v.reference_count_versionstamp.clone()),
			subtree_count: message
				.metadata
				.subtree
				.count
				.or(old_value.as_ref().and_then(|v| v.subtree_count)),
			subtree_depth: message
				.metadata
				.subtree
				.depth
				.or(old_value.as_ref().and_then(|v| v.subtree_depth)),
			subtree_size: message
				.metadata
				.subtree
				.size
				.or(old_value.as_ref().and_then(|v| v.subtree_size)),
			subtree_solvable: message
				.metadata
				.subtree
				.solvable
				.or(old_value.as_ref().and_then(|v| v.subtree_solvable)),
			subtree_solved: message
				.metadata
				.subtree
				.solved
				.or(old_value.as_ref().and_then(|v| v.subtree_solved)),
			subtree_stored: message.stored.subtree
				|| old_value.as_ref().is_some_and(|v| v.subtree_stored),
			touched_at: old_value
				.as_ref()
				.map_or(message.touched_at, |v| v.touched_at.max(message.touched_at)),
		};

		let value_bytes = tangram_serialize::to_vec(&new_value)
			.map_err(|source| tg::error!(!source, "failed to serialize object"))?;
		txn.set(&key, &value_bytes);

		// Update secondary index.
		if let Some(ref old) = old_value {
			let ref_zero = old.reference_count == Some(0);
			let old_key = Key::ObjectByTouchedAt {
				touched_at: old.touched_at,
				reference_count_zero: ref_zero,
				id: &message.id,
			}
			.pack_to_vec();
			txn.clear(&old_key);
		}
		let ref_zero = new_value.reference_count == Some(0);
		let new_key = Key::ObjectByTouchedAt {
			touched_at: new_value.touched_at,
			reference_count_zero: ref_zero,
			id: &message.id,
		}
		.pack_to_vec();
		txn.set(&new_key, &[]);

		// Determine if subtree changed.
		let changed = inserted
			|| old_value.as_ref().is_some_and(|old| {
				old.subtree_count != new_value.subtree_count
					|| old.subtree_depth != new_value.subtree_depth
					|| old.subtree_size != new_value.subtree_size
					|| old.subtree_solvable != new_value.subtree_solvable
					|| old.subtree_solved != new_value.subtree_solved
					|| old.subtree_stored != new_value.subtree_stored
			});

		// Enqueue for reference count if inserted using versionstamped key.
		if inserted {
			// Build key with versionstamp placeholder: (8, kind, <versionstamp>, id)
			let id_bytes = message.id.to_bytes();
			let queue_key = (8i64, 0i64, Versionstamp::incomplete(0), id_bytes.as_ref())
				.pack_to_vec_with_versionstamp();
			txn.atomic_op(&queue_key, &[], MutationType::SetVersionstampedKey);
		}

		// Enqueue for stored and metadata if changed using versionstamped key.
		if changed {
			let id_bytes = message.id.to_bytes();
			let queue_key = (8i64, 1i64, Versionstamp::incomplete(0), id_bytes.as_ref())
				.pack_to_vec_with_versionstamp();
			txn.atomic_op(&queue_key, &[], MutationType::SetVersionstampedKey);
		}

		Ok(())
	}

	pub(crate) async fn touch_object_fdb_inner(
		&self,
		txn: &fdb::Transaction,
		message: &TouchObject,
	) -> tg::Result<()> {
		let key = Key::Object(&message.id).pack_to_vec();

		let existing = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object"))?;

		if let Some(bytes) = existing {
			let mut value: ObjectValue = tangram_serialize::from_slice(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize object"))?;

			if message.touched_at > value.touched_at {
				let old_touched_at = value.touched_at;
				value.touched_at = message.touched_at;

				let value_bytes = tangram_serialize::to_vec(&value)
					.map_err(|source| tg::error!(!source, "failed to serialize object"))?;
				txn.set(&key, &value_bytes);

				// Update secondary index.
				let ref_zero = value.reference_count == Some(0);
				let old_key = Key::ObjectByTouchedAt {
					touched_at: old_touched_at,
					reference_count_zero: ref_zero,
					id: &message.id,
				}
				.pack_to_vec();
				txn.clear(&old_key);

				let new_key = Key::ObjectByTouchedAt {
					touched_at: value.touched_at,
					reference_count_zero: ref_zero,
					id: &message.id,
				}
				.pack_to_vec();
				txn.set(&new_key, &[]);
			}
		}

		Ok(())
	}

	async fn put_process_fdb(
		&self,
		txn: &fdb::Transaction,
		message: &PutProcess,
	) -> tg::Result<()> {
		let key = Key::Process(&message.id).pack_to_vec();

		// Try to get existing value.
		let existing = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process"))?;

		let (old_value, inserted) = if let Some(bytes) = existing {
			let value: ProcessValue = tangram_serialize::from_slice(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize process"))?;
			(Some(value), false)
		} else {
			(None, true)
		};

		// Insert children.
		for (position, child) in message.children.iter().enumerate() {
			let child_key = Key::ProcessChildren {
				parent: &message.id,
				position: position.to_u64().unwrap(),
			}
			.pack_to_vec();
			txn.set(&child_key, child.to_bytes().as_ref());

			let reverse_key = Key::ProcessChildrenReverse {
				child,
				parent: &message.id,
			}
			.pack_to_vec();
			txn.set(&reverse_key, &[]);
		}

		// Insert process objects.
		for (object, kind) in &message.objects {
			let kind_value: i64 = match kind {
				ProcessObjectKind::Command => 0,
				ProcessObjectKind::Error => 1,
				ProcessObjectKind::Log => 2,
				ProcessObjectKind::Output => 3,
			};
			let obj_key = Key::ProcessObjects {
				process: &message.id,
				object,
				kind: kind_value,
			}
			.pack_to_vec();
			txn.set(&obj_key, &[]);

			let reverse_key = Key::ProcessObjectsReverse {
				object,
				kind: kind_value,
				process: &message.id,
			}
			.pack_to_vec();
			txn.set(&reverse_key, &[]);
		}

		// Create or update the value.
		let new_value = ProcessValue {
			node_command_count: message
				.metadata
				.node
				.command
				.count
				.or(old_value.as_ref().and_then(|v| v.node_command_count)),
			node_command_depth: message
				.metadata
				.node
				.command
				.depth
				.or(old_value.as_ref().and_then(|v| v.node_command_depth)),
			node_command_size: message
				.metadata
				.node
				.command
				.size
				.or(old_value.as_ref().and_then(|v| v.node_command_size)),
			node_command_stored: message.stored.node_command
				|| old_value.as_ref().is_some_and(|v| v.node_command_stored),
			node_error_count: message
				.metadata
				.node
				.error
				.count
				.or(old_value.as_ref().and_then(|v| v.node_error_count)),
			node_error_depth: message
				.metadata
				.node
				.error
				.depth
				.or(old_value.as_ref().and_then(|v| v.node_error_depth)),
			node_error_size: message
				.metadata
				.node
				.error
				.size
				.or(old_value.as_ref().and_then(|v| v.node_error_size)),
			node_error_stored: message.stored.node_error
				|| old_value.as_ref().is_some_and(|v| v.node_error_stored),
			node_log_count: message
				.metadata
				.node
				.log
				.count
				.or(old_value.as_ref().and_then(|v| v.node_log_count)),
			node_log_depth: message
				.metadata
				.node
				.log
				.depth
				.or(old_value.as_ref().and_then(|v| v.node_log_depth)),
			node_log_size: message
				.metadata
				.node
				.log
				.size
				.or(old_value.as_ref().and_then(|v| v.node_log_size)),
			node_log_stored: message.stored.node_log
				|| old_value.as_ref().is_some_and(|v| v.node_log_stored),
			node_output_count: message
				.metadata
				.node
				.output
				.count
				.or(old_value.as_ref().and_then(|v| v.node_output_count)),
			node_output_depth: message
				.metadata
				.node
				.output
				.depth
				.or(old_value.as_ref().and_then(|v| v.node_output_depth)),
			node_output_size: message
				.metadata
				.node
				.output
				.size
				.or(old_value.as_ref().and_then(|v| v.node_output_size)),
			node_output_stored: message.stored.node_output
				|| old_value.as_ref().is_some_and(|v| v.node_output_stored),
			reference_count: old_value.as_ref().and_then(|v| v.reference_count),
			reference_count_versionstamp: old_value
				.as_ref()
				.and_then(|v| v.reference_count_versionstamp.clone()),
			subtree_command_count: message
				.metadata
				.subtree
				.command
				.count
				.or(old_value.as_ref().and_then(|v| v.subtree_command_count)),
			subtree_command_depth: message
				.metadata
				.subtree
				.command
				.depth
				.or(old_value.as_ref().and_then(|v| v.subtree_command_depth)),
			subtree_command_size: message
				.metadata
				.subtree
				.command
				.size
				.or(old_value.as_ref().and_then(|v| v.subtree_command_size)),
			subtree_command_stored: message.stored.subtree_command
				|| old_value.as_ref().is_some_and(|v| v.subtree_command_stored),
			subtree_count: message
				.metadata
				.subtree
				.count
				.or(old_value.as_ref().and_then(|v| v.subtree_count)),
			subtree_error_count: message
				.metadata
				.subtree
				.error
				.count
				.or(old_value.as_ref().and_then(|v| v.subtree_error_count)),
			subtree_error_depth: message
				.metadata
				.subtree
				.error
				.depth
				.or(old_value.as_ref().and_then(|v| v.subtree_error_depth)),
			subtree_error_size: message
				.metadata
				.subtree
				.error
				.size
				.or(old_value.as_ref().and_then(|v| v.subtree_error_size)),
			subtree_error_stored: message.stored.subtree_error
				|| old_value.as_ref().is_some_and(|v| v.subtree_error_stored),
			subtree_log_count: message
				.metadata
				.subtree
				.log
				.count
				.or(old_value.as_ref().and_then(|v| v.subtree_log_count)),
			subtree_log_depth: message
				.metadata
				.subtree
				.log
				.depth
				.or(old_value.as_ref().and_then(|v| v.subtree_log_depth)),
			subtree_log_size: message
				.metadata
				.subtree
				.log
				.size
				.or(old_value.as_ref().and_then(|v| v.subtree_log_size)),
			subtree_log_stored: message.stored.subtree_log
				|| old_value.as_ref().is_some_and(|v| v.subtree_log_stored),
			subtree_output_count: message
				.metadata
				.subtree
				.output
				.count
				.or(old_value.as_ref().and_then(|v| v.subtree_output_count)),
			subtree_output_depth: message
				.metadata
				.subtree
				.output
				.depth
				.or(old_value.as_ref().and_then(|v| v.subtree_output_depth)),
			subtree_output_size: message
				.metadata
				.subtree
				.output
				.size
				.or(old_value.as_ref().and_then(|v| v.subtree_output_size)),
			subtree_output_stored: message.stored.subtree_output
				|| old_value.as_ref().is_some_and(|v| v.subtree_output_stored),
			subtree_stored: message.stored.subtree
				|| old_value.as_ref().is_some_and(|v| v.subtree_stored),
			touched_at: old_value
				.as_ref()
				.map_or(message.touched_at, |v| v.touched_at.max(message.touched_at)),
		};

		let value_bytes = tangram_serialize::to_vec(&new_value)
			.map_err(|source| tg::error!(!source, "failed to serialize process"))?;
		txn.set(&key, &value_bytes);

		// Update secondary index.
		if let Some(ref old) = old_value {
			let ref_zero = old.reference_count == Some(0);
			let old_key = Key::ProcessByTouchedAt {
				touched_at: old.touched_at,
				reference_count_zero: ref_zero,
				id: &message.id,
			}
			.pack_to_vec();
			txn.clear(&old_key);
		}
		let ref_zero = new_value.reference_count == Some(0);
		let new_key = Key::ProcessByTouchedAt {
			touched_at: new_value.touched_at,
			reference_count_zero: ref_zero,
			id: &message.id,
		}
		.pack_to_vec();
		txn.set(&new_key, &[]);

		// Enqueue for reference count if inserted using versionstamped key.
		if inserted {
			// Build key with versionstamp placeholder: (15, kind, <versionstamp>, id)
			let id_bytes = message.id.to_bytes();
			let queue_key = (15i64, 0i64, Versionstamp::incomplete(0), id_bytes.as_ref())
				.pack_to_vec_with_versionstamp();
			txn.atomic_op(&queue_key, &[], MutationType::SetVersionstampedKey);
		}

		// Enqueue for stored and metadata propagation using versionstamped keys.
		for kind in 1i64..=5 {
			let id_bytes = message.id.to_bytes();
			let queue_key = (15i64, kind, Versionstamp::incomplete(0), id_bytes.as_ref())
				.pack_to_vec_with_versionstamp();
			txn.atomic_op(&queue_key, &[], MutationType::SetVersionstampedKey);
		}

		Ok(())
	}

	pub(crate) async fn touch_process_fdb_inner(
		&self,
		txn: &fdb::Transaction,
		message: &TouchProcess,
	) -> tg::Result<()> {
		let key = Key::Process(&message.id).pack_to_vec();

		let existing = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process"))?;

		if let Some(bytes) = existing {
			let mut value: ProcessValue = tangram_serialize::from_slice(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize process"))?;

			if message.touched_at > value.touched_at {
				let old_touched_at = value.touched_at;
				value.touched_at = message.touched_at;

				let value_bytes = tangram_serialize::to_vec(&value)
					.map_err(|source| tg::error!(!source, "failed to serialize process"))?;
				txn.set(&key, &value_bytes);

				// Update secondary index.
				let ref_zero = value.reference_count == Some(0);
				let old_key = Key::ProcessByTouchedAt {
					touched_at: old_touched_at,
					reference_count_zero: ref_zero,
					id: &message.id,
				}
				.pack_to_vec();
				txn.clear(&old_key);

				let new_key = Key::ProcessByTouchedAt {
					touched_at: value.touched_at,
					reference_count_zero: ref_zero,
					id: &message.id,
				}
				.pack_to_vec();
				txn.set(&new_key, &[]);
			}
		}

		Ok(())
	}

	async fn put_tag_fdb(&self, txn: &fdb::Transaction, message: &PutTagMessage) -> tg::Result<()> {
		let key = Key::Tag(&message.tag).pack_to_vec();

		// Get old item if exists.
		let old_item = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get tag"))?;

		// New item bytes.
		let new_item_bytes = match &message.item {
			tg::Either::Left(object_id) => object_id.to_bytes().to_vec(),
			tg::Either::Right(process_id) => process_id.to_bytes().to_vec(),
		};

		// Set the tag.
		txn.set(&key, &new_item_bytes);

		// Update reverse index.
		if let Some(old_bytes) = old_item {
			if old_bytes.as_ref() != new_item_bytes {
				// Remove old reverse index.
				let old_reverse_key = Key::TagByItem(old_bytes.as_ref()).pack_to_vec();
				txn.clear(&old_reverse_key);

				// Decrement reference count on old item.
				self.decrement_reference_count_fdb(txn, old_bytes.as_ref())
					.await?;

				// Increment reference count on new item.
				self.increment_reference_count_fdb(txn, &new_item_bytes)
					.await?;
			}
		} else {
			// Increment reference count on new item.
			self.increment_reference_count_fdb(txn, &new_item_bytes)
				.await?;
		}

		// Set new reverse index.
		let new_reverse_key = Key::TagByItem(&new_item_bytes).pack_to_vec();
		txn.set(&new_reverse_key, message.tag.as_bytes());

		Ok(())
	}

	async fn delete_tag_fdb(&self, txn: &fdb::Transaction, message: &DeleteTag) -> tg::Result<()> {
		let key = Key::Tag(&message.tag).pack_to_vec();

		// Get old item if exists.
		let old_item = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get tag"))?;

		if let Some(old_bytes) = old_item {
			// Clear the tag.
			txn.clear(&key);

			// Remove reverse index.
			let reverse_key = Key::TagByItem(old_bytes.as_ref()).pack_to_vec();
			txn.clear(&reverse_key);

			// Decrement reference count.
			self.decrement_reference_count_fdb(txn, old_bytes.as_ref())
				.await?;
		}

		Ok(())
	}

	async fn increment_reference_count_fdb(
		&self,
		txn: &fdb::Transaction,
		item_bytes: &[u8],
	) -> tg::Result<()> {
		// Try as object.
		if let Ok(object_id) = tg::object::Id::from_slice(item_bytes) {
			let key = Key::Object(&object_id).pack_to_vec();
			if let Some(bytes) = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get object"))?
			{
				let mut value: ObjectValue = tangram_serialize::from_slice(&bytes)
					.map_err(|source| tg::error!(!source, "failed to deserialize object"))?;
				let old_ref_zero = value.reference_count == Some(0);
				value.reference_count = Some(value.reference_count.unwrap_or(0) + 1);
				let new_ref_zero = value.reference_count == Some(0);

				let value_bytes = tangram_serialize::to_vec(&value)
					.map_err(|source| tg::error!(!source, "failed to serialize object"))?;
				txn.set(&key, &value_bytes);

				// Update secondary index if reference_count_zero changed.
				if old_ref_zero != new_ref_zero {
					let old_key = Key::ObjectByTouchedAt {
						touched_at: value.touched_at,
						reference_count_zero: old_ref_zero,
						id: &object_id,
					}
					.pack_to_vec();
					txn.clear(&old_key);

					let new_key = Key::ObjectByTouchedAt {
						touched_at: value.touched_at,
						reference_count_zero: new_ref_zero,
						id: &object_id,
					}
					.pack_to_vec();
					txn.set(&new_key, &[]);
				}
			}
			return Ok(());
		}

		// Try as process.
		if let Ok(process_id) = tg::process::Id::from_slice(item_bytes) {
			let key = Key::Process(&process_id).pack_to_vec();
			if let Some(bytes) = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get process"))?
			{
				let mut value: ProcessValue = tangram_serialize::from_slice(&bytes)
					.map_err(|source| tg::error!(!source, "failed to deserialize process"))?;
				let old_ref_zero = value.reference_count == Some(0);
				value.reference_count = Some(value.reference_count.unwrap_or(0) + 1);
				let new_ref_zero = value.reference_count == Some(0);

				let value_bytes = tangram_serialize::to_vec(&value)
					.map_err(|source| tg::error!(!source, "failed to serialize process"))?;
				txn.set(&key, &value_bytes);

				// Update secondary index if reference_count_zero changed.
				if old_ref_zero != new_ref_zero {
					let old_key = Key::ProcessByTouchedAt {
						touched_at: value.touched_at,
						reference_count_zero: old_ref_zero,
						id: &process_id,
					}
					.pack_to_vec();
					txn.clear(&old_key);

					let new_key = Key::ProcessByTouchedAt {
						touched_at: value.touched_at,
						reference_count_zero: new_ref_zero,
						id: &process_id,
					}
					.pack_to_vec();
					txn.set(&new_key, &[]);
				}
			}
			return Ok(());
		}

		// Try as cache entry (artifact).
		if let Ok(artifact_id) = tg::artifact::Id::from_slice(item_bytes) {
			let key = Key::CacheEntry(&artifact_id).pack_to_vec();
			if let Some(bytes) = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get cache entry"))?
			{
				let mut value: CacheEntryValue = tangram_serialize::from_slice(&bytes)
					.map_err(|source| tg::error!(!source, "failed to deserialize cache entry"))?;
				value.reference_count = Some(value.reference_count.unwrap_or(0) + 1);

				let value_bytes = tangram_serialize::to_vec(&value)
					.map_err(|source| tg::error!(!source, "failed to serialize cache entry"))?;
				txn.set(&key, &value_bytes);
			}
		}

		Ok(())
	}

	async fn decrement_reference_count_fdb(
		&self,
		txn: &fdb::Transaction,
		item_bytes: &[u8],
	) -> tg::Result<()> {
		// Try as object.
		if let Ok(object_id) = tg::object::Id::from_slice(item_bytes) {
			let key = Key::Object(&object_id).pack_to_vec();
			if let Some(bytes) = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get object"))?
			{
				let mut value: ObjectValue = tangram_serialize::from_slice(&bytes)
					.map_err(|source| tg::error!(!source, "failed to deserialize object"))?;
				let old_ref_zero = value.reference_count == Some(0);
				value.reference_count = Some(value.reference_count.unwrap_or(0) - 1);
				let new_ref_zero = value.reference_count == Some(0);

				let value_bytes = tangram_serialize::to_vec(&value)
					.map_err(|source| tg::error!(!source, "failed to serialize object"))?;
				txn.set(&key, &value_bytes);

				// Update secondary index if reference_count_zero changed.
				if old_ref_zero != new_ref_zero {
					let old_key = Key::ObjectByTouchedAt {
						touched_at: value.touched_at,
						reference_count_zero: old_ref_zero,
						id: &object_id,
					}
					.pack_to_vec();
					txn.clear(&old_key);

					let new_key = Key::ObjectByTouchedAt {
						touched_at: value.touched_at,
						reference_count_zero: new_ref_zero,
						id: &object_id,
					}
					.pack_to_vec();
					txn.set(&new_key, &[]);
				}
			}
			return Ok(());
		}

		// Try as process.
		if let Ok(process_id) = tg::process::Id::from_slice(item_bytes) {
			let key = Key::Process(&process_id).pack_to_vec();
			if let Some(bytes) = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get process"))?
			{
				let mut value: ProcessValue = tangram_serialize::from_slice(&bytes)
					.map_err(|source| tg::error!(!source, "failed to deserialize process"))?;
				let old_ref_zero = value.reference_count == Some(0);
				value.reference_count = Some(value.reference_count.unwrap_or(0) - 1);
				let new_ref_zero = value.reference_count == Some(0);

				let value_bytes = tangram_serialize::to_vec(&value)
					.map_err(|source| tg::error!(!source, "failed to serialize process"))?;
				txn.set(&key, &value_bytes);

				// Update secondary index if reference_count_zero changed.
				if old_ref_zero != new_ref_zero {
					let old_key = Key::ProcessByTouchedAt {
						touched_at: value.touched_at,
						reference_count_zero: old_ref_zero,
						id: &process_id,
					}
					.pack_to_vec();
					txn.clear(&old_key);

					let new_key = Key::ProcessByTouchedAt {
						touched_at: value.touched_at,
						reference_count_zero: new_ref_zero,
						id: &process_id,
					}
					.pack_to_vec();
					txn.set(&new_key, &[]);
				}
			}
			return Ok(());
		}

		// Try as cache entry (artifact).
		if let Ok(artifact_id) = tg::artifact::Id::from_slice(item_bytes) {
			let key = Key::CacheEntry(&artifact_id).pack_to_vec();
			if let Some(bytes) = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get cache entry"))?
			{
				let mut value: CacheEntryValue = tangram_serialize::from_slice(&bytes)
					.map_err(|source| tg::error!(!source, "failed to deserialize cache entry"))?;
				value.reference_count = Some(value.reference_count.unwrap_or(0) - 1);

				let value_bytes = tangram_serialize::to_vec(&value)
					.map_err(|source| tg::error!(!source, "failed to serialize cache entry"))?;
				txn.set(&key, &value_bytes);
			}
		}

		Ok(())
	}

	pub(super) async fn indexer_handle_queue_fdb(
		&self,
		config: &crate::config::Indexer,
		database: &Arc<fdb::Database>,
	) -> tg::Result<usize> {
		let batch_size = config.queue_batch_size;

		let txn = database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let mut n = batch_size;
		n -= self.handle_stored_object_queue_fdb(&txn, n).await?;
		n -= self.handle_stored_process_queue_fdb(&txn, n).await?;
		n -= self
			.handle_reference_count_cache_entry_queue_fdb(&txn, n)
			.await?;
		n -= self
			.handle_reference_count_object_queue_fdb(&txn, n)
			.await?;
		n -= self
			.handle_reference_count_process_queue_fdb(&txn, n)
			.await?;
		let processed = batch_size - n;

		if processed > 0 {
			txn.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;
		}

		Ok(processed)
	}

	async fn handle_stored_object_queue_fdb(
		&self,
		txn: &fdb::Transaction,
		limit: usize,
	) -> tg::Result<usize> {
		// Dequeue objects with kind = 1 (stored/metadata propagation).
		// Use prefix (8, 1) for ObjectQueue with kind=1.
		let start = (8, 1i64).pack_to_vec();
		let mut end = start.clone();
		end.push(0xFF);

		let range = RangeOption::from((start.as_slice(), end.as_slice()));
		let items = txn
			.get_range(&range, limit, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object queue range"))?;

		for item in &items {
			txn.clear(item.key());

			// Parse object ID from key and enqueue parents.
			// For now, we skip the complex subtree aggregation logic.
			// This would require reading children and aggregating metrics.
		}

		Ok(items.len())
	}

	async fn handle_stored_process_queue_fdb(
		&self,
		txn: &fdb::Transaction,
		limit: usize,
	) -> tg::Result<usize> {
		// Dequeue processes with kind >= 1.
		// Use prefix (15, 1) for ProcessQueue with kind=1.
		let start = (15, 1i64).pack_to_vec();
		let end = (16,).pack_to_vec();

		let range = RangeOption::from((start.as_slice(), end.as_slice()));
		let items = txn
			.get_range(&range, limit, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process queue range"))?;

		for item in &items {
			txn.clear(item.key());
		}

		Ok(items.len())
	}

	async fn handle_reference_count_cache_entry_queue_fdb(
		&self,
		txn: &fdb::Transaction,
		limit: usize,
	) -> tg::Result<usize> {
		// Dequeue cache entries with prefix (3).
		let start = (3,).pack_to_vec();
		let mut end = start.clone();
		end.push(0xFF);

		let range = RangeOption::from((start.as_slice(), end.as_slice()));
		let items = txn
			.get_range(&range, limit, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get cache entry queue range"))?;

		for item in &items {
			txn.clear(item.key());
		}

		Ok(items.len())
	}

	async fn handle_reference_count_object_queue_fdb(
		&self,
		txn: &fdb::Transaction,
		limit: usize,
	) -> tg::Result<usize> {
		// Dequeue objects with kind = 0 (reference count).
		// Use prefix (8, 0) for ObjectQueue with kind=0.
		let start = (8, 0i64).pack_to_vec();
		let mut end = start.clone();
		end.push(0xFF);

		let range = RangeOption::from((start.as_slice(), end.as_slice()));
		let items = txn
			.get_range(&range, limit, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object queue range"))?;

		for item in &items {
			txn.clear(item.key());
		}

		Ok(items.len())
	}

	async fn handle_reference_count_process_queue_fdb(
		&self,
		txn: &fdb::Transaction,
		limit: usize,
	) -> tg::Result<usize> {
		// Dequeue processes with kind = 0 (reference count).
		// Use prefix (15, 0) for ProcessQueue with kind=0.
		let start = (15, 0i64).pack_to_vec();
		let mut end = start.clone();
		end.push(0xFF);

		let range = RangeOption::from((start.as_slice(), end.as_slice()));
		let items = txn
			.get_range(&range, limit, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process queue range"))?;

		for item in &items {
			txn.clear(item.key());
		}

		Ok(items.len())
	}

	pub(super) async fn indexer_get_queue_size_fdb(
		&self,
		database: &Arc<fdb::Database>,
	) -> tg::Result<u64> {
		let txn = database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let mut count = 0u64;

		// Count cache entry queue items.
		let start = (3,).pack_to_vec();
		let mut end = start.clone();
		end.push(0xFF);
		let range = RangeOption::from((start.as_slice(), end.as_slice()));
		let items = txn
			.get_range(&range, 10000, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to count cache entry queue"))?;
		count += items.len() as u64;

		// Count object queue items.
		let start = (8,).pack_to_vec();
		let mut end = start.clone();
		end.push(0xFF);
		let range = RangeOption::from((start.as_slice(), end.as_slice()));
		let items = txn
			.get_range(&range, 10000, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to count object queue"))?;
		count += items.len() as u64;

		// Count process queue items.
		let start = (15,).pack_to_vec();
		let mut end = start.clone();
		end.push(0xFF);
		let range = RangeOption::from((start.as_slice(), end.as_slice()));
		let items = txn
			.get_range(&range, 10000, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to count process queue"))?;
		count += items.len() as u64;

		Ok(count)
	}
}
