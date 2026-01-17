use {
	crate::{
		CleanOutput, DeleteTagArg, ObjectQueueKind, ObjectStored, ProcessObjectKind,
		ProcessQueueKind, ProcessStored, PutCacheEntryArg, PutObjectArg, PutProcessArg, PutTagArg,
		TouchObjectArg, TouchProcessArg,
	},
	foundationdb_tuple::{TupleDepth, TuplePack, Versionstamp, VersionstampOffset},
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

mod clean;
mod indexer;
mod metadata;
mod queue;
mod stored;
mod touch;
mod transaction;

pub struct Index {
	database: foundationdb::Database,
}

impl Index {
	pub fn new(cluster_path: &std::path::Path) -> tg::Result<Self> {
		// Open the cluster file and get a database handle.
		let database = foundationdb::Database::new(Some(cluster_path.to_str().unwrap()))
			.map_err(|source| tg::error!(!source, "failed to open the foundationdb cluster"))?;

		Ok(Self { database })
	}

	pub async fn sync(&self) -> tg::Result<()> {
		Ok(())
	}
}

impl crate::Index for Index {
	async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		self.try_get_object_metadata(id).await
	}

	async fn try_get_object_metadata_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::object::Metadata>>> {
		self.try_get_object_metadata_batch(ids).await
	}

	async fn try_get_object_stored(&self, id: &tg::object::Id) -> tg::Result<Option<ObjectStored>> {
		self.try_get_object_stored(id).await
	}

	async fn try_get_object_stored_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<ObjectStored>>> {
		self.try_get_object_stored_batch(ids).await
	}

	async fn try_get_object_stored_and_metadata(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(ObjectStored, tg::object::Metadata)>> {
		self.try_get_object_stored_and_metadata(id).await
	}

	async fn try_get_object_stored_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(ObjectStored, tg::object::Metadata)>>> {
		self.try_get_object_stored_and_metadata_batch(ids).await
	}

	async fn try_touch_object_and_get_stored_and_metadata(
		&self,
		id: &tg::object::Id,
		touched_at: i64,
	) -> tg::Result<Option<(ObjectStored, tg::object::Metadata)>> {
		self.try_touch_object_and_get_stored_and_metadata(id, touched_at)
			.await
	}

	async fn try_touch_object_and_get_stored_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(ObjectStored, tg::object::Metadata)>>> {
		self.try_touch_object_and_get_stored_and_metadata_batch(ids, touched_at)
			.await
	}

	async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		self.try_get_process_metadata(id).await
	}

	async fn try_get_process_metadata_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::Metadata>>> {
		self.try_get_process_metadata_batch(ids).await
	}

	async fn try_get_process_stored(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<ProcessStored>> {
		self.try_get_process_stored(id).await
	}

	async fn try_get_process_stored_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<ProcessStored>>> {
		self.try_get_process_stored_batch(ids).await
	}

	async fn try_get_process_stored_and_metadata_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(ProcessStored, tg::process::Metadata)>>> {
		self.try_get_process_stored_and_metadata_batch(ids).await
	}

	async fn try_touch_process_and_get_stored_and_metadata(
		&self,
		id: &tg::process::Id,
		touched_at: i64,
	) -> tg::Result<Option<(ProcessStored, tg::process::Metadata)>> {
		self.try_touch_process_and_get_stored_and_metadata(id, touched_at)
			.await
	}

	async fn try_touch_process_and_get_stored_and_metadata_batch(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(ProcessStored, tg::process::Metadata)>>> {
		self.try_touch_process_and_get_stored_and_metadata_batch(ids, touched_at)
			.await
	}

	async fn handle_messages(
		&self,
		put_cache_entry_messages: Vec<PutCacheEntryArg>,
		put_object_messages: Vec<PutObjectArg>,
		touch_object_messages: Vec<TouchObjectArg>,
		put_process_messages: Vec<PutProcessArg>,
		touch_process_messages: Vec<TouchProcessArg>,
		put_tag_messages: Vec<PutTagArg>,
		delete_tag_messages: Vec<DeleteTagArg>,
	) -> tg::Result<()> {
		self.handle_messages(
			put_cache_entry_messages,
			put_object_messages,
			touch_object_messages,
			put_process_messages,
			touch_process_messages,
			put_tag_messages,
			delete_tag_messages,
		)
		.await
	}

	async fn handle_queue(&self, batch_size: usize) -> tg::Result<usize> {
		self.handle_queue(batch_size).await
	}

	async fn get_transaction_id(&self) -> tg::Result<u64> {
		self.get_transaction_id().await
	}

	async fn get_queue_size(&self, transaction_id: u64) -> tg::Result<u64> {
		self.get_queue_size(transaction_id).await
	}

	async fn touch_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		self.touch_object(id).await
	}

	async fn touch_process(&self, id: &tg::process::Id) -> tg::Result<()> {
		self.touch_process(id).await
	}

	async fn clean(&self, max_touched_at: i64, n: usize) -> tg::Result<CleanOutput> {
		self.clean(max_touched_at, n).await
	}

	async fn sync(&self) -> tg::Result<()> {
		self.sync().await
	}
}

#[derive(Debug, Clone)]
pub enum Key<'a> {
	// Entity field keys.
	CacheEntryField {
		id: &'a tg::artifact::Id,
		field: CacheEntryKind,
	},
	ObjectField {
		id: &'a tg::object::Id,
		field: ObjectKind,
	},
	ProcessField {
		id: &'a tg::process::Id,
		field: ProcessKind,
	},

	// Tag key.
	Tag(&'a str),

	// Relationship keys.
	ObjectChild {
		object: &'a tg::object::Id,
		child: &'a tg::object::Id,
	},
	ObjectChildReverse {
		child: &'a tg::object::Id,
		parent: &'a tg::object::Id,
	},
	ObjectCacheEntry {
		cache_entry: &'a tg::artifact::Id,
		object: &'a tg::object::Id,
	},

	ProcessChild {
		process: &'a tg::process::Id,
		position: u64,
	},
	ProcessChildDedup {
		process: &'a tg::process::Id,
		child: &'a tg::process::Id,
	},
	ProcessChildReverse {
		child: &'a tg::process::Id,
		parent: &'a tg::process::Id,
	},
	ProcessObject {
		process: &'a tg::process::Id,
		object: &'a tg::object::Id,
		kind: ProcessObjectKind,
	},
	ProcessObjectReverse {
		object: &'a tg::object::Id,
		process: &'a tg::process::Id,
		kind: ProcessObjectKind,
	},

	TagItemReverse {
		item: &'a [u8],
		tag: &'a str,
	},

	// Queue keys.
	CacheEntryQueue {
		versionstamp: Versionstamp,
	},
	CacheEntryQueueDedup(&'a tg::artifact::Id),
	ObjectQueue {
		kind: ObjectQueueKind,
		versionstamp: Versionstamp,
	},
	ObjectQueueDedup {
		object: &'a tg::object::Id,
		kind: ObjectQueueKind,
	},
	ProcessQueue {
		kind: ProcessQueueKind,
		versionstamp: Versionstamp,
	},
	ProcessQueueDedup {
		process: &'a tg::process::Id,
		kind: ProcessQueueKind,
	},

	// Clean queue keys.
	CleanQueueCacheEntry {
		touched_at: i64,
		id: &'a tg::artifact::Id,
	},
	CleanQueueObject {
		touched_at: i64,
		id: &'a tg::object::Id,
	},
	CleanQueueProcess {
		touched_at: i64,
		id: &'a tg::process::Id,
	},

	// Queue count key.
	QueueCount {
		queue: Kind,
	},
}

impl TuplePack for Key<'_> {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: TupleDepth,
	) -> std::io::Result<VersionstampOffset> {
		match self {
			Key::CacheEntryField { id, field } => (
				Kind::CacheEntry.to_i32().unwrap(),
				id.to_bytes().as_ref(),
				field.to_i32().unwrap(),
			)
				.pack(w, tuple_depth),

			Key::ObjectField { id, field } => (
				Kind::Object.to_i32().unwrap(),
				id.to_bytes().as_ref(),
				field.to_i32().unwrap(),
			)
				.pack(w, tuple_depth),

			Key::ProcessField { id, field } => (
				Kind::Process.to_i32().unwrap(),
				id.to_bytes().as_ref(),
				field.to_i32().unwrap(),
			)
				.pack(w, tuple_depth),

			Key::Tag(pattern) => (Kind::Tag.to_i32().unwrap(), *pattern).pack(w, tuple_depth),

			Key::ObjectChild { object, child } => (
				Kind::ObjectChild.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				child.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ObjectChildReverse { child, parent } => (
				Kind::ObjectChildReverse.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				parent.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ObjectCacheEntry {
				cache_entry,
				object,
			} => (
				Kind::ObjectCacheEntry.to_i32().unwrap(),
				cache_entry.to_bytes().as_ref(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ProcessChild { process, position } => (
				Kind::ProcessChild.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				*position,
			)
				.pack(w, tuple_depth),

			Key::ProcessChildDedup { process, child } => (
				Kind::ProcessChildDedup.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				child.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ProcessChildReverse { child, parent } => (
				Kind::ProcessChildReverse.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				parent.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ProcessObject {
				process,
				object,
				kind,
			} => (
				Kind::ProcessObject.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				object.to_bytes().as_ref(),
				kind.to_i32().unwrap(),
			)
				.pack(w, tuple_depth),

			Key::ProcessObjectReverse {
				object,
				process,
				kind,
			} => (
				Kind::ProcessObjectReverse.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				process.to_bytes().as_ref(),
				kind.to_i32().unwrap(),
			)
				.pack(w, tuple_depth),

			Key::TagItemReverse { item, tag } => {
				(Kind::TagItemReverse.to_i32().unwrap(), *item, *tag).pack(w, tuple_depth)
			},

			Key::CacheEntryQueue { versionstamp } => {
				(Kind::CacheEntryQueue.to_i32().unwrap(), versionstamp).pack(w, tuple_depth)
			},

			Key::CacheEntryQueueDedup(id) => (
				Kind::CacheEntryQueueDedup.to_i32().unwrap(),
				id.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ObjectQueue { kind, versionstamp } => (
				Kind::ObjectQueue.to_i32().unwrap(),
				kind.to_i32().unwrap(),
				versionstamp,
			)
				.pack(w, tuple_depth),

			Key::ObjectQueueDedup { object, kind } => (
				Kind::ObjectQueueDedup.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				kind.to_i32().unwrap(),
			)
				.pack(w, tuple_depth),

			Key::ProcessQueue { kind, versionstamp } => (
				Kind::ProcessQueue.to_i32().unwrap(),
				kind.to_i32().unwrap(),
				versionstamp,
			)
				.pack(w, tuple_depth),

			Key::ProcessQueueDedup { process, kind } => (
				Kind::ProcessQueueDedup.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				kind.to_i32().unwrap(),
			)
				.pack(w, tuple_depth),

			Key::CleanQueueCacheEntry { touched_at, id } => (
				Kind::CleanQueueCacheEntry.to_i32().unwrap(),
				*touched_at,
				id.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::CleanQueueObject { touched_at, id } => (
				Kind::CleanQueueObject.to_i32().unwrap(),
				*touched_at,
				id.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::CleanQueueProcess { touched_at, id } => (
				Kind::CleanQueueProcess.to_i32().unwrap(),
				*touched_at,
				id.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::QueueCount { queue } => {
				(Kind::QueueCount.to_i32().unwrap(), queue.to_i32().unwrap()).pack(w, tuple_depth)
			},
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
pub enum Kind {
	CacheEntry = 0,
	Object = 1,
	Process = 2,
	Tag = 3,
	ObjectChild = 4,
	ObjectChildReverse = 5,
	ObjectCacheEntry = 6,
	ProcessChild = 7,
	ProcessChildDedup = 8,
	ProcessChildReverse = 9,
	ProcessObject = 10,
	ProcessObjectReverse = 11,
	TagItemReverse = 12,
	CacheEntryQueue = 16,
	CacheEntryQueueDedup = 17,
	ObjectQueue = 18,
	ObjectQueueDedup = 19,
	ProcessQueue = 20,
	ProcessQueueDedup = 21,
	CleanQueueCacheEntry = 32,
	CleanQueueObject = 33,
	CleanQueueProcess = 34,
	QueueCount = 48,
}

/// Field kinds for cache entries.
#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
pub enum CacheEntryKind {
	TouchedAt = 0,
	ReferenceCount = 1,
	ReferenceCountVersionstamp = 2,
}

/// Field kinds for objects.
#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
pub enum ObjectKind {
	// Core fields (0-15).
	CacheEntry = 0,
	TouchedAt = 1,
	ReferenceCount = 2,
	ReferenceCountVersionstamp = 3,
	InsertVersionstamp = 4,

	// Stored fields (16-31).
	StoredSubtree = 16,

	// Metadata fields (32-63).
	MetadataNodeSize = 32,
	MetadataNodeSolvable = 33,
	MetadataNodeSolved = 34,
	MetadataSubtreeCount = 35,
	MetadataSubtreeDepth = 36,
	MetadataSubtreeSize = 37,
	MetadataSubtreeSolvable = 38,
	MetadataSubtreeSolved = 39,
}

impl ObjectKind {
	pub const STORED_START: u8 = 16;
	pub const STORED_END: u8 = 17;
	pub const METADATA_START: u8 = 32;
	pub const METADATA_END: u8 = 40;
}

/// Field kinds for processes.
#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
pub enum ProcessKind {
	// Core fields (0-15).
	TouchedAt = 0,
	ReferenceCount = 1,
	ReferenceCountVersionstamp = 2,
	InsertVersionstamp = 3,

	// Stored fields (16-31).
	StoredNodeCommand = 16,
	StoredNodeError = 17,
	StoredNodeLog = 18,
	StoredNodeOutput = 19,
	StoredSubtree = 20,
	StoredSubtreeCommand = 21,
	StoredSubtreeError = 22,
	StoredSubtreeLog = 23,
	StoredSubtreeOutput = 24,

	// Metadata fields (32+).
	MetadataNodeCommandCount = 32,
	MetadataNodeCommandDepth = 33,
	MetadataNodeCommandSize = 34,
	MetadataNodeCommandSolvable = 35,
	MetadataNodeCommandSolved = 36,
	MetadataNodeErrorCount = 37,
	MetadataNodeErrorDepth = 38,
	MetadataNodeErrorSize = 39,
	MetadataNodeErrorSolvable = 40,
	MetadataNodeErrorSolved = 41,
	MetadataNodeLogCount = 42,
	MetadataNodeLogDepth = 43,
	MetadataNodeLogSize = 44,
	MetadataNodeLogSolvable = 45,
	MetadataNodeLogSolved = 46,
	MetadataNodeOutputCount = 47,
	MetadataNodeOutputDepth = 48,
	MetadataNodeOutputSize = 49,
	MetadataNodeOutputSolvable = 50,
	MetadataNodeOutputSolved = 51,
	MetadataSubtreeCommandCount = 52,
	MetadataSubtreeCommandDepth = 53,
	MetadataSubtreeCommandSize = 54,
	MetadataSubtreeCommandSolvable = 55,
	MetadataSubtreeCommandSolved = 56,
	MetadataSubtreeCount = 57,
	MetadataSubtreeErrorCount = 58,
	MetadataSubtreeErrorDepth = 59,
	MetadataSubtreeErrorSize = 60,
	MetadataSubtreeErrorSolvable = 61,
	MetadataSubtreeErrorSolved = 62,
	MetadataSubtreeLogCount = 63,
	MetadataSubtreeLogDepth = 64,
	MetadataSubtreeLogSize = 65,
	MetadataSubtreeLogSolvable = 66,
	MetadataSubtreeLogSolved = 67,
	MetadataSubtreeOutputCount = 68,
	MetadataSubtreeOutputDepth = 69,
	MetadataSubtreeOutputSize = 70,
	MetadataSubtreeOutputSolvable = 71,
	MetadataSubtreeOutputSolved = 72,
}

impl ProcessKind {
	pub const STORED_START: u8 = 16;
	pub const STORED_END: u8 = 25;
	pub const METADATA_START: u8 = 32;
	pub const METADATA_END: u8 = 73;
}
