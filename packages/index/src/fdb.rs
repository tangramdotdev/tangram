use {
	crate::{
		CleanOutput, DeleteTagArg, ObjectQueueKind, ObjectStored, ProcessObjectKind,
		ProcessQueueKind, ProcessStored, PutCacheEntryArg, PutObjectArg, PutProcessArg, PutTagArg,
		TouchObjectArg, TouchProcessArg,
	},
	bytes::Bytes,
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
	CacheEntry(&'a tg::artifact::Id),
	Object(&'a tg::object::Id),
	Process(&'a tg::process::Id),
	Tag(&'a str),

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

	QueueCount {
		queue: i32,
	},
}

impl TuplePack for Key<'_> {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: TupleDepth,
	) -> std::io::Result<VersionstampOffset> {
		match self {
			Key::CacheEntry(id) => (0, id.to_bytes().as_ref()).pack(w, tuple_depth),
			Key::Object(id) => (1, id.to_bytes().as_ref()).pack(w, tuple_depth),
			Key::Process(id) => (2, id.to_bytes().as_ref()).pack(w, tuple_depth),
			Key::Tag(pattern) => (3, *pattern).pack(w, tuple_depth),

			Key::ObjectChild { object, child } => {
				(4, object.to_bytes().as_ref(), child.to_bytes().as_ref()).pack(w, tuple_depth)
			},
			Key::ObjectChildReverse { child, parent } => {
				(5, child.to_bytes().as_ref(), parent.to_bytes().as_ref()).pack(w, tuple_depth)
			},
			Key::ObjectCacheEntry {
				cache_entry,
				object,
			} => (
				6,
				cache_entry.to_bytes().as_ref(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ProcessChild { process, position } => {
				(7, process.to_bytes().as_ref(), *position).pack(w, tuple_depth)
			},
			Key::ProcessChildDedup { process, child } => {
				(8, process.to_bytes().as_ref(), child.to_bytes().as_ref()).pack(w, tuple_depth)
			},
			Key::ProcessChildReverse { child, parent } => {
				(9, child.to_bytes().as_ref(), parent.to_bytes().as_ref()).pack(w, tuple_depth)
			},
			Key::ProcessObject {
				process,
				object,
				kind,
			} => (
				10,
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
				11,
				object.to_bytes().as_ref(),
				process.to_bytes().as_ref(),
				kind.to_i32().unwrap(),
			)
				.pack(w, tuple_depth),
			Key::TagItemReverse { item, tag } => (12, *item, *tag).pack(w, tuple_depth),

			Key::CacheEntryQueue { versionstamp } => (16, versionstamp).pack(w, tuple_depth),
			Key::CacheEntryQueueDedup(id) => (17, id.to_bytes().as_ref()).pack(w, tuple_depth),
			Key::ObjectQueue { kind, versionstamp } => {
				(18, kind.to_i32().unwrap(), versionstamp).pack(w, tuple_depth)
			},
			Key::ObjectQueueDedup { object, kind } => {
				(19, object.to_bytes().as_ref(), kind.to_i32().unwrap()).pack(w, tuple_depth)
			},
			Key::ProcessQueue { kind, versionstamp } => {
				(20, kind.to_i32().unwrap(), versionstamp).pack(w, tuple_depth)
			},
			Key::ProcessQueueDedup { process, kind } => {
				(21, process.to_bytes().as_ref(), kind.to_i32().unwrap()).pack(w, tuple_depth)
			},

			Key::CleanQueueCacheEntry { touched_at, id } => {
				(32, *touched_at, id.to_bytes().as_ref()).pack(w, tuple_depth)
			},
			Key::CleanQueueObject { touched_at, id } => {
				(33, *touched_at, id.to_bytes().as_ref()).pack(w, tuple_depth)
			},
			Key::CleanQueueProcess { touched_at, id } => {
				(34, *touched_at, id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::QueueCount { queue } => (48, *queue).pack(w, tuple_depth),
		}
	}
}

pub mod subspace {
	pub const CACHE_ENTRY: i32 = 0;
	pub const OBJECT: i32 = 1;
	pub const PROCESS: i32 = 2;
	pub const TAG: i32 = 3;
	pub const OBJECT_CHILD: i32 = 4;
	pub const OBJECT_CHILD_REVERSE: i32 = 5;
	pub const OBJECT_CACHE_ENTRY: i32 = 6;
	pub const PROCESS_CHILD: i32 = 7;
	pub const PROCESS_CHILD_DEDUP: i32 = 8;
	pub const PROCESS_CHILD_REVERSE: i32 = 9;
	pub const PROCESS_OBJECT: i32 = 10;
	pub const PROCESS_OBJECT_REVERSE: i32 = 11;
	pub const TAG_ITEM_REVERSE: i32 = 12;
	pub const CACHE_ENTRY_QUEUE: i32 = 16;
	pub const CACHE_ENTRY_QUEUE_DEDUP: i32 = 17;
	pub const OBJECT_QUEUE: i32 = 18;
	pub const OBJECT_QUEUE_DEDUP: i32 = 19;
	pub const PROCESS_QUEUE: i32 = 20;
	pub const PROCESS_QUEUE_DEDUP: i32 = 21;
	pub const CLEAN_QUEUE_CACHE_ENTRY: i32 = 32;
	pub const CLEAN_QUEUE_OBJECT: i32 = 33;
	pub const CLEAN_QUEUE_PROCESS: i32 = 34;
	pub const QUEUE_COUNT: i32 = 48;
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct CacheEntryValue {
	#[tangram_serialize(id = 0)]
	pub touched_at: i64,

	#[tangram_serialize(id = 1)]
	pub reference_count: Option<u64>,

	#[tangram_serialize(id = 2)]
	pub reference_count_versionstamp: Option<Bytes>,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct ObjectValue {
	#[tangram_serialize(id = 0)]
	pub cache_entry: Option<Bytes>,

	#[tangram_serialize(id = 1)]
	pub stored: ObjectStored,

	#[tangram_serialize(id = 2)]
	pub metadata: tg::object::Metadata,

	#[tangram_serialize(id = 3)]
	pub touched_at: i64,

	#[tangram_serialize(id = 4)]
	pub reference_count: Option<u64>,

	#[tangram_serialize(id = 5)]
	pub reference_count_versionstamp: Option<Bytes>,

	#[tangram_serialize(id = 6)]
	pub insert_versionstamp: Bytes,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct ProcessValue {
	#[tangram_serialize(id = 0)]
	pub stored: ProcessStored,

	#[tangram_serialize(id = 1)]
	pub metadata: tg::process::Metadata,

	#[tangram_serialize(id = 2)]
	pub touched_at: i64,

	#[tangram_serialize(id = 3)]
	pub reference_count: Option<u64>,

	#[tangram_serialize(id = 4)]
	pub reference_count_versionstamp: Option<Bytes>,

	#[tangram_serialize(id = 5)]
	pub insert_versionstamp: Bytes,
}
