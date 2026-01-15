use {std::collections::BTreeSet, tangram_client::prelude::*, tangram_util::serde::is_false};

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

pub mod prelude {
	pub use super::Index as _;
}

/// The stored status of an object in the index.
#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ObjectStored {
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "is_false")]
	pub subtree: bool,
}

/// The stored status of a process in the index.
#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ProcessStored {
	/// Whether this node's command's subtree is stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "is_false")]
	pub node_command: bool,

	/// Whether this node's error's subtree is stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 7, default, skip_serializing_if = "is_false")]
	pub node_error: bool,

	/// Whether this node's log's subtree is stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "is_false")]
	pub node_log: bool,

	/// Whether this node's outputs' subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "is_false")]
	pub node_output: bool,

	/// Whether this node's subtree is stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 3, default, skip_serializing_if = "is_false")]
	pub subtree: bool,

	/// Whether this node's subtree's commands' subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 4, default, skip_serializing_if = "is_false")]
	pub subtree_command: bool,

	/// Whether this node's subtree's errors' subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 8, default, skip_serializing_if = "is_false")]
	pub subtree_error: bool,

	/// Whether this node's subtree's logs' subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 5, default, skip_serializing_if = "is_false")]
	pub subtree_log: bool,

	/// Whether this node's subtree's outputs' subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 6, default, skip_serializing_if = "is_false")]
	pub subtree_output: bool,
}

/// The output of a clean operation.
#[derive(Clone, Debug, Default)]
pub struct CleanOutput {
	pub bytes: u64,
	pub cache_entries: Vec<tg::artifact::Id>,
	pub objects: Vec<tg::object::Id>,
	pub processes: Vec<tg::process::Id>,
}

/// The kind of object associated with a process.
#[derive(
	Clone,
	Copy,
	Debug,
	num_derive::FromPrimitive,
	num_derive::ToPrimitive,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub enum ProcessObjectKind {
	#[tangram_serialize(id = 0)]
	Command = 0,
	#[tangram_serialize(id = 1)]
	Error = 1,
	#[tangram_serialize(id = 2)]
	Log = 2,
	#[tangram_serialize(id = 3)]
	Output = 3,
}

impl std::fmt::Display for ProcessObjectKind {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Command => write!(formatter, "command"),
			Self::Error => write!(formatter, "error"),
			Self::Log => write!(formatter, "log"),
			Self::Output => write!(formatter, "output"),
		}
	}
}

impl std::str::FromStr for ProcessObjectKind {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"command" => Ok(Self::Command),
			"error" => Ok(Self::Error),
			"log" => Ok(Self::Log),
			"output" => Ok(Self::Output),
			_ => Err(tg::error!("invalid kind")),
		}
	}
}

/// Arguments for putting a cache entry.
#[derive(Clone, Debug)]
pub struct PutCacheEntryArg {
	pub id: tg::artifact::Id,
	pub touched_at: i64,
}

/// Arguments for putting an object.
#[derive(Clone, Debug)]
pub struct PutObjectArg {
	pub cache_entry: Option<tg::artifact::Id>,
	pub children: BTreeSet<tg::object::Id>,
	pub id: tg::object::Id,
	pub metadata: tg::object::Metadata,
	pub stored: ObjectStored,
	pub touched_at: i64,
}

/// Arguments for touching an object.
#[derive(Clone, Debug)]
pub struct TouchObjectArg {
	pub id: tg::object::Id,
	pub touched_at: i64,
}

/// Arguments for putting a process.
#[derive(Clone, Debug)]
pub struct PutProcessArg {
	pub children: Vec<tg::process::Id>,
	pub id: tg::process::Id,
	pub metadata: tg::process::Metadata,
	pub objects: Vec<(tg::object::Id, ProcessObjectKind)>,
	pub stored: ProcessStored,
	pub touched_at: i64,
}

/// Arguments for touching a process.
#[derive(Clone, Debug)]
pub struct TouchProcessArg {
	pub id: tg::process::Id,
	pub touched_at: i64,
}

/// Arguments for putting a tag.
#[derive(Clone, Debug)]
pub struct PutTagArg {
	pub tag: String,
	pub item: tg::Either<tg::object::Id, tg::process::Id>,
}

/// Arguments for deleting a tag.
#[derive(Clone, Debug)]
pub struct DeleteTagArg {
	pub tag: String,
}

/// The index trait defines database operations for the index.
pub trait Index {
	// Object metadata operations.

	fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> impl std::future::Future<Output = tg::Result<Option<tg::object::Metadata>>> + Send;

	fn try_get_object_metadata_batch(
		&self,
		ids: &[tg::object::Id],
	) -> impl std::future::Future<Output = tg::Result<Vec<Option<tg::object::Metadata>>>> + Send;

	// Object stored operations.

	fn try_get_object_stored(
		&self,
		id: &tg::object::Id,
	) -> impl std::future::Future<Output = tg::Result<Option<ObjectStored>>> + Send;

	fn try_get_object_stored_batch(
		&self,
		ids: &[tg::object::Id],
	) -> impl std::future::Future<Output = tg::Result<Vec<Option<ObjectStored>>>> + Send;

	fn try_get_object_stored_and_metadata(
		&self,
		id: &tg::object::Id,
	) -> impl std::future::Future<Output = tg::Result<Option<(ObjectStored, tg::object::Metadata)>>> + Send;

	fn try_get_object_stored_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
	) -> impl std::future::Future<
		Output = tg::Result<Vec<Option<(ObjectStored, tg::object::Metadata)>>>,
	> + Send;

	fn try_touch_object_and_get_stored_and_metadata(
		&self,
		id: &tg::object::Id,
		touched_at: i64,
	) -> impl std::future::Future<Output = tg::Result<Option<(ObjectStored, tg::object::Metadata)>>> + Send;

	fn try_touch_object_and_get_stored_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> impl std::future::Future<
		Output = tg::Result<Vec<Option<(ObjectStored, tg::object::Metadata)>>>,
	> + Send;

	// Process metadata operations.

	fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> impl std::future::Future<Output = tg::Result<Option<tg::process::Metadata>>> + Send;

	fn try_get_process_metadata_batch(
		&self,
		ids: &[tg::process::Id],
	) -> impl std::future::Future<Output = tg::Result<Vec<Option<tg::process::Metadata>>>> + Send;

	// Process stored operations.

	fn try_get_process_stored(
		&self,
		id: &tg::process::Id,
	) -> impl std::future::Future<Output = tg::Result<Option<ProcessStored>>> + Send;

	fn try_get_process_stored_batch(
		&self,
		ids: &[tg::process::Id],
	) -> impl std::future::Future<Output = tg::Result<Vec<Option<ProcessStored>>>> + Send;

	fn try_get_process_stored_and_metadata_batch(
		&self,
		ids: &[tg::process::Id],
	) -> impl std::future::Future<
		Output = tg::Result<Vec<Option<(ProcessStored, tg::process::Metadata)>>>,
	> + Send;

	fn try_touch_process_and_get_stored_and_metadata(
		&self,
		id: &tg::process::Id,
		touched_at: i64,
	) -> impl std::future::Future<Output = tg::Result<Option<(ProcessStored, tg::process::Metadata)>>>
	+ Send;

	fn try_touch_process_and_get_stored_and_metadata_batch(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> impl std::future::Future<
		Output = tg::Result<Vec<Option<(ProcessStored, tg::process::Metadata)>>>,
	> + Send;

	// Indexer operations.

	#[allow(clippy::too_many_arguments)]
	fn handle_messages(
		&self,
		put_cache_entries: Vec<PutCacheEntryArg>,
		put_objects: Vec<PutObjectArg>,
		touch_objects: Vec<TouchObjectArg>,
		put_processes: Vec<PutProcessArg>,
		touch_processes: Vec<TouchProcessArg>,
		put_tags: Vec<PutTagArg>,
		delete_tags: Vec<DeleteTagArg>,
	) -> impl std::future::Future<Output = tg::Result<()>> + Send;

	fn handle_queue(
		&self,
		batch_size: usize,
	) -> impl std::future::Future<Output = tg::Result<usize>> + Send;

	fn get_transaction_id(&self) -> impl std::future::Future<Output = tg::Result<u64>> + Send;

	fn get_queue_size(
		&self,
		transaction_id: u64,
	) -> impl std::future::Future<Output = tg::Result<u64>> + Send;

	// Touch operations.

	fn touch_object(
		&self,
		id: &tg::object::Id,
	) -> impl std::future::Future<Output = tg::Result<()>> + Send;

	fn touch_process(
		&self,
		id: &tg::process::Id,
	) -> impl std::future::Future<Output = tg::Result<()>> + Send;

	// Cleaner operations.

	fn clean(
		&self,
		max_touched_at: i64,
		batch_size: usize,
	) -> impl std::future::Future<Output = tg::Result<CleanOutput>> + Send;

	// Sync operations.

	fn sync(&self) -> impl std::future::Future<Output = tg::Result<()>> + Send;
}
