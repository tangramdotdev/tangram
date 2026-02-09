use {
	futures::FutureExt as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_util::serde::{is_default, is_false},
};

#[cfg(feature = "foundationdb")]
pub mod fdb;
#[cfg(feature = "lmdb")]
pub mod lmdb;

pub mod prelude {
	pub use super::Index as _;
}

pub trait Index {
	fn try_get_objects(
		&self,
		ids: &[tg::object::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<Object>>>> + Send;

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<Object>>> + Send {
		self.try_get_objects(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<Process>>>> + Send;

	fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<Process>>> + Send {
		self.try_get_processes(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn touch_cache_entries(
		&self,
		ids: &[tg::artifact::Id],
		touched_at: i64,
	) -> impl Future<Output = tg::Result<Vec<Option<CacheEntry>>>> + Send;

	fn touch_cache_entry(
		&self,
		id: &tg::artifact::Id,
		touched_at: i64,
	) -> impl Future<Output = tg::Result<Option<CacheEntry>>> + Send {
		self.touch_cache_entries(std::slice::from_ref(id), touched_at)
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> impl Future<Output = tg::Result<Vec<Option<Object>>>> + Send;

	fn touch_object(
		&self,
		id: &tg::object::Id,
		touched_at: i64,
	) -> impl Future<Output = tg::Result<Option<Object>>> + Send {
		self.touch_objects(std::slice::from_ref(id), touched_at)
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> impl Future<Output = tg::Result<Vec<Option<Process>>>> + Send;

	fn touch_process(
		&self,
		id: &tg::process::Id,
		touched_at: i64,
	) -> impl Future<Output = tg::Result<Option<Process>>> + Send {
		self.touch_processes(std::slice::from_ref(id), touched_at)
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn put(&self, arg: PutArg) -> impl Future<Output = tg::Result<()>> + Send;

	fn put_tags(&self, args: &[PutTagArg]) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_tags(&self, tags: &[String]) -> impl Future<Output = tg::Result<()>> + Send;

	fn updates_finished(
		&self,
		transaction_id: u64,
	) -> impl Future<Output = tg::Result<bool>> + Send;

	fn update_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> impl Future<Output = tg::Result<usize>> + Send;

	fn clean(
		&self,
		max_touched_at: i64,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> impl Future<Output = tg::Result<CleanOutput>> + Send;

	fn get_transaction_id(&self) -> impl Future<Output = tg::Result<u64>> + Send;

	fn sync(&self) -> impl Future<Output = tg::Result<()>> + Send;

	fn partition_total(&self) -> u64;
}

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct CacheEntry {
	#[tangram_serialize(id = 0, default, skip_serializing_if = "is_default")]
	pub reference_count: u64,

	#[tangram_serialize(id = 1, default, skip_serializing_if = "is_default")]
	pub touched_at: i64,
}

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Object {
	#[tangram_serialize(id = 0, default, skip_serializing_if = "is_default")]
	pub cache_entry: Option<tg::artifact::Id>,

	#[tangram_serialize(id = 1, default, skip_serializing_if = "is_default")]
	pub metadata: tg::object::Metadata,

	#[tangram_serialize(id = 2, default, skip_serializing_if = "is_default")]
	pub reference_count: u64,

	#[tangram_serialize(id = 3, default, skip_serializing_if = "is_default")]
	pub stored: ObjectStored,

	#[tangram_serialize(id = 4)]
	pub touched_at: i64,
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

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Process {
	#[tangram_serialize(id = 0, default, skip_serializing_if = "is_default")]
	pub metadata: tg::process::Metadata,

	#[tangram_serialize(id = 1, default, skip_serializing_if = "is_default")]
	pub reference_count: u64,

	#[tangram_serialize(id = 2, default, skip_serializing_if = "is_default")]
	pub stored: ProcessStored,

	#[tangram_serialize(id = 3)]
	pub touched_at: i64,
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

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Tag {
	#[tangram_serialize(id = 0)]
	pub item: tg::Either<tg::object::Id, tg::process::Id>,
}

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

#[derive(Clone, Debug, Default)]
pub struct PutArg {
	pub cache_entries: Vec<PutCacheEntryArg>,
	pub objects: Vec<PutObjectArg>,
	pub processes: Vec<PutProcessArg>,
}

#[derive(Clone, Debug)]
pub struct PutCacheEntryArg {
	pub id: tg::artifact::Id,
	pub touched_at: i64,
	pub dependencies: Vec<tg::artifact::Id>,
}

#[derive(Clone, Debug)]
pub struct PutObjectArg {
	pub cache_entry: Option<tg::artifact::Id>,
	pub children: BTreeSet<tg::object::Id>,
	pub id: tg::object::Id,
	pub metadata: tg::object::Metadata,
	pub stored: ObjectStored,
	pub touched_at: i64,
}

impl PutObjectArg {
	#[must_use]
	pub fn complete(&self) -> bool {
		self.metadata.subtree.complete()
	}
}

#[derive(Clone, Debug)]
pub struct PutProcessArg {
	pub children: Vec<tg::process::Id>,
	pub id: tg::process::Id,
	pub metadata: tg::process::Metadata,
	pub objects: Vec<(tg::object::Id, ProcessObjectKind)>,
	pub stored: ProcessStored,
	pub touched_at: i64,
}

impl PutProcessArg {
	#[must_use]
	pub fn complete(&self) -> bool {
		self.metadata.subtree.count.is_some()
			&& self.metadata.subtree.command.complete()
			&& self.metadata.subtree.error.complete()
			&& self.metadata.subtree.log.complete()
			&& self.metadata.subtree.output.complete()
			&& self.metadata.node.command.complete()
			&& self.metadata.node.error.complete()
			&& self.metadata.node.log.complete()
			&& self.metadata.node.output.complete()
	}
}

pub enum ItemArg {
	CacheEntry(PutCacheEntryArg),
	Object(PutObjectArg),
	Process(PutProcessArg),
}

#[derive(Clone, Debug)]
pub struct PutTagArg {
	pub tag: String,
	pub item: tg::Either<tg::object::Id, tg::process::Id>,
}

#[derive(Clone, Debug, Default)]
pub struct CleanOutput {
	pub bytes: u64,
	pub cache_entries: Vec<tg::artifact::Id>,
	pub done: bool,
	pub objects: Vec<tg::object::Id>,
	pub processes: Vec<tg::process::Id>,
}

impl CacheEntry {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|source| tg::error!(!source, "failed to serialize the cache entry"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the cache entry"))
	}
}

impl Object {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|source| tg::error!(!source, "failed to serialize the object"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the object"))
	}
}

impl ObjectStored {
	pub fn merge(&mut self, other: &Self) {
		self.subtree = self.subtree || other.subtree;
	}
}

impl Process {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|source| tg::error!(!source, "failed to serialize the process"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the process"))
	}
}

impl ProcessStored {
	pub fn merge(&mut self, other: &Self) {
		self.node_command = self.node_command || other.node_command;
		self.node_error = self.node_error || other.node_error;
		self.node_log = self.node_log || other.node_log;
		self.node_output = self.node_output || other.node_output;
		self.subtree = self.subtree || other.subtree;
		self.subtree_command = self.subtree_command || other.subtree_command;
		self.subtree_error = self.subtree_error || other.subtree_error;
		self.subtree_log = self.subtree_log || other.subtree_log;
		self.subtree_output = self.subtree_output || other.subtree_output;
	}
}

impl Tag {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|source| tg::error!(!source, "failed to serialize the tag"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the tag"))
	}
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
