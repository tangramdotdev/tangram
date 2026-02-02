use {
	crate::{CleanOutput, ProcessObjectKind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	num_traits::{FromPrimitive as _, ToPrimitive as _},
	std::sync::Arc,
	tangram_client::prelude::*,
};

mod clean;
mod get;
mod put;
mod tag;
mod touch;
mod update;

pub struct Index {
	database: Arc<fdb::Database>,
	partition_total: u64,
	subspace: fdbt::Subspace,
	put_sender: tokio::sync::mpsc::UnboundedSender<self::put::Request>,
}

pub struct Options {
	pub cluster: std::path::PathBuf,
	pub partition_total: u64,
	pub prefix: Option<String>,
	pub put_concurrency: usize,
	pub put_max_keys_per_transaction: usize,
}

#[derive(Debug, Clone)]
enum Key {
	CacheEntry {
		id: tg::artifact::Id,
		field: CacheEntryField,
	},
	Object {
		id: tg::object::Id,
		field: ObjectField,
	},
	Process {
		id: tg::process::Id,
		field: ProcessField,
	},
	Tag {
		tag: String,
		field: TagField,
	},
	ObjectChild {
		object: tg::object::Id,
		child: tg::object::Id,
	},
	ChildObject {
		child: tg::object::Id,
		object: tg::object::Id,
	},
	ObjectCacheEntry {
		object: tg::object::Id,
		cache_entry: tg::artifact::Id,
	},
	CacheEntryObject {
		cache_entry: tg::artifact::Id,
		object: tg::object::Id,
	},
	ProcessChild {
		process: tg::process::Id,
		child: tg::process::Id,
	},
	ChildProcess {
		child: tg::process::Id,
		parent: tg::process::Id,
	},
	ProcessObject {
		process: tg::process::Id,
		kind: ProcessObjectKind,
		object: tg::object::Id,
	},
	ObjectProcess {
		object: tg::object::Id,
		kind: ProcessObjectKind,
		process: tg::process::Id,
	},
	ItemTag {
		item: Vec<u8>,
		tag: String,
	},
	Update {
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
	UpdateVersion {
		partition: u64,
		version: fdbt::Versionstamp,
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
	Clean {
		partition: u64,
		touched_at: i64,
		kind: ItemKind,
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum ItemKind {
	CacheEntry = 0,
	Object = 1,
	Process = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum KeyKind {
	CacheEntry = 0,
	Object = 1,
	Process = 2,
	Tag = 3,
	ObjectChild = 4,
	ChildObject = 5,
	ObjectCacheEntry = 6,
	CacheEntryObject = 7,
	ProcessChild = 8,
	ChildProcess = 9,
	ProcessObject = 10,
	ObjectProcess = 11,
	ItemTag = 12,
	Update = 13,
	UpdateVersion = 14,
	Clean = 15,
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum CacheEntryFamily {
	Core = 0,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum CacheEntryField {
	Core(CacheEntryCoreField),
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum CacheEntryCoreField {
	Exists = 0,
	TouchedAt = 1,
	ReferenceCount = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum ObjectFamily {
	Core = 0,
	Metadata = 1,
	Stored = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, derive_more::From)]
enum ObjectField {
	Core(ObjectCoreField),
	Metadata(ObjectMetadataField),
	Stored(ObjectStoredField),
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum ObjectCoreField {
	Exists = 0,
	TouchedAt = 1,
	ReferenceCount = 2,
	CacheEntry = 3,
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum ObjectMetadataField {
	NodeSize = 0,
	NodeSolvable = 1,
	NodeSolved = 2,
	SubtreeCount = 3,
	SubtreeDepth = 4,
	SubtreeSize = 5,
	SubtreeSolvable = 6,
	SubtreeSolved = 7,
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum ObjectStoredField {
	Subtree = 0,
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum ProcessFamily {
	Core = 0,
	Metadata = 1,
	Stored = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, derive_more::From)]
enum ProcessField {
	Core(ProcessCoreField),
	Metadata(ProcessMetadataField),
	Stored(ProcessStoredField),
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum ProcessCoreField {
	Exists = 0,
	TouchedAt = 1,
	ReferenceCount = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum ProcessMetadataField {
	NodeCommandCount = 0,
	NodeCommandDepth = 1,
	NodeCommandSize = 2,
	NodeCommandSolvable = 3,
	NodeCommandSolved = 4,
	NodeErrorCount = 5,
	NodeErrorDepth = 6,
	NodeErrorSize = 7,
	NodeErrorSolvable = 8,
	NodeErrorSolved = 9,
	NodeLogCount = 10,
	NodeLogDepth = 11,
	NodeLogSize = 12,
	NodeLogSolvable = 13,
	NodeLogSolved = 14,
	NodeOutputCount = 15,
	NodeOutputDepth = 16,
	NodeOutputSize = 17,
	NodeOutputSolvable = 18,
	NodeOutputSolved = 19,
	SubtreeCommandCount = 20,
	SubtreeCommandDepth = 21,
	SubtreeCommandSize = 22,
	SubtreeCommandSolvable = 23,
	SubtreeCommandSolved = 24,
	SubtreeCount = 25,
	SubtreeErrorCount = 26,
	SubtreeErrorDepth = 27,
	SubtreeErrorSize = 28,
	SubtreeErrorSolvable = 29,
	SubtreeErrorSolved = 30,
	SubtreeLogCount = 31,
	SubtreeLogDepth = 32,
	SubtreeLogSize = 33,
	SubtreeLogSolvable = 34,
	SubtreeLogSolved = 35,
	SubtreeOutputCount = 36,
	SubtreeOutputDepth = 37,
	SubtreeOutputSize = 38,
	SubtreeOutputSolvable = 39,
	SubtreeOutputSolved = 40,
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum ProcessStoredField {
	NodeCommand = 0,
	NodeError = 1,
	NodeLog = 2,
	NodeOutput = 3,
	Subtree = 4,
	SubtreeCommand = 5,
	SubtreeError = 6,
	SubtreeLog = 7,
	SubtreeOutput = 8,
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum TagFamily {
	Core = 0,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum TagField {
	Core(TagCoreField),
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum TagCoreField {
	Item = 0,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
enum Update {
	#[tangram_serialize(id = 0)]
	Put,

	#[tangram_serialize(id = 1)]
	Propagate(PropagateUpdate),
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
enum PropagateUpdate {
	#[tangram_serialize(id = 0)]
	Object(ObjectPropagateUpdate),

	#[tangram_serialize(id = 1)]
	Process(ProcessPropagateUpdate),
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
struct ObjectPropagateUpdate {
	#[tangram_serialize(id = 0)]
	fields: u64,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
struct ProcessPropagateUpdate {
	#[tangram_serialize(id = 0)]
	fields: u64,
}

bitflags::bitflags! {
	#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
	pub struct ObjectPropagateUpdateFields: u64 {
		// Stored fields (bits 0-15).
		const STORED_SUBTREE = 1 << 0;

		// Subtree metadata fields only (bits 16+).
		const METADATA_SUBTREE_COUNT = 1 << 16;
		const METADATA_SUBTREE_DEPTH = 1 << 17;
		const METADATA_SUBTREE_SIZE = 1 << 18;
		const METADATA_SUBTREE_SOLVABLE = 1 << 19;
		const METADATA_SUBTREE_SOLVED = 1 << 20;

		const ALL_STORED = Self::STORED_SUBTREE.bits();
		const ALL_METADATA = Self::METADATA_SUBTREE_COUNT.bits()
			| Self::METADATA_SUBTREE_DEPTH.bits()
			| Self::METADATA_SUBTREE_SIZE.bits()
			| Self::METADATA_SUBTREE_SOLVABLE.bits()
			| Self::METADATA_SUBTREE_SOLVED.bits();
		const ALL = Self::ALL_STORED.bits() | Self::ALL_METADATA.bits();
	}
}

bitflags::bitflags! {
	#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
	pub struct ProcessPropagateUpdateFields: u64 {
		// Stored fields (bits 0-15).
		const STORED_NODE_COMMAND = 1 << 0;
		const STORED_NODE_ERROR = 1 << 1;
		const STORED_NODE_LOG = 1 << 2;
		const STORED_NODE_OUTPUT = 1 << 3;
		const STORED_SUBTREE = 1 << 4;
		const STORED_SUBTREE_COMMAND = 1 << 5;
		const STORED_SUBTREE_ERROR = 1 << 6;
		const STORED_SUBTREE_LOG = 1 << 7;
		const STORED_SUBTREE_OUTPUT = 1 << 8;

		// Node metadata fields (bits 16-35).
		const METADATA_NODE_COMMAND_COUNT = 1 << 16;
		const METADATA_NODE_COMMAND_DEPTH = 1 << 17;
		const METADATA_NODE_COMMAND_SIZE = 1 << 18;
		const METADATA_NODE_COMMAND_SOLVABLE = 1 << 19;
		const METADATA_NODE_COMMAND_SOLVED = 1 << 20;
		const METADATA_NODE_ERROR_COUNT = 1 << 21;
		const METADATA_NODE_ERROR_DEPTH = 1 << 22;
		const METADATA_NODE_ERROR_SIZE = 1 << 23;
		const METADATA_NODE_ERROR_SOLVABLE = 1 << 24;
		const METADATA_NODE_ERROR_SOLVED = 1 << 25;
		const METADATA_NODE_LOG_COUNT = 1 << 26;
		const METADATA_NODE_LOG_DEPTH = 1 << 27;
		const METADATA_NODE_LOG_SIZE = 1 << 28;
		const METADATA_NODE_LOG_SOLVABLE = 1 << 29;
		const METADATA_NODE_LOG_SOLVED = 1 << 30;
		const METADATA_NODE_OUTPUT_COUNT = 1 << 31;
		const METADATA_NODE_OUTPUT_DEPTH = 1 << 32;
		const METADATA_NODE_OUTPUT_SIZE = 1 << 33;
		const METADATA_NODE_OUTPUT_SOLVABLE = 1 << 34;
		const METADATA_NODE_OUTPUT_SOLVED = 1 << 35;

		// Subtree metadata fields (bits 36-56).
		const METADATA_SUBTREE_COUNT = 1 << 36;
		const METADATA_SUBTREE_COMMAND_COUNT = 1 << 37;
		const METADATA_SUBTREE_COMMAND_DEPTH = 1 << 38;
		const METADATA_SUBTREE_COMMAND_SIZE = 1 << 39;
		const METADATA_SUBTREE_COMMAND_SOLVABLE = 1 << 40;
		const METADATA_SUBTREE_COMMAND_SOLVED = 1 << 41;
		const METADATA_SUBTREE_ERROR_COUNT = 1 << 42;
		const METADATA_SUBTREE_ERROR_DEPTH = 1 << 43;
		const METADATA_SUBTREE_ERROR_SIZE = 1 << 44;
		const METADATA_SUBTREE_ERROR_SOLVABLE = 1 << 45;
		const METADATA_SUBTREE_ERROR_SOLVED = 1 << 46;
		const METADATA_SUBTREE_LOG_COUNT = 1 << 47;
		const METADATA_SUBTREE_LOG_DEPTH = 1 << 48;
		const METADATA_SUBTREE_LOG_SIZE = 1 << 49;
		const METADATA_SUBTREE_LOG_SOLVABLE = 1 << 50;
		const METADATA_SUBTREE_LOG_SOLVED = 1 << 51;
		const METADATA_SUBTREE_OUTPUT_COUNT = 1 << 52;
		const METADATA_SUBTREE_OUTPUT_DEPTH = 1 << 53;
		const METADATA_SUBTREE_OUTPUT_SIZE = 1 << 54;
		const METADATA_SUBTREE_OUTPUT_SOLVABLE = 1 << 55;
		const METADATA_SUBTREE_OUTPUT_SOLVED = 1 << 56;

		const ALL_STORED = Self::STORED_NODE_COMMAND.bits()
			| Self::STORED_NODE_ERROR.bits()
			| Self::STORED_NODE_LOG.bits()
			| Self::STORED_NODE_OUTPUT.bits()
			| Self::STORED_SUBTREE.bits()
			| Self::STORED_SUBTREE_COMMAND.bits()
			| Self::STORED_SUBTREE_ERROR.bits()
			| Self::STORED_SUBTREE_LOG.bits()
			| Self::STORED_SUBTREE_OUTPUT.bits();
		const ALL_NODE_METADATA = Self::METADATA_NODE_COMMAND_COUNT.bits()
			| Self::METADATA_NODE_COMMAND_DEPTH.bits()
			| Self::METADATA_NODE_COMMAND_SIZE.bits()
			| Self::METADATA_NODE_COMMAND_SOLVABLE.bits()
			| Self::METADATA_NODE_COMMAND_SOLVED.bits()
			| Self::METADATA_NODE_ERROR_COUNT.bits()
			| Self::METADATA_NODE_ERROR_DEPTH.bits()
			| Self::METADATA_NODE_ERROR_SIZE.bits()
			| Self::METADATA_NODE_ERROR_SOLVABLE.bits()
			| Self::METADATA_NODE_ERROR_SOLVED.bits()
			| Self::METADATA_NODE_LOG_COUNT.bits()
			| Self::METADATA_NODE_LOG_DEPTH.bits()
			| Self::METADATA_NODE_LOG_SIZE.bits()
			| Self::METADATA_NODE_LOG_SOLVABLE.bits()
			| Self::METADATA_NODE_LOG_SOLVED.bits()
			| Self::METADATA_NODE_OUTPUT_COUNT.bits()
			| Self::METADATA_NODE_OUTPUT_DEPTH.bits()
			| Self::METADATA_NODE_OUTPUT_SIZE.bits()
			| Self::METADATA_NODE_OUTPUT_SOLVABLE.bits()
			| Self::METADATA_NODE_OUTPUT_SOLVED.bits();
		const ALL_SUBTREE_METADATA = Self::METADATA_SUBTREE_COUNT.bits()
			| Self::METADATA_SUBTREE_COMMAND_COUNT.bits()
			| Self::METADATA_SUBTREE_COMMAND_DEPTH.bits()
			| Self::METADATA_SUBTREE_COMMAND_SIZE.bits()
			| Self::METADATA_SUBTREE_COMMAND_SOLVABLE.bits()
			| Self::METADATA_SUBTREE_COMMAND_SOLVED.bits()
			| Self::METADATA_SUBTREE_ERROR_COUNT.bits()
			| Self::METADATA_SUBTREE_ERROR_DEPTH.bits()
			| Self::METADATA_SUBTREE_ERROR_SIZE.bits()
			| Self::METADATA_SUBTREE_ERROR_SOLVABLE.bits()
			| Self::METADATA_SUBTREE_ERROR_SOLVED.bits()
			| Self::METADATA_SUBTREE_LOG_COUNT.bits()
			| Self::METADATA_SUBTREE_LOG_DEPTH.bits()
			| Self::METADATA_SUBTREE_LOG_SIZE.bits()
			| Self::METADATA_SUBTREE_LOG_SOLVABLE.bits()
			| Self::METADATA_SUBTREE_LOG_SOLVED.bits()
			| Self::METADATA_SUBTREE_OUTPUT_COUNT.bits()
			| Self::METADATA_SUBTREE_OUTPUT_DEPTH.bits()
			| Self::METADATA_SUBTREE_OUTPUT_SIZE.bits()
			| Self::METADATA_SUBTREE_OUTPUT_SOLVABLE.bits()
			| Self::METADATA_SUBTREE_OUTPUT_SOLVED.bits();
		const ALL_METADATA = Self::ALL_NODE_METADATA.bits() | Self::ALL_SUBTREE_METADATA.bits();
		const ALL = Self::ALL_STORED.bits() | Self::ALL_METADATA.bits();
	}
}

impl Index {
	pub fn new(options: &Options) -> tg::Result<Self> {
		let database = fdb::Database::new(Some(options.cluster.to_str().unwrap()))
			.map_err(|source| tg::error!(!source, "failed to open the foundationdb cluster"))?;
		let database = Arc::new(database);

		let subspace = match &options.prefix {
			Some(s) => fdbt::Subspace::from_bytes(s.clone().into_bytes()),
			None => fdbt::Subspace::all(),
		};

		let partition_total = options.partition_total;

		let (put_sender, put_receiver) = tokio::sync::mpsc::unbounded_channel();
		let put_concurrency = options.put_concurrency;
		let put_max_keys_per_transaction = options.put_max_keys_per_transaction;
		tokio::spawn({
			let database = database.clone();
			let subspace = subspace.clone();
			async move {
				Self::put_task(
					database,
					subspace,
					put_receiver,
					put_concurrency,
					put_max_keys_per_transaction,
					partition_total,
				)
				.await;
			}
		});

		let index = Self {
			database,
			partition_total,
			subspace,
			put_sender,
		};

		Ok(index)
	}

	fn partition_for_id(id_bytes: &[u8], partition_total: u64) -> u64 {
		let len = id_bytes.len();
		let start = len.saturating_sub(8);
		let mut bytes = [0u8; 8];
		bytes[8 - (len - start)..].copy_from_slice(&id_bytes[start..]);
		u64::from_be_bytes(bytes) % partition_total
	}

	fn pack<T: fdbt::TuplePack>(subspace: &fdbt::Subspace, key: &T) -> Vec<u8> {
		subspace.pack(key)
	}

	fn pack_with_versionstamp<T: fdbt::TuplePack>(subspace: &fdbt::Subspace, key: &T) -> Vec<u8> {
		subspace.pack_with_versionstamp(key)
	}

	fn unpack<'a, T: fdbt::TupleUnpack<'a>>(
		subspace: &fdbt::Subspace,
		bytes: &'a [u8],
	) -> tg::Result<T> {
		subspace
			.unpack(bytes)
			.map_err(|source| tg::error!(!source, "failed to unpack key"))
	}

	pub async fn get_transaction_id(&self) -> tg::Result<u64> {
		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create the transaction"))?;
		let version = txn
			.get_read_version()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the read version"))?
			.cast_unsigned();
		Ok(version)
	}

	pub async fn sync(&self) -> tg::Result<()> {
		Ok(())
	}
}

impl Update {
	fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|source| tg::error!(!source, "failed to serialize the update"))
	}

	fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the update"))
	}
}

impl crate::Index for Index {
	async fn try_get_objects(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<crate::Object>>> {
		self.try_get_objects(ids).await
	}

	async fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<crate::Process>>> {
		self.try_get_processes(ids).await
	}

	async fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<crate::Object>>> {
		self.touch_objects(ids, touched_at).await
	}

	async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<crate::Process>>> {
		self.touch_processes(ids, touched_at).await
	}

	async fn put(&self, arg: crate::PutArg) -> tg::Result<()> {
		self.put(arg).await
	}

	async fn put_tags(&self, args: &[crate::PutTagArg]) -> tg::Result<()> {
		self.put_tags(args).await
	}

	async fn delete_tags(&self, tags: &[String]) -> tg::Result<()> {
		self.delete_tags(tags).await
	}

	async fn updates_finished(&self, transaction_id: u64) -> tg::Result<bool> {
		self.updates_finished(transaction_id).await
	}

	async fn update_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<usize> {
		self.update_batch(batch_size, partition_start, partition_count)
			.await
	}

	async fn clean(
		&self,
		max_touched_at: i64,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<CleanOutput> {
		self.clean(max_touched_at, batch_size, partition_start, partition_count)
			.await
	}

	fn partition_total(&self) -> u64 {
		self.partition_total
	}

	async fn get_transaction_id(&self) -> tg::Result<u64> {
		self.get_transaction_id().await
	}

	async fn sync(&self) -> tg::Result<()> {
		self.sync().await
	}
}

impl fdbt::TuplePack for Key {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		match self {
			Key::CacheEntry { id, field } => {
				KeyKind::CacheEntry.to_i32().unwrap().pack(w, tuple_depth)?;
				id.to_bytes().as_ref().pack(w, tuple_depth)?;
				field.pack(w, tuple_depth)
			},

			Key::Object { id, field } => {
				KeyKind::Object.to_i32().unwrap().pack(w, tuple_depth)?;
				id.to_bytes().as_ref().pack(w, tuple_depth)?;
				field.pack(w, tuple_depth)
			},

			Key::Process { id, field } => {
				KeyKind::Process.to_i32().unwrap().pack(w, tuple_depth)?;
				id.to_bytes().as_ref().pack(w, tuple_depth)?;
				field.pack(w, tuple_depth)
			},

			Key::Tag { tag, field } => {
				KeyKind::Tag.to_i32().unwrap().pack(w, tuple_depth)?;
				tag.as_str().pack(w, tuple_depth)?;
				field.pack(w, tuple_depth)
			},

			Key::ObjectChild { object, child } => (
				KeyKind::ObjectChild.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				child.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ChildObject {
				child,
				object: parent,
			} => (
				KeyKind::ChildObject.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				parent.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ObjectCacheEntry {
				object,
				cache_entry,
			} => (
				KeyKind::ObjectCacheEntry.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				cache_entry.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::CacheEntryObject {
				cache_entry,
				object,
			} => (
				KeyKind::CacheEntryObject.to_i32().unwrap(),
				cache_entry.to_bytes().as_ref(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ProcessChild { process, child } => (
				KeyKind::ProcessChild.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				child.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ChildProcess { child, parent } => (
				KeyKind::ChildProcess.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				parent.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ProcessObject {
				process,
				kind,
				object,
			} => (
				KeyKind::ProcessObject.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				kind.to_i32().unwrap(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ObjectProcess {
				object,
				kind,
				process,
			} => (
				KeyKind::ObjectProcess.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				kind.to_i32().unwrap(),
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ItemTag { item, tag } => (
				KeyKind::ItemTag.to_i32().unwrap(),
				item.as_slice(),
				tag.as_str(),
			)
				.pack(w, tuple_depth),

			Key::Update { id } => {
				KeyKind::Update.to_i32().unwrap().pack(w, tuple_depth)?;
				let id = match &id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				id.as_ref().pack(w, tuple_depth)
			},

			Key::UpdateVersion {
				partition,
				version,
				id,
			} => {
				let mut offset = KeyKind::UpdateVersion
					.to_i32()
					.unwrap()
					.pack(w, tuple_depth)?;
				offset += partition.pack(w, tuple_depth)?;
				offset += version.pack(w, tuple_depth)?;
				let id = match &id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				offset += id.as_ref().pack(w, tuple_depth)?;
				Ok(offset)
			},

			Key::Clean {
				partition,
				touched_at,
				kind,
				id,
			} => {
				KeyKind::Clean.to_i32().unwrap().pack(w, tuple_depth)?;
				partition.pack(w, tuple_depth)?;
				touched_at.pack(w, tuple_depth)?;
				kind.to_i32().unwrap().pack(w, tuple_depth)?;
				let id = match &id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				id.as_ref().pack(w, tuple_depth)
			},
		}
	}
}

impl fdbt::TupleUnpack<'_> for Key {
	fn unpack(input: &[u8], tuple_depth: fdbt::TupleDepth) -> fdbt::PackResult<(&[u8], Self)> {
		let (input, kind) = i32::unpack(input, tuple_depth)?;
		let kind =
			KeyKind::from_i32(kind).ok_or(fdbt::PackError::Message("invalid kind".into()))?;

		match kind {
			KeyKind::CacheEntry => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, field) = CacheEntryField::unpack(input, tuple_depth)?;
				let id = tg::artifact::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				Ok((input, Key::CacheEntry { id, field }))
			},

			KeyKind::Object => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, field) = ObjectField::unpack(input, tuple_depth)?;
				let id = tg::object::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				Ok((input, Key::Object { id, field }))
			},

			KeyKind::Process => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, field) = ProcessField::unpack(input, tuple_depth)?;
				let id = tg::process::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((input, Key::Process { id, field }))
			},

			KeyKind::Tag => {
				let (input, tag): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, field) = TagField::unpack(input, tuple_depth)?;
				Ok((input, Key::Tag { tag, field }))
			},

			KeyKind::ObjectChild => {
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let child = tg::object::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				Ok((input, Key::ObjectChild { object, child }))
			},

			KeyKind::ChildObject => {
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let child = tg::object::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				Ok((input, Key::ChildObject { child, object }))
			},

			KeyKind::ObjectCacheEntry => {
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, cache_entry_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let cache_entry = tg::artifact::Id::from_slice(&cache_entry_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let key = Key::ObjectCacheEntry {
					object,
					cache_entry,
				};
				Ok((input, key))
			},

			KeyKind::CacheEntryObject => {
				let (input, cache_entry_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let cache_entry = tg::artifact::Id::from_slice(&cache_entry_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let key = Key::CacheEntryObject {
					cache_entry,
					object,
				};
				Ok((input, key))
			},

			KeyKind::ProcessChild => {
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let child = tg::process::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((input, Key::ProcessChild { process, child }))
			},

			KeyKind::ChildProcess => {
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, parent_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let child = tg::process::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let parent = tg::process::Id::from_slice(&parent_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((input, Key::ChildProcess { child, parent }))
			},

			KeyKind::ProcessObject => {
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind) = ProcessObjectKind::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let key = Key::ProcessObject {
					process,
					kind,
					object,
				};
				Ok((input, key))
			},

			KeyKind::ObjectProcess => {
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind) = ProcessObjectKind::unpack(input, tuple_depth)?;
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let key = Key::ObjectProcess {
					object,
					kind,
					process,
				};
				Ok((input, key))
			},

			KeyKind::ItemTag => {
				let (input, item): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, tag): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				Ok((input, Key::ItemTag { item, tag }))
			},

			KeyKind::Update => {
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let id = if let Ok(id) = tg::process::Id::try_from(id.clone()) {
					tg::Either::Right(id)
				} else if let Ok(id) = tg::object::Id::try_from(id) {
					tg::Either::Left(id)
				} else {
					return Err(fdbt::PackError::Message("invalid id".into()));
				};
				Ok((input, Key::Update { id }))
			},

			KeyKind::UpdateVersion => {
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, version) = fdbt::Versionstamp::unpack(input, tuple_depth)?;
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let id = if let Ok(id) = tg::process::Id::try_from(id.clone()) {
					tg::Either::Right(id)
				} else if let Ok(id) = tg::object::Id::try_from(id) {
					tg::Either::Left(id)
				} else {
					return Err(fdbt::PackError::Message("invalid id".into()));
				};
				let key = Key::UpdateVersion {
					partition,
					version,
					id,
				};
				Ok((input, key))
			},

			KeyKind::Clean => {
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, touched_at): (_, i64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind): (_, i32) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let kind = ItemKind::from_i32(kind)
					.ok_or(fdbt::PackError::Message("invalid item kind".into()))?;
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let id = if let Ok(id) = tg::process::Id::try_from(id.clone()) {
					tg::Either::Right(id)
				} else if let Ok(id) = tg::object::Id::try_from(id) {
					tg::Either::Left(id)
				} else {
					return Err(fdbt::PackError::Message("invalid id".into()));
				};
				let key = Key::Clean {
					partition,
					touched_at,
					kind,
					id,
				};
				Ok((input, key))
			},
		}
	}
}

impl fdbt::TuplePack for KeyKind {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		self.to_i32().unwrap().pack(w, tuple_depth)
	}
}

impl fdbt::TupleUnpack<'_> for KeyKind {
	fn unpack(input: &[u8], tuple_depth: fdbt::TupleDepth) -> fdbt::PackResult<(&[u8], Self)> {
		let (input, value) = i32::unpack(input, tuple_depth)?;
		let kind = Self::from_i32(value).ok_or(fdbt::PackError::Message("invalid kind".into()))?;
		Ok((input, kind))
	}
}

impl fdbt::TuplePack for CacheEntryField {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		match self {
			CacheEntryField::Core(field) => {
				CacheEntryFamily::Core
					.to_i32()
					.unwrap()
					.pack(w, tuple_depth)?;
				field.to_i32().unwrap().pack(w, tuple_depth)
			},
		}
	}
}

impl fdbt::TupleUnpack<'_> for CacheEntryField {
	fn unpack(input: &[u8], tuple_depth: fdbt::TupleDepth) -> fdbt::PackResult<(&[u8], Self)> {
		let (input, family) = i32::unpack(input, tuple_depth)?;
		let family = CacheEntryFamily::from_i32(family).ok_or(fdbt::PackError::Message(
			"invalid cache entry family".into(),
		))?;
		let (input, field) = i32::unpack(input, tuple_depth)?;
		match family {
			CacheEntryFamily::Core => {
				let field = CacheEntryCoreField::from_i32(field).ok_or(
					fdbt::PackError::Message("invalid cache entry core field".into()),
				)?;
				Ok((input, CacheEntryField::Core(field)))
			},
		}
	}
}

impl fdbt::TuplePack for ObjectField {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		match self {
			ObjectField::Core(field) => {
				ObjectFamily::Core.to_i32().unwrap().pack(w, tuple_depth)?;
				field.to_i32().unwrap().pack(w, tuple_depth)
			},
			ObjectField::Metadata(field) => {
				ObjectFamily::Metadata
					.to_i32()
					.unwrap()
					.pack(w, tuple_depth)?;
				field.to_i32().unwrap().pack(w, tuple_depth)
			},
			ObjectField::Stored(field) => {
				ObjectFamily::Stored
					.to_i32()
					.unwrap()
					.pack(w, tuple_depth)?;
				field.to_i32().unwrap().pack(w, tuple_depth)
			},
		}
	}
}

impl fdbt::TupleUnpack<'_> for ObjectField {
	fn unpack(input: &[u8], tuple_depth: fdbt::TupleDepth) -> fdbt::PackResult<(&[u8], Self)> {
		let (input, family) = i32::unpack(input, tuple_depth)?;
		let family = ObjectFamily::from_i32(family)
			.ok_or(fdbt::PackError::Message("invalid object family".into()))?;
		let (input, field) = i32::unpack(input, tuple_depth)?;
		match family {
			ObjectFamily::Core => {
				let field = ObjectCoreField::from_i32(field)
					.ok_or(fdbt::PackError::Message("invalid object core field".into()))?;
				Ok((input, ObjectField::Core(field)))
			},
			ObjectFamily::Metadata => {
				let field = ObjectMetadataField::from_i32(field).ok_or(
					fdbt::PackError::Message("invalid object metadata field".into()),
				)?;
				Ok((input, ObjectField::Metadata(field)))
			},
			ObjectFamily::Stored => {
				let field = ObjectStoredField::from_i32(field).ok_or(fdbt::PackError::Message(
					"invalid object stored field".into(),
				))?;
				Ok((input, ObjectField::Stored(field)))
			},
		}
	}
}

impl fdbt::TuplePack for ProcessField {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		match self {
			ProcessField::Core(field) => {
				ProcessFamily::Core.to_i32().unwrap().pack(w, tuple_depth)?;
				field.to_i32().unwrap().pack(w, tuple_depth)
			},
			ProcessField::Metadata(field) => {
				ProcessFamily::Metadata
					.to_i32()
					.unwrap()
					.pack(w, tuple_depth)?;
				field.to_i32().unwrap().pack(w, tuple_depth)
			},
			ProcessField::Stored(field) => {
				ProcessFamily::Stored
					.to_i32()
					.unwrap()
					.pack(w, tuple_depth)?;
				field.to_i32().unwrap().pack(w, tuple_depth)
			},
		}
	}
}

impl fdbt::TupleUnpack<'_> for ProcessField {
	fn unpack(input: &[u8], tuple_depth: fdbt::TupleDepth) -> fdbt::PackResult<(&[u8], Self)> {
		let (input, family) = i32::unpack(input, tuple_depth)?;
		let family = ProcessFamily::from_i32(family)
			.ok_or(fdbt::PackError::Message("invalid process family".into()))?;
		let (input, field) = i32::unpack(input, tuple_depth)?;
		match family {
			ProcessFamily::Core => {
				let field = ProcessCoreField::from_i32(field).ok_or(fdbt::PackError::Message(
					"invalid process core field".into(),
				))?;
				Ok((input, ProcessField::Core(field)))
			},
			ProcessFamily::Metadata => {
				let field = ProcessMetadataField::from_i32(field).ok_or(
					fdbt::PackError::Message("invalid process metadata field".into()),
				)?;
				Ok((input, ProcessField::Metadata(field)))
			},
			ProcessFamily::Stored => {
				let field = ProcessStoredField::from_i32(field).ok_or(fdbt::PackError::Message(
					"invalid process stored field".into(),
				))?;
				Ok((input, ProcessField::Stored(field)))
			},
		}
	}
}

impl fdbt::TuplePack for TagField {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		match self {
			TagField::Core(field) => {
				TagFamily::Core.to_i32().unwrap().pack(w, tuple_depth)?;
				field.to_i32().unwrap().pack(w, tuple_depth)
			},
		}
	}
}

impl fdbt::TupleUnpack<'_> for TagField {
	fn unpack(input: &[u8], tuple_depth: fdbt::TupleDepth) -> fdbt::PackResult<(&[u8], Self)> {
		let (input, family) = i32::unpack(input, tuple_depth)?;
		let family = TagFamily::from_i32(family)
			.ok_or(fdbt::PackError::Message("invalid tag family".into()))?;
		let (input, field) = i32::unpack(input, tuple_depth)?;
		match family {
			TagFamily::Core => {
				let field = TagCoreField::from_i32(field)
					.ok_or(fdbt::PackError::Message("invalid tag core field".into()))?;
				Ok((input, TagField::Core(field)))
			},
		}
	}
}

impl fdbt::TuplePack for ProcessObjectKind {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		(*self as i32).pack(w, tuple_depth)
	}
}

impl fdbt::TupleUnpack<'_> for ProcessObjectKind {
	fn unpack(input: &[u8], tuple_depth: fdbt::TupleDepth) -> fdbt::PackResult<(&[u8], Self)> {
		let (input, value) = i32::unpack(input, tuple_depth)?;
		let kind = Self::from_i32(value).ok_or(fdbt::PackError::Message(
			"invalid process object kind".into(),
		))?;
		Ok((input, kind))
	}
}
