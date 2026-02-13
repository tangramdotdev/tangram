use {
	crate::{CleanOutput, Object, Process, ProcessObjectKind, PutArg, PutTagArg},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	num_traits::{FromPrimitive as _, ToPrimitive as _},
	std::sync::Arc,
	tangram_client::prelude::*,
};

mod clean;
mod get;
mod put;
mod tag;
mod task;
mod touch;
mod update;

pub(super) use task::Metrics;

pub struct Index {
	database: Arc<fdb::Database>,
	partition_total: u64,
	subspace: fdbt::Subspace,
	sender_high: RequestSender,
	sender_medium: RequestSender,
	sender_low: RequestSender,
}

pub struct Options {
	pub cluster: std::path::PathBuf,
	pub concurrency: usize,
	pub max_items_per_transaction: usize,
	pub partition_total: u64,
	pub prefix: Option<String>,
}

type RequestSender = tokio::sync::mpsc::UnboundedSender<(Request, ResponseSender)>;
type RequestReceiver = tokio::sync::mpsc::UnboundedReceiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;

#[derive(Clone)]
enum Request {
	Clean {
		max_touched_at: i64,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	},
	DeleteTags(Vec<String>),
	Put(PutArg),
	PutTags(Vec<PutTagArg>),
	TouchCacheEntries {
		ids: Vec<tg::artifact::Id>,
		touched_at: i64,
	},
	TouchObjects {
		ids: Vec<tg::object::Id>,
		touched_at: i64,
	},
	TouchProcesses {
		ids: Vec<tg::process::Id>,
		touched_at: i64,
	},
	Update {
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	},
}

#[derive(Clone)]
enum Response {
	Unit,
	CacheEntries(Vec<Option<crate::CacheEntry>>),
	Objects(Vec<Option<Object>>),
	Processes(Vec<Option<Process>>),
	CleanOutput(CleanOutput),
	UpdateCount(usize),
}

#[derive(Debug, Clone)]
enum Key {
	CacheEntry(tg::artifact::Id),
	Object(tg::object::Id),
	Process(tg::process::Id),
	Tag(String),
	CacheEntryDependency {
		cache_entry: tg::artifact::Id,
		dependency: tg::artifact::Id,
	},
	DependencyCacheEntry {
		dependency: tg::artifact::Id,
		cache_entry: tg::artifact::Id,
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
	Clean {
		partition: u64,
		touched_at: i64,
		kind: ItemKind,
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
	Update {
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
	UpdateVersion {
		partition: u64,
		version: fdbt::Versionstamp,
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
	CacheEntryDependency = 4,
	DependencyCacheEntry = 5,
	ObjectChild = 6,
	ChildObject = 7,
	ObjectCacheEntry = 8,
	CacheEntryObject = 9,
	ProcessChild = 10,
	ChildProcess = 11,
	ProcessObject = 12,
	ObjectProcess = 13,
	ItemTag = 14,
	Clean = 15,
	Update = 16,
	UpdateVersion = 17,
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum Update {
	Put = 0,
	Propagate = 1,
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

		let metrics = Metrics::new();

		let (sender_high, receiver_high) = tokio::sync::mpsc::unbounded_channel();
		let (sender_medium, receiver_medium) = tokio::sync::mpsc::unbounded_channel();
		let (sender_low, receiver_low) = tokio::sync::mpsc::unbounded_channel();

		let concurrency = options.concurrency;
		let max_items_per_transaction = options.max_items_per_transaction;
		tokio::spawn({
			let database = database.clone();
			let subspace = subspace.clone();
			let metrics = metrics.clone();
			async move {
				Self::task(
					database,
					subspace,
					receiver_high,
					receiver_medium,
					receiver_low,
					concurrency,
					max_items_per_transaction,
					partition_total,
					metrics,
				)
				.await;
			}
		});

		let index = Self {
			database,
			partition_total,
			subspace,
			sender_high,
			sender_medium,
			sender_low,
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

	async fn touch_cache_entries(
		&self,
		ids: &[tg::artifact::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<crate::CacheEntry>>> {
		self.touch_cache_entries(ids, touched_at).await
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
			Key::CacheEntry(id) => (
				KeyKind::CacheEntry.to_i32().unwrap(),
				id.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(id) => {
				(KeyKind::Object.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Process(id) => {
				(KeyKind::Process.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Tag(tag) => (KeyKind::Tag.to_i32().unwrap(), tag.as_str()).pack(w, tuple_depth),

			Key::CacheEntryDependency {
				cache_entry,
				dependency,
			} => (
				KeyKind::CacheEntryDependency.to_i32().unwrap(),
				cache_entry.to_bytes().as_ref(),
				dependency.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::DependencyCacheEntry {
				dependency,
				cache_entry,
			} => (
				KeyKind::DependencyCacheEntry.to_i32().unwrap(),
				dependency.to_bytes().as_ref(),
				cache_entry.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

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
				let id = tg::artifact::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				Ok((input, Key::CacheEntry(id)))
			},

			KeyKind::Object => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::object::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				Ok((input, Key::Object(id)))
			},

			KeyKind::Process => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::process::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((input, Key::Process(id)))
			},

			KeyKind::Tag => {
				let (input, tag): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				Ok((input, Key::Tag(tag)))
			},

			KeyKind::CacheEntryDependency => {
				let (input, cache_entry_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, dependency_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let cache_entry = tg::artifact::Id::from_slice(&cache_entry_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let dependency = tg::artifact::Id::from_slice(&dependency_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let key = Key::CacheEntryDependency {
					cache_entry,
					dependency,
				};
				Ok((input, key))
			},

			KeyKind::DependencyCacheEntry => {
				let (input, dependency_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, cache_entry_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let dependency = tg::artifact::Id::from_slice(&dependency_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let cache_entry = tg::artifact::Id::from_slice(&cache_entry_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let key = Key::DependencyCacheEntry {
					dependency,
					cache_entry,
				};
				Ok((input, key))
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
