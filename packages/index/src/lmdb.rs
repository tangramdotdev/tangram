use {
	crate::{CleanOutput, ProcessObjectKind, PutArg},
	crossbeam_channel as crossbeam, foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::{FromPrimitive as _, ToPrimitive as _},
	std::path::PathBuf,
	tangram_client::prelude::*,
};

mod clean;
mod get;
mod put;
mod tag;
mod touch;
mod update;

#[derive(Clone, Debug)]
pub struct Config {
	pub map_size: usize,
	pub path: PathBuf,
}

pub struct Index {
	db: Db,
	env: lmdb::Env,
	sender_high: RequestSender,
	sender_low: RequestSender,
}

type Db = lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>;

type RequestSender = crossbeam::Sender<(Request, ResponseSender)>;
type RequestReceiver = crossbeam::Receiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;

#[derive(Clone)]
enum Request {
	Clean {
		max_touched_at: i64,
		batch_size: usize,
	},
	DeleteTags(Vec<String>),
	Put(PutArg),
	PutTags(Vec<crate::PutTagArg>),
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
	},
}

#[derive(Clone)]
enum Response {
	Unit,
	Objects(Vec<Option<crate::Object>>),
	Processes(Vec<Option<crate::Process>>),
	CleanOutput(CleanOutput),
	UpdateCount(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum Kind {
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
	Clean = 13,
	Update = 14,
	UpdateVersion = 15,
}

#[derive(Debug)]
enum Key {
	CacheEntry(tg::artifact::Id),
	Object(tg::object::Id),
	Process(tg::process::Id),
	Tag(String),
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
		touched_at: i64,
		kind: ItemKind,
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
	Update {
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
	UpdateVersion {
		version: u64,
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
enum Update {
	Put = 0,
	Propagate = 1,
}

impl Index {
	pub fn new(config: &Config) -> tg::Result<Self> {
		std::fs::OpenOptions::new()
			.create(true)
			.truncate(false)
			.read(true)
			.write(true)
			.open(&config.path)
			.map_err(
				|source| tg::error!(!source, path = %config.path.display(), "failed to open the lmdb file"),
			)?;
		let env = unsafe {
			lmdb::EnvOpenOptions::new()
				.map_size(config.map_size)
				.max_dbs(3)
				.max_readers(1_000)
				.flags(
					lmdb::EnvFlags::NO_SUB_DIR
						| lmdb::EnvFlags::WRITE_MAP
						| lmdb::EnvFlags::MAP_ASYNC,
				)
				.open(&config.path)
				.map_err(|source| {
					tg::error!(!source, path = %config.path.display(), "failed to open the lmdb environment")
				})?
		};
		let mut transaction = env.write_txn().unwrap();
		let db = env
			.create_database(&mut transaction, None)
			.map_err(|source| tg::error!(!source, "failed to create the database"))?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		let (sender_high, receiver_high) = crossbeam::bounded(256);
		let (sender_low, receiver_low) = crossbeam::bounded(256);

		std::thread::spawn({
			let env = env.clone();
			move || Self::task(&env, &db, &receiver_high, &receiver_low)
		});

		Ok(Self {
			db,
			env,
			sender_high,
			sender_low,
		})
	}

	fn task(
		env: &lmdb::Env,
		db: &Db,
		receiver_high: &RequestReceiver,
		receiver_low: &RequestReceiver,
	) {
		loop {
			// Get requests and senders.
			let mut requests: Vec<Request> = Vec::new();
			let mut senders: Vec<ResponseSender> = Vec::new();
			while let Ok((request, sender)) = receiver_high.try_recv() {
				requests.push(request);
				senders.push(sender);
			}
			if requests.is_empty() {
				while let Ok((request, sender)) = receiver_low.try_recv() {
					requests.push(request);
					senders.push(sender);
				}
			}
			if requests.is_empty() {
				crossbeam::select! {
					recv(receiver_high) -> result => {
						if let Ok((request, sender)) = result {
							requests.push(request);
							senders.push(sender);
						}
					},
					recv(receiver_low) -> result => {
						if let Ok((request, sender)) = result {
							requests.push(request);
							senders.push(sender);
						}
					},
				}
				while let Ok((request, sender)) = receiver_high.try_recv() {
					requests.push(request);
					senders.push(sender);
				}
			}
			if requests.is_empty() {
				break;
			}

			// Begin a write transaction.
			let result = env
				.write_txn()
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"));
			let mut transaction = match result {
				Ok(transaction) => transaction,
				Err(error) => {
					for sender in senders {
						sender.send(Err(error.clone())).ok();
					}
					continue;
				},
			};

			// Process all requests.
			let mut responses = vec![];
			for request in requests {
				let result = match request {
					Request::Clean {
						max_touched_at,
						batch_size,
					} => Self::task_clean(db, &mut transaction, max_touched_at, batch_size)
						.map(Response::CleanOutput),
					Request::DeleteTags(tags) => {
						Self::task_delete_tags(db, &mut transaction, &tags).map(|()| Response::Unit)
					},
					Request::Put(arg) => {
						Self::task_put(db, &mut transaction, arg).map(|()| Response::Unit)
					},
					Request::PutTags(tags) => {
						Self::task_put_tags(db, &mut transaction, &tags).map(|()| Response::Unit)
					},
					Request::TouchObjects { ids, touched_at } => {
						Self::task_touch_objects(db, &mut transaction, &ids, touched_at)
							.map(Response::Objects)
					},
					Request::TouchProcesses { ids, touched_at } => {
						Self::task_touch_processes(db, &mut transaction, &ids, touched_at)
							.map(Response::Processes)
					},
					Request::Update { batch_size } => {
						Self::task_update_batch(db, &mut transaction, batch_size)
							.map(Response::UpdateCount)
					},
				};
				responses.push(result);
			}

			// Commit the transaction.
			let result = transaction
				.commit()
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"));
			if let Err(error) = result {
				for sender in senders {
					sender.send(Err(error.clone())).ok();
				}
				continue;
			}

			// Send responses.
			for (sender, result) in std::iter::zip(senders, responses) {
				sender.send(result).ok();
			}
		}
	}

	pub async fn get_transaction_id(&self) -> tg::Result<u64> {
		let env = self.env.clone();
		tokio::task::spawn_blocking(move || {
			let transaction = env
				.read_txn()
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
			Ok(transaction.id() as u64)
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}

	pub async fn sync(&self) -> tg::Result<()> {
		tokio::task::spawn_blocking({
			let env = self.env.clone();
			move || {
				env.force_sync()
					.map_err(|source| tg::error!(!source, "failed to sync"))
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))??;
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

	async fn update_batch(&self, batch_size: usize) -> tg::Result<usize> {
		self.update_batch(batch_size).await
	}

	async fn get_transaction_id(&self) -> tg::Result<u64> {
		self.get_transaction_id().await
	}

	async fn clean(&self, max_touched_at: i64, batch_size: usize) -> tg::Result<CleanOutput> {
		self.clean(max_touched_at, batch_size).await
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
			Key::CacheEntry(id) => {
				(Kind::CacheEntry.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Object(id) => {
				(Kind::Object.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Process(id) => {
				(Kind::Process.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Tag(tag) => (Kind::Tag.to_i32().unwrap(), tag.as_str()).pack(w, tuple_depth),

			Key::ObjectChild { object, child } => (
				Kind::ObjectChild.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				child.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ChildObject { child, object } => (
				Kind::ChildObject.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ObjectCacheEntry {
				object,
				cache_entry,
			} => (
				Kind::ObjectCacheEntry.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				cache_entry.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::CacheEntryObject {
				cache_entry,
				object,
			} => (
				Kind::CacheEntryObject.to_i32().unwrap(),
				cache_entry.to_bytes().as_ref(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ProcessChild { process, child } => (
				Kind::ProcessChild.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				child.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ChildProcess { child, parent } => (
				Kind::ChildProcess.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				parent.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ProcessObject {
				process,
				kind,
				object,
			} => (
				Kind::ProcessObject.to_i32().unwrap(),
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
				Kind::ObjectProcess.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				kind.to_i32().unwrap(),
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ItemTag { item, tag } => (
				Kind::ItemTag.to_i32().unwrap(),
				item.as_slice(),
				tag.as_str(),
			)
				.pack(w, tuple_depth),

			Key::Clean {
				touched_at,
				kind,
				id,
			} => {
				Kind::Clean.to_i32().unwrap().pack(w, tuple_depth)?;
				touched_at.pack(w, tuple_depth)?;
				kind.to_i32().unwrap().pack(w, tuple_depth)?;
				let id = match &id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				id.as_ref().pack(w, tuple_depth)
			},

			Key::Update { id } => {
				Kind::Update.to_i32().unwrap().pack(w, tuple_depth)?;
				let id = match &id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				id.as_ref().pack(w, tuple_depth)
			},

			Key::UpdateVersion { version, id } => {
				Kind::UpdateVersion.to_i32().unwrap().pack(w, tuple_depth)?;
				version.pack(w, tuple_depth)?;
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
		let (input, kind_value) = i32::unpack(input, tuple_depth)?;
		let kind =
			Kind::from_i32(kind_value).ok_or(fdbt::PackError::Message("invalid kind".into()))?;

		match kind {
			Kind::CacheEntry => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::artifact::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				Ok((input, Key::CacheEntry(id)))
			},

			Kind::Object => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::object::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				Ok((input, Key::Object(id)))
			},

			Kind::Process => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::process::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((input, Key::Process(id)))
			},

			Kind::Tag => {
				let (input, tag): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				Ok((input, Key::Tag(tag)))
			},

			Kind::ObjectChild => {
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

			Kind::ChildObject => {
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

			Kind::ObjectCacheEntry => {
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

			Kind::CacheEntryObject => {
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

			Kind::ProcessChild => {
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

			Kind::ChildProcess => {
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

			Kind::ProcessObject => {
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind_value) = i32::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let kind = ProcessObjectKind::from_i32(kind_value).ok_or(
					fdbt::PackError::Message("invalid process object kind".into()),
				)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let key = Key::ProcessObject {
					process,
					kind,
					object,
				};
				Ok((input, key))
			},

			Kind::ObjectProcess => {
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind_value) = i32::unpack(input, tuple_depth)?;
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let kind = ProcessObjectKind::from_i32(kind_value).ok_or(
					fdbt::PackError::Message("invalid process object kind".into()),
				)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let key = Key::ObjectProcess {
					object,
					kind,
					process,
				};
				Ok((input, key))
			},

			Kind::ItemTag => {
				let (input, item): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, tag): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				Ok((input, Key::ItemTag { item, tag }))
			},

			Kind::Clean => {
				let (input, touched_at): (_, i64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind_value): (_, i32) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let kind = ItemKind::from_i32(kind_value)
					.ok_or(fdbt::PackError::Message("invalid item kind".into()))?;
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let id = match kind {
					ItemKind::CacheEntry | ItemKind::Object => {
						let id = tg::object::Id::try_from(id)
							.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
						tg::Either::Left(id)
					},
					ItemKind::Process => {
						let id = tg::process::Id::try_from(id)
							.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
						tg::Either::Right(id)
					},
				};
				let key = Key::Clean {
					touched_at,
					kind,
					id,
				};
				Ok((input, key))
			},

			Kind::Update => {
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

			Kind::UpdateVersion => {
				let (input, version): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
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
				Ok((input, Key::UpdateVersion { version, id }))
			},
		}
	}
}
