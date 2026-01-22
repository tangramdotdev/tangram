use {
	crate::{CleanOutput, ProcessObjectKind, PutArg},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	std::path::PathBuf,
	tangram_client::prelude::*,
};

mod clean;
mod delete;
mod get;
mod put;
mod queue;
mod touch;

#[derive(Clone, Debug)]
pub struct Config {
	pub map_size: usize,
	pub path: PathBuf,
}

pub struct Index {
	db: Db,
	env: lmdb::Env,
	sender: RequestSender,
}

type Db = lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>;

type RequestSender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;
type RequestReceiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;

#[derive(Clone)]
enum Request {
	Clean {
		max_touched_at: i64,
		batch_size: usize,
	},
	DeleteTags(Vec<String>),
	Put(PutArg),
	TouchObjects {
		ids: Vec<tg::object::Id>,
		touched_at: i64,
	},
	TouchProcesses {
		ids: Vec<tg::process::Id>,
		touched_at: i64,
	},
}

#[derive(Clone)]
enum Response {
	Unit,
	Objects(Vec<Option<crate::Object>>),
	Processes(Vec<Option<crate::Process>>),
	CleanOutput(CleanOutput),
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
	ProcessChild = 7,
	ChildProcess = 8,
	ProcessObject = 9,
	ObjectProcess = 10,
	ItemTag = 11,
}

#[derive(Debug)]
enum Key<'a> {
	CacheEntry(&'a tg::artifact::Id),
	Object(&'a tg::object::Id),
	Process(&'a tg::process::Id),
	Tag(&'a str),
	ObjectChild {
		object: &'a tg::object::Id,
		child: &'a tg::object::Id,
	},
	ChildObject {
		child: &'a tg::object::Id,
		object: &'a tg::object::Id,
	},
	ObjectCacheEntry {
		cache_entry: &'a tg::artifact::Id,
		object: &'a tg::object::Id,
	},
	ProcessChild {
		process: &'a tg::process::Id,
		child: &'a tg::process::Id,
	},
	ChildProcess {
		child: &'a tg::process::Id,
		parent: &'a tg::process::Id,
	},
	ProcessObject {
		process: &'a tg::process::Id,
		object: &'a tg::object::Id,
		kind: ProcessObjectKind,
	},
	ObjectProcess {
		object: &'a tg::object::Id,
		process: &'a tg::process::Id,
		kind: ProcessObjectKind,
	},
	ItemTag {
		item: &'a [u8],
		tag: &'a str,
	},
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

		// Create the task thread.
		let (sender, receiver) = tokio::sync::mpsc::channel(256);
		std::thread::spawn({
			let env = env.clone();
			move || Self::task(&env, &db, receiver)
		});

		Ok(Self { db, env, sender })
	}

	fn task(env: &lmdb::Env, db: &Db, mut receiver: RequestReceiver) {
		while let Some((request, sender)) = receiver.blocking_recv() {
			let mut requests = vec![request];
			let mut senders = vec![sender];
			while let Ok((request, sender)) = receiver.try_recv() {
				requests.push(request);
				senders.push(sender);
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
					Request::TouchObjects { ids, touched_at } => {
						Self::task_touch_objects(db, &mut transaction, &ids, touched_at)
							.map(Response::Objects)
					},
					Request::TouchProcesses { ids, touched_at } => {
						Self::task_touch_processes(db, &mut transaction, &ids, touched_at)
							.map(Response::Processes)
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

	async fn delete_tags(&self, tags: &[String]) -> tg::Result<()> {
		self.delete_tags(tags).await
	}

	async fn queue(&self, batch_size: usize) -> tg::Result<usize> {
		self.queue(batch_size).await
	}

	async fn get_transaction_id(&self) -> tg::Result<u64> {
		self.get_transaction_id().await
	}

	async fn get_queue_size(&self, transaction_id: u64) -> tg::Result<u64> {
		self.get_queue_size(transaction_id).await
	}

	async fn clean(&self, max_touched_at: i64, batch_size: usize) -> tg::Result<CleanOutput> {
		self.clean(max_touched_at, batch_size).await
	}

	async fn sync(&self) -> tg::Result<()> {
		self.sync().await
	}
}

impl fdbt::TuplePack for Key<'_> {
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
			Key::Tag(tag) => (Kind::Tag.to_i32().unwrap(), *tag).pack(w, tuple_depth),
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
				cache_entry,
				object,
			} => (
				Kind::ObjectCacheEntry.to_i32().unwrap(),
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
				object,
				kind,
			} => (
				Kind::ProcessObject.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				object.to_bytes().as_ref(),
				kind.to_i32().unwrap(),
			)
				.pack(w, tuple_depth),
			Key::ObjectProcess {
				object,
				process,
				kind,
			} => (
				Kind::ObjectProcess.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				process.to_bytes().as_ref(),
				kind.to_i32().unwrap(),
			)
				.pack(w, tuple_depth),
			Key::ItemTag { item, tag } => {
				(Kind::ItemTag.to_i32().unwrap(), *item, *tag).pack(w, tuple_depth)
			},
		}
	}
}
