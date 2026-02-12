use {
	crate::{
		CachePointer, DeleteObjectArg, DeleteProcessLogArg, Object, ProcessLogEntry, PutObjectArg,
		PutProcessLogArg, ReadProcessLogArg,
	},
	bytes::Bytes,
	foundationdb_tuple::TuplePack as _,
	heed as lmdb,
	num::ToPrimitive as _,
	std::borrow::Cow,
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub struct Config {
	pub map_size: usize,
	pub path: std::path::PathBuf,
}

pub struct Store {
	db: Db,
	env: lmdb::Env,
	sender: RequestSender,
}

pub type Db = lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>;

type RequestSender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;
type RequestReceiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<()>>;
type _ResponseReceiver = tokio::sync::oneshot::Receiver<tg::Result<()>>;

enum Request {
	Object(ObjectRequest),
	ProcessLog(ProcessLogRequest),
}

enum ObjectRequest {
	Put(PutObject),
	PutBatch(Vec<PutObject>),
	Delete(DeleteObject),
	DeleteBatch(Vec<DeleteObject>),
}

struct PutObject {
	bytes: Option<Bytes>,
	cache_pointer: Option<CachePointer>,
	id: tg::object::Id,
	touched_at: i64,
}

struct DeleteObject {
	id: tg::object::Id,
	now: i64,
	ttl: u64,
}

enum ProcessLogRequest {
	Put(PutProcessLog),
	Delete(DeleteProcessLog),
}

struct PutProcessLog {
	bytes: Bytes,
	id: tg::process::Id,
	stream: tg::process::log::Stream,
	timestamp: i64,
}

struct DeleteProcessLog {
	id: tg::process::Id,
}

#[derive(Debug)]
enum Key<'a> {
	Object(&'a tg::object::Id),
	ProcessLogEntry(&'a tg::process::Id, u64),
	ProcessLogStreamPosition(&'a tg::process::Id, tg::process::log::Stream, u64),
}

impl Store {
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

		// Create the thread.
		let (sender, receiver) = tokio::sync::mpsc::channel(256);
		std::thread::spawn({
			let env = env.clone();
			move || Self::task(&env, &db, receiver)
		});

		Ok(Self { db, env, sender })
	}

	#[must_use]
	pub fn db(&self) -> Db {
		self.db
	}

	#[must_use]
	pub fn env(&self) -> &lmdb::Env {
		&self.env
	}

	pub fn try_get_object_sync(&self, id: &tg::object::Id) -> tg::Result<Option<Object<'static>>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		self.try_get_object_with_transaction(&transaction, id)
	}

	pub fn try_get_object_batch_sync(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<Object<'static>>>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let mut outputs = Vec::with_capacity(ids.len());
		for id in ids {
			let key = Key::Object(id);
			let value = self
				.db
				.get(&transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
				.and_then(|bytes| Object::deserialize(bytes).ok());
			outputs.push(value);
		}
		Ok(outputs)
	}

	pub fn try_get_object_data_sync(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		self.try_get_object_data_with_transaction(&transaction, id)
	}

	pub fn try_get_object_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Option<Object<'static>>> {
		let key = Key::Object(id);
		let key_bytes = key.pack_to_vec();
		let Some(bytes) = self
			.db
			.get(transaction, &key_bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
		else {
			return Ok(None);
		};
		let value = Object::deserialize(bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to deserialize the object"))?;
		Ok(Some(value))
	}

	pub fn try_get_object_data_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let kind = id.kind();
		let key = Key::Object(id);
		let Some(raw_bytes) = self
			.db
			.get(transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
		else {
			return Ok(None);
		};
		let value = Object::deserialize(raw_bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to deserialize the object"))?;
		let Some(bytes) = value.bytes else {
			return Ok(None);
		};
		let size = bytes.len().to_u64().unwrap();
		let data = tg::object::Data::deserialize(kind, &*bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to deserialize the object data"))?;
		Ok(Some((size, data)))
	}

	fn task(env: &lmdb::Env, db: &Db, mut receiver: RequestReceiver) {
		while let Some((request, sender)) = receiver.blocking_recv() {
			let mut requests = vec![request];
			let mut senders = vec![sender];
			while let Ok((request, sender)) = receiver.try_recv() {
				requests.push(request);
				senders.push(sender);
			}
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
			let mut responses = vec![];
			for request in requests {
				match request {
					Request::Object(ObjectRequest::Put(request)) => {
						let result = Self::task_put_object(env, db, &mut transaction, request);
						responses.push(result);
					},
					Request::Object(ObjectRequest::PutBatch(requests)) => {
						for request in requests {
							let result = Self::task_put_object(env, db, &mut transaction, request);
							responses.push(result);
						}
					},
					Request::Object(ObjectRequest::Delete(request)) => {
						let result = Self::task_delete_object(env, db, &mut transaction, request);
						responses.push(result);
					},
					Request::Object(ObjectRequest::DeleteBatch(requests)) => {
						for request in requests {
							let result =
								Self::task_delete_object(env, db, &mut transaction, request);
							responses.push(result);
						}
					},
					Request::ProcessLog(ProcessLogRequest::Put(request)) => {
						let result = Self::task_put_process_log(env, db, &mut transaction, request);
						responses.push(result);
					},
					Request::ProcessLog(ProcessLogRequest::Delete(request)) => {
						let result =
							Self::task_delete_process_log(env, db, &mut transaction, request);
						responses.push(result);
					},
				}
			}
			let result = transaction
				.commit()
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"));
			if let Err(error) = result {
				for sender in senders {
					sender.send(Err(error.clone())).ok();
				}
				continue;
			}
			for (sender, result) in std::iter::zip(senders, responses) {
				sender.send(result).ok();
			}
		}
	}

	fn task_put_object(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: PutObject,
	) -> tg::Result<()> {
		let id = &request.id;
		let key = Key::Object(id);
		let key_bytes = key.pack_to_vec();

		// Read existing value if any.
		let existing = db
			.get(transaction, &key_bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
			.and_then(|bytes| Object::deserialize(bytes).ok());

		// Determine bytes: use existing if present (NO_OVERWRITE semantics), otherwise use request.
		let bytes = existing
			.as_ref()
			.and_then(|entry| entry.bytes.clone())
			.or(request.bytes.map(|bytes| Cow::Owned(bytes.to_vec())));

		// Merge cache_pointer: use request if provided, otherwise keep existing.
		let cache_pointer = request
			.cache_pointer
			.or_else(|| existing.and_then(|entry| entry.cache_pointer));

		let value = Object {
			bytes,
			touched_at: request.touched_at,
			cache_pointer,
		};
		let value_bytes = value.serialize().unwrap();
		db.put(transaction, &key_bytes, &value_bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to put the object"))?;
		Ok(())
	}

	#[expect(clippy::needless_pass_by_value)]
	fn task_delete_object(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: DeleteObject,
	) -> tg::Result<()> {
		let id = &request.id;
		let key = Key::Object(id);
		let key_bytes = key.pack_to_vec();

		let Some(bytes) = db
			.get(transaction, &key_bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
		else {
			return Ok(());
		};
		let value = Object::deserialize(bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to deserialize the object"))?;

		if request.now - value.touched_at >= request.ttl.to_i64().unwrap() {
			db.delete(transaction, &key_bytes)
				.map_err(|source| tg::error!(!source, %id, "failed to delete the object"))?;
		}
		Ok(())
	}

	#[allow(clippy::needless_pass_by_value)]
	fn task_put_process_log(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: PutProcessLog,
	) -> tg::Result<()> {
		let id = &request.id;

		// Get the current combined position by finding the last entry.
		let key = Key::ProcessLogEntry(id, u64::MAX);
		let position = db
			.get_lower_than_or_equal_to(transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, "failed to get the last combined entry"))?
			.and_then(|(_, value)| {
				tangram_serialize::from_slice::<ProcessLogEntry>(value)
					.ok()
					.map(|entry| entry.position + entry.bytes.len().to_u64().unwrap())
			})
			.unwrap_or(0);

		// Get the current stream position by finding the last stream entry.
		let stream_start_key = Key::ProcessLogStreamPosition(id, request.stream, 0);
		let stream_start = stream_start_key.pack_to_vec();
		let key = Key::ProcessLogStreamPosition(id, request.stream, u64::MAX);
		let stream_position = db
			.get_lower_than_or_equal_to(transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, "failed to get the last stream entry"))?
			.and_then(|(key, value)| {
				// Verify the key is actually for this stream, not a different one.
				if key < stream_start.as_slice() {
					return None;
				}
				u64::from_le_bytes(value.try_into().ok()?).into()
			})
			.map_or(0, |combined_pos| {
				// Read the entry at this combined position to get stream_position.
				let key = Key::ProcessLogEntry(id, combined_pos);
				db.get(transaction, &key.pack_to_vec())
					.ok()
					.flatten()
					.and_then(|value| tangram_serialize::from_slice::<ProcessLogEntry>(value).ok())
					.map_or(0, |entry| {
						entry.stream_position + entry.bytes.len().to_u64().unwrap()
					})
			});

		// Create the log entry.
		let entry = ProcessLogEntry {
			bytes: Cow::Owned(request.bytes.to_vec()),
			position,
			stream_position,
			stream: request.stream,
			timestamp: request.timestamp,
		};

		// Store the primary entry keyed by combined position.
		let key = Key::ProcessLogEntry(id, position);
		let val = tangram_serialize::to_vec(&entry).unwrap();
		db.put(transaction, &key.pack_to_vec(), &val)
			.map_err(|source| tg::error!(!source, "failed to store the log entry"))?;

		// Store the stream index pointer.
		let key = Key::ProcessLogStreamPosition(id, request.stream, stream_position);
		let val = position.to_le_bytes();
		db.put(transaction, &key.pack_to_vec(), &val)
			.map_err(|source| tg::error!(!source, "failed to store the stream index"))?;

		Ok(())
	}

	#[expect(clippy::needless_pass_by_value)]
	fn task_delete_process_log(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: DeleteProcessLog,
	) -> tg::Result<()> {
		let id = &request.id;

		// Helper to delete all keys in a range.
		let delete_range = |db: &Db,
		                    transaction: &mut lmdb::RwTxn<'_>,
		                    start: Vec<u8>,
		                    end: Vec<u8>|
		 -> tg::Result<()> {
			let mut keys_to_delete = Vec::new();
			let mut current_key = start.clone();
			loop {
				let Some((key, _)) = db
					.get_greater_than_or_equal_to(transaction, &current_key)
					.map_err(|source| tg::error!(!source, "failed to iterate log entries"))?
				else {
					break;
				};
				if key > end.as_slice() {
					break;
				}
				keys_to_delete.push(key.to_vec());
				current_key = key.to_vec();
				current_key.push(0);
			}
			for key in keys_to_delete {
				db.delete(transaction, &key)
					.map_err(|source| tg::error!(!source, "failed to delete the log entry"))?;
			}
			Ok(())
		};

		// Delete all log entries.
		let start_key = Key::ProcessLogEntry(id, 0);
		let end_key = Key::ProcessLogEntry(id, u64::MAX);
		delete_range(
			db,
			transaction,
			start_key.pack_to_vec(),
			end_key.pack_to_vec(),
		)?;

		// Delete all stream position entries.
		for stream in [
			tg::process::log::Stream::Stdout,
			tg::process::log::Stream::Stderr,
		] {
			let start_key = Key::ProcessLogStreamPosition(id, stream, 0);
			let end_key = Key::ProcessLogStreamPosition(id, stream, u64::MAX);
			delete_range(
				db,
				transaction,
				start_key.pack_to_vec(),
				end_key.pack_to_vec(),
			)?;
		}

		Ok(())
	}

	pub fn put_object_sync(&self, arg: PutObjectArg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let request = PutObject {
			bytes: arg.bytes,
			cache_pointer: arg.cache_pointer,
			id: arg.id,
			touched_at: arg.touched_at,
		};
		Self::task_put_object(&self.env, &self.db, &mut transaction, request)?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn put_object_batch_sync(&self, args: Vec<PutObjectArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		for arg in args {
			let request = PutObject {
				bytes: arg.bytes,
				cache_pointer: arg.cache_pointer,
				id: arg.id,
				touched_at: arg.touched_at,
			};
			Self::task_put_object(&self.env, &self.db, &mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn delete_object_sync(&self, arg: DeleteObjectArg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let request = DeleteObject {
			id: arg.id,
			now: arg.now,
			ttl: arg.ttl,
		};
		Self::task_delete_object(&self.env, &self.db, &mut transaction, request)?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn delete_object_batch_sync(&self, args: Vec<DeleteObjectArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		for arg in args {
			let request = DeleteObject {
				id: arg.id,
				now: arg.now,
				ttl: arg.ttl,
			};
			Self::task_delete_object(&self.env, &self.db, &mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn flush_sync(&self) -> tg::Result<()> {
		self.env
			.force_sync()
			.map_err(|source| tg::error!(!source, "failed to sync"))?;
		Ok(())
	}
}

impl crate::Store for Store {
	async fn try_get_object(&self, id: &tg::object::Id) -> tg::Result<Option<Object<'static>>> {
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let id = id.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let key = Key::Object(&id);
				let Some(raw_bytes) = db
					.get(&transaction, &key.pack_to_vec())
					.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
				else {
					return Ok(None);
				};
				let value = Object::deserialize(raw_bytes).map_err(
					|source| tg::error!(!source, %id, "failed to deserialize the object"),
				)?;
				Ok(Some(value))
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}

	async fn try_get_object_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<Object<'static>>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let ids = ids.to_owned();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let mut outputs = Vec::with_capacity(ids.len());
				for id in &ids {
					let key = Key::Object(id);
					let value = db
						.get(&transaction, &key.pack_to_vec())
						.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
						.and_then(|bytes| Object::deserialize(bytes).ok());
					outputs.push(value);
				}
				Ok(outputs)
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}

	async fn put_object(&self, arg: PutObjectArg) -> tg::Result<()> {
		let id = arg.id.clone();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Object(ObjectRequest::Put(PutObject {
			bytes: arg.bytes,
			cache_pointer: arg.cache_pointer,
			id: arg.id,
			touched_at: arg.touched_at,
		}));
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!(%id, "the task panicked"))?
	}

	async fn put_object_batch(&self, args: Vec<PutObjectArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Object(ObjectRequest::PutBatch(
			args.into_iter()
				.map(|arg| PutObject {
					bytes: arg.bytes,
					cache_pointer: arg.cache_pointer,
					id: arg.id,
					touched_at: arg.touched_at,
				})
				.collect(),
		));
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))?
	}

	async fn delete_object(&self, arg: DeleteObjectArg) -> tg::Result<()> {
		let id = arg.id.clone();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Object(ObjectRequest::Delete(DeleteObject {
			id: arg.id,
			now: arg.now,
			ttl: arg.ttl,
		}));
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!(%id, "the task panicked"))?
	}

	async fn delete_object_batch(&self, args: Vec<DeleteObjectArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Object(ObjectRequest::DeleteBatch(
			args.into_iter()
				.map(|arg| DeleteObject {
					id: arg.id,
					now: arg.now,
					ttl: arg.ttl,
				})
				.collect(),
		));
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))?
	}

	async fn try_read_process_log(
		&self,
		arg: ReadProcessLogArg,
	) -> tg::Result<Vec<ProcessLogEntry<'static>>> {
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let id = &arg.process;

				// Find the starting combined position.
				let start_position = if let Some(stream) = arg.stream {
					// For stream-specific reads, look up the stream index.
					let stream_start_key = Key::ProcessLogStreamPosition(id, stream, 0);
					let stream_start = stream_start_key.pack_to_vec();
					let key = Key::ProcessLogStreamPosition(id, stream, arg.position);
					let key = key.pack_to_vec();
					let result = if arg.position == 0 {
						db.get(&transaction, &key).map_err(|source| {
							tg::error!(!source, "failed to get the stream index")
						})?
					} else {
						db.get_lower_than_or_equal_to(&transaction, &key)
							.map_err(|source| {
								tg::error!(!source, "failed to get the stream index")
							})?
							.and_then(|(key, value)| {
								// Verify the key is for this stream.
								if key < stream_start.as_slice() {
									return None;
								}
								Some(value)
							})
					};
					let Some(bytes) = result else {
						return Ok(Vec::new());
					};
					u64::from_le_bytes(
						bytes
							.try_into()
							.map_err(|_| tg::error!("expected 8 bytes"))?,
					)
				} else {
					// For combined reads, the position is the combined position.
					arg.position
				};

				// Seek to the starting entry.
				let key = Key::ProcessLogEntry(id, start_position);
				let key = key.pack_to_vec();
				let result = if start_position == 0 {
					db.get(&transaction, &key)
						.map_err(|source| tg::error!(!source, "failed to get the log entry"))?
						.map(|value| (key.clone(), value))
				} else {
					db.get_lower_than_or_equal_to(&transaction, &key)
						.map_err(|source| tg::error!(!source, "failed to get the log entry"))?
						.map(|(key, value)| (key.to_vec(), value))
				};
				let Some((mut current_key, first_value)) = result else {
					return Ok(Vec::new());
				};

				// Create an end key to bound iteration within combined entries.
				let end_key = Key::ProcessLogEntry(id, u64::MAX);
				let end_key = end_key.pack_to_vec();

				let mut remaining = arg.length;
				let mut output = Vec::new();
				let mut current: Option<ProcessLogEntry> = None;

				// Process the first entry.
				let mut value = Some(first_value);

				while remaining > 0 {
					let val = if let Some(value) = value.take() {
						value
					} else {
						// Get the next entry.
						let Some(result) = db
							.get_greater_than(&transaction, &current_key)
							.map_err(|source| {
								tg::error!(!source, "failed to get the next entry")
							})?
						else {
							break;
						};
						// Check that we're still within combined entries.
						if result.0 > end_key.as_slice() {
							break;
						}
						current_key = result.0.to_vec();
						result.1
					};

					let chunk = tangram_serialize::from_slice::<ProcessLogEntry>(val).map_err(
						|source| tg::error!(!source, "failed to deserialize the log entry"),
					)?;

					// Skip chunks that do not match the stream filter.
					if arg.stream.is_some_and(|stream| stream != chunk.stream) {
						continue;
					}

					// Get the position based on stream filter.
					let position = if arg.stream.is_some() {
						chunk.stream_position
					} else {
						chunk.position
					};

					let offset = arg.position.saturating_sub(position);

					let available = chunk.bytes.len().to_u64().unwrap().saturating_sub(offset);
					let take = remaining.min(available);

					let bytes: Cow<'_, [u8]> =
						if offset > 0 || take < chunk.bytes.len().to_u64().unwrap() {
							let start = offset.to_usize().unwrap();
							let end = (offset + take).to_usize().unwrap();
							Cow::Owned(chunk.bytes[start..end].to_vec())
						} else {
							chunk.bytes.clone()
						};

					// Combine sequential entries from the same stream.
					if let Some(ref mut entry) = current {
						if entry.stream == chunk.stream {
							let mut combined = entry.bytes.to_vec();
							combined.extend_from_slice(&bytes);
							entry.bytes = Cow::Owned(combined);
						} else {
							output.push(current.take().unwrap());
							current = Some(ProcessLogEntry {
								bytes,
								position: chunk.position + offset,
								stream_position: chunk.stream_position + offset,
								stream: chunk.stream,
								timestamp: chunk.timestamp,
							});
						}
					} else {
						current = Some(ProcessLogEntry {
							bytes,
							position: chunk.position + offset,
							stream_position: chunk.stream_position + offset,
							stream: chunk.stream,
							timestamp: chunk.timestamp,
						});
					}

					remaining -= take;
				}

				// Push the last entry if any.
				if let Some(entry) = current {
					output.push(entry);
				}

				// Convert all entries to owned.
				let output = output
					.into_iter()
					.map(ProcessLogEntry::into_static)
					.collect();
				Ok(output)
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}

	async fn try_get_process_log_length(
		&self,
		id: &tg::process::Id,
		stream: Option<tg::process::log::Stream>,
	) -> tg::Result<Option<u64>> {
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let id = id.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

				let length = if let Some(stream) = stream {
					// For stream-specific queries, find the last stream position entry.
					let start_key = Key::ProcessLogStreamPosition(&id, stream, 0);
					let start = start_key.pack_to_vec();
					let key = Key::ProcessLogStreamPosition(&id, stream, u64::MAX);
					let Some((found_key, value)) = db
						.get_lower_than_or_equal_to(&transaction, &key.pack_to_vec())
						.map_err(|source| tg::error!(!source, "failed to get the last entry"))?
					else {
						return Ok(None);
					};

					// Verify the key is for the stream we're querying.
					if found_key < start.as_slice() {
						return Ok(None);
					}

					// The value is a position pointer. Read that entry to get stream_position.
					let position = u64::from_le_bytes(
						value
							.try_into()
							.map_err(|_| tg::error!("expected 8 bytes"))?,
					);
					let entry_key = Key::ProcessLogEntry(&id, position);
					let Some(entry_bytes) = db
						.get(&transaction, &entry_key.pack_to_vec())
						.map_err(|source| tg::error!(!source, "failed to get the log entry"))?
					else {
						return Ok(None);
					};
					let entry = tangram_serialize::from_slice::<ProcessLogEntry>(entry_bytes)
						.map_err(|source| tg::error!(!source, "failed to deserialize entry"))?;
					entry.stream_position + entry.bytes.len().to_u64().unwrap()
				} else {
					// For combined queries, find the last log entry.
					let start_key = Key::ProcessLogEntry(&id, 0);
					let start = start_key.pack_to_vec();
					let key = Key::ProcessLogEntry(&id, u64::MAX);
					let Some((found_key, value)) = db
						.get_lower_than_or_equal_to(&transaction, &key.pack_to_vec())
						.map_err(|source| tg::error!(!source, "failed to get the last entry"))?
					else {
						return Ok(None);
					};

					// Verify the key is for this process.
					if found_key < start.as_slice() {
						return Ok(None);
					}

					let entry = tangram_serialize::from_slice::<ProcessLogEntry>(value)
						.map_err(|source| tg::error!(!source, "failed to deserialize entry"))?;
					entry.position + entry.bytes.len().to_u64().unwrap()
				};

				Ok(Some(length))
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}

	async fn put_process_log(&self, arg: PutProcessLogArg) -> tg::Result<()> {
		if arg.bytes.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::ProcessLog(ProcessLogRequest::Put(PutProcessLog {
			bytes: arg.bytes,
			id: arg.process,
			stream: arg.stream,
			timestamp: arg.timestamp,
		}));
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))?
	}

	async fn delete_process_log(&self, arg: DeleteProcessLogArg) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::ProcessLog(ProcessLogRequest::Delete(DeleteProcessLog {
			id: arg.process,
		}));
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))?
	}

	async fn flush(&self) -> tg::Result<()> {
		tokio::task::spawn_blocking({
			let env = self.env.clone();
			move || {
				env.force_sync()
					.map_err(|source| tg::error!(!source, "failed to sync"))
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}
}

impl foundationdb_tuple::TuplePack for Key<'_> {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: foundationdb_tuple::TupleDepth,
	) -> std::io::Result<foundationdb_tuple::VersionstampOffset> {
		match self {
			Key::Object(id) => (0, id.to_bytes().as_ref()).pack(w, tuple_depth),
			Key::ProcessLogEntry(id, position) => {
				(1, id.to_bytes().as_ref(), 0, *position).pack(w, tuple_depth)
			},
			Key::ProcessLogStreamPosition(id, tg::process::log::Stream::Stdout, position) => {
				(1, id.to_bytes().as_ref(), 1, *position).pack(w, tuple_depth)
			},
			Key::ProcessLogStreamPosition(id, tg::process::log::Stream::Stderr, position) => {
				(1, id.to_bytes().as_ref(), 2, *position).pack(w, tuple_depth)
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::Store as _;

	fn collect_bytes(entries: Vec<ProcessLogEntry>) -> Bytes {
		entries
			.into_iter()
			.flat_map(|entry| entry.bytes.to_vec())
			.collect::<Vec<_>>()
			.into()
	}

	#[tokio::test]
	async fn test_put_and_read_log_single_chunk() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		let process = tg::process::Id::new();

		// Insert a single chunk.
		store
			.put_process_log(PutProcessLogArg {
				bytes: Bytes::from("hello world"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1000,
			})
			.await
			.unwrap();

		// Read the entire chunk.
		let result = store
			.try_read_process_log(ReadProcessLogArg {
				process: process.clone(),
				position: 0,
				length: 11,
				stream: Some(tg::process::log::Stream::Stdout),
			})
			.await
			.unwrap();
		assert_eq!(collect_bytes(result), Bytes::from("hello world"));

		// Read a subset of the chunk.
		let result = store
			.try_read_process_log(ReadProcessLogArg {
				process: process.clone(),
				position: 6,
				length: 5,
				stream: Some(tg::process::log::Stream::Stdout),
			})
			.await
			.unwrap();
		assert_eq!(collect_bytes(result), Bytes::from("world"));
	}

	#[tokio::test]
	async fn test_put_and_read_log_multiple_chunks() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		let process = tg::process::Id::new();

		// Insert multiple chunks.
		store
			.put_process_log(PutProcessLogArg {
				bytes: Bytes::from("hello"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1000,
			})
			.await
			.unwrap();
		store
			.put_process_log(PutProcessLogArg {
				bytes: Bytes::from(" "),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1001,
			})
			.await
			.unwrap();
		store
			.put_process_log(PutProcessLogArg {
				bytes: Bytes::from("world"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1002,
			})
			.await
			.unwrap();

		// Read across all chunks.
		let result = store
			.try_read_process_log(ReadProcessLogArg {
				process: process.clone(),
				position: 0,
				length: 11,
				stream: Some(tg::process::log::Stream::Stdout),
			})
			.await
			.unwrap();
		assert_eq!(collect_bytes(result), Bytes::from("hello world"));
	}

	#[tokio::test]
	async fn test_read_log_across_chunk_boundaries() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		let process = tg::process::Id::new();

		// Insert chunks: "AAAA" (0-3), "BBBB" (4-7), "CCCC" (8-11).
		store
			.put_process_log(PutProcessLogArg {
				bytes: Bytes::from("AAAA"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1000,
			})
			.await
			.unwrap();
		store
			.put_process_log(PutProcessLogArg {
				bytes: Bytes::from("BBBB"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1001,
			})
			.await
			.unwrap();
		store
			.put_process_log(PutProcessLogArg {
				bytes: Bytes::from("CCCC"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1002,
			})
			.await
			.unwrap();

		// Read starting in the middle of the first chunk, across into the second.
		let result = store
			.try_read_process_log(ReadProcessLogArg {
				process: process.clone(),
				position: 2,
				length: 4,
				stream: Some(tg::process::log::Stream::Stdout),
			})
			.await
			.unwrap();
		assert_eq!(collect_bytes(result), Bytes::from("AABB"));

		// Read starting in the middle of the second chunk, across into the third.
		let result = store
			.try_read_process_log(ReadProcessLogArg {
				process: process.clone(),
				position: 6,
				length: 4,
				stream: Some(tg::process::log::Stream::Stdout),
			})
			.await
			.unwrap();
		assert_eq!(collect_bytes(result), Bytes::from("BBCC"));

		// Read spanning all three chunks.
		let result = store
			.try_read_process_log(ReadProcessLogArg {
				process: process.clone(),
				position: 2,
				length: 8,
				stream: Some(tg::process::log::Stream::Stdout),
			})
			.await
			.unwrap();
		assert_eq!(collect_bytes(result), Bytes::from("AABBBBCC"));
	}

	#[tokio::test]
	async fn test_read_log_combined_stream() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		let process = tg::process::Id::new();

		// Insert interleaved stdout and stderr chunks.
		store
			.put_process_log(PutProcessLogArg {
				bytes: Bytes::from("out1"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1000,
			})
			.await
			.unwrap();
		store
			.put_process_log(PutProcessLogArg {
				bytes: Bytes::from("err1"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stderr,
				timestamp: 1001,
			})
			.await
			.unwrap();
		store
			.put_process_log(PutProcessLogArg {
				bytes: Bytes::from("out2"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1002,
			})
			.await
			.unwrap();

		// Read the combined stream.
		let result = store
			.try_read_process_log(ReadProcessLogArg {
				process: process.clone(),
				position: 0,
				length: 12,
				stream: None,
			})
			.await
			.unwrap();
		assert_eq!(collect_bytes(result), Bytes::from("out1err1out2"));

		// Read only stdout.
		let result = store
			.try_read_process_log(ReadProcessLogArg {
				process: process.clone(),
				position: 0,
				length: 8,
				stream: Some(tg::process::log::Stream::Stdout),
			})
			.await
			.unwrap();
		assert_eq!(collect_bytes(result), Bytes::from("out1out2"));

		// Read only stderr.
		let result = store
			.try_read_process_log(ReadProcessLogArg {
				process: process.clone(),
				position: 0,
				length: 4,
				stream: Some(tg::process::log::Stream::Stderr),
			})
			.await
			.unwrap();
		assert_eq!(collect_bytes(result), Bytes::from("err1"));
	}

	#[tokio::test]
	async fn test_delete_log_removes_all_chunks() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		let process = tg::process::Id::new();

		// Insert some chunks.
		store
			.put_process_log(PutProcessLogArg {
				bytes: Bytes::from("hello"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1000,
			})
			.await
			.unwrap();
		store
			.put_process_log(PutProcessLogArg {
				bytes: Bytes::from("world"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stderr,
				timestamp: 1001,
			})
			.await
			.unwrap();

		// Verify the log exists.
		let result = store
			.try_read_process_log(ReadProcessLogArg {
				process: process.clone(),
				position: 0,
				length: 10,
				stream: None,
			})
			.await
			.unwrap();
		assert_eq!(collect_bytes(result), Bytes::from("helloworld"));

		// Delete the log.
		store
			.delete_process_log(DeleteProcessLogArg {
				process: process.clone(),
			})
			.await
			.unwrap();

		// Verify the log no longer exists.
		let result = store
			.try_read_process_log(ReadProcessLogArg {
				process: process.clone(),
				position: 0,
				length: 10,
				stream: None,
			})
			.await
			.unwrap();
		assert!(result.is_empty());
	}

	#[tokio::test]
	async fn test_try_get_log_length() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		let process = tg::process::Id::new();

		// Insert chunks.
		store
			.put_process_log(PutProcessLogArg {
				bytes: Bytes::from("hello"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1000,
			})
			.await
			.unwrap();
		store
			.put_process_log(PutProcessLogArg {
				bytes: Bytes::from("err"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stderr,
				timestamp: 1001,
			})
			.await
			.unwrap();
		store
			.put_process_log(PutProcessLogArg {
				bytes: Bytes::from("world"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1002,
			})
			.await
			.unwrap();

		// Check lengths.
		assert_eq!(
			store
				.try_get_process_log_length(&process, None)
				.await
				.unwrap(),
			Some(13)
		); // 5 + 3 + 5
		assert_eq!(
			store
				.try_get_process_log_length(&process, Some(tg::process::log::Stream::Stdout))
				.await
				.unwrap(),
			Some(10)
		); // 5 + 5
		assert_eq!(
			store
				.try_get_process_log_length(&process, Some(tg::process::log::Stream::Stderr))
				.await
				.unwrap(),
			Some(3)
		);
	}

	#[tokio::test]
	async fn test_read_log_at_end_returns_empty() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		let process = tg::process::Id::new();

		// Insert a chunk.
		store
			.put_process_log(PutProcessLogArg {
				bytes: Bytes::from("hello"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1000,
			})
			.await
			.unwrap();

		// Read at the end position.
		let result = store
			.try_read_process_log(ReadProcessLogArg {
				process: process.clone(),
				position: 5,
				length: 10,
				stream: Some(tg::process::log::Stream::Stdout),
			})
			.await
			.unwrap();
		assert_eq!(collect_bytes(result), Bytes::new());
	}

	#[tokio::test]
	async fn test_read_log_nonexistent_process() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		let process = tg::process::Id::new();

		// Try to read from a process that does not exist.
		let result = store
			.try_read_process_log(ReadProcessLogArg {
				process,
				position: 0,
				length: 10,
				stream: None,
			})
			.await
			.unwrap();
		assert!(result.is_empty());
	}

	#[tokio::test]
	async fn test_put_and_get_object() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();

		// Create object data and ID.
		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		// Put the object.
		store
			.put_object(crate::PutObjectArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				touched_at: 12345,
			})
			.await
			.unwrap();

		// Get the object.
		let result = store.try_get_object(&id).await.unwrap();
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}

	#[tokio::test]
	async fn test_put_object_without_bytes_then_with_bytes() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();

		// Create object data and ID.
		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		// Put without bytes first (should not store anything).
		store
			.put_object(crate::PutObjectArg {
				bytes: None,
				cache_pointer: None,
				id: id.clone(),
				touched_at: 12345,
			})
			.await
			.unwrap();

		// Verify object bytes do not exist (object may exist with bytes=None).
		let result = store.try_get_object(&id).await.unwrap();
		assert!(
			result.is_none()
				|| result
					.as_ref()
					.and_then(|object| object.bytes.as_ref())
					.is_none()
		);

		// Put with bytes.
		store
			.put_object(crate::PutObjectArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				touched_at: 12346,
			})
			.await
			.unwrap();

		// Verify object now exists.
		let result = store.try_get_object(&id).await.unwrap();
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}

	#[test]
	fn test_put_and_get_object_sync() {
		// This test mimics what the server does using sync functions.
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();

		// Create object data and ID similar to server's write.rs.
		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		// Put the object using sync function (like server does).
		store
			.put_object_sync(crate::PutObjectArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				touched_at: 12345,
			})
			.unwrap();

		// Get the object using sync function.
		let result = store.try_get_object_sync(&id).unwrap();
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}

	#[tokio::test]
	async fn test_put_batch_and_get_object() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();

		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		store
			.put_object_batch(vec![crate::PutObjectArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				touched_at: 12345,
			}])
			.await
			.unwrap();

		let result = store.try_get_object(&id).await.unwrap();
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}
}
