use {
	crate::{CacheReference, DeleteArg, DeleteLogArg, Error as _, PutArg, PutLogArg, ReadLogArg},
	bytes::Bytes,
	foundationdb_tuple::TuplePack as _,
	heed as lmdb,
	num::ToPrimitive as _,
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

type Db = lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>;

type RequestSender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;
type RequestReceiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<()>>;
type _ResponseReceiver = tokio::sync::oneshot::Receiver<tg::Result<()>>;

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Lmdb(lmdb::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

enum Request {
	Put(Put),
	PutLog(PutLog),
	PutBatch(Vec<Put>),
	Delete(Delete),
	DeleteLog(DeleteLog),
	DeleteBatch(Vec<Delete>),
}

struct Put {
	bytes: Option<Bytes>,
	cache_reference: Option<CacheReference>,
	id: tg::object::Id,
	touched_at: i64,
}

struct PutLog {
	bytes: Bytes,
	id: tg::process::Id,
	stream: tg::process::log::Stream,
	timestamp: i64,
}

struct Delete {
	id: tg::object::Id,
	now: i64,
	ttl: u64,
}

struct DeleteLog {
	id: tg::process::Id,
}

#[derive(Debug)]
struct Key<'a> {
	id: &'a [u8],
	kind: KeyKind,
}

#[derive(Debug)]
enum KeyKind {
	Bytes,
	TouchedAt,
	CacheReference,
	LogPosition(Option<tg::process::log::Stream>),
	Log(Option<tg::process::log::Stream>, u64),
	LogIndex,
	LogBytes(u64),
}

impl Store {
	pub fn new(config: &Config) -> Result<Self, Error> {
		std::fs::OpenOptions::new()
			.create(true)
			.truncate(false)
			.read(true)
			.write(true)
			.open(&config.path)
			.map_err(Error::other)?;
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
					Error::other(
						tg::error!(!source, path = %config.path.display(), "failed to open the lmdb environment"),
					)
				})?
		};
		let mut transaction = env.write_txn().unwrap();
		let db = env
			.create_database(&mut transaction, None)
			.map_err(|source| Error::other(tg::error!(!source, "failed to create the database")))?;
		transaction.commit().map_err(|source| {
			Error::other(tg::error!(!source, "failed to commit the transaction"))
		})?;

		// Create the thread.
		let (sender, receiver) = tokio::sync::mpsc::channel(256);
		std::thread::spawn({
			let env = env.clone();
			move || Self::task(&env, &db, receiver)
		});

		Ok(Self { db, env, sender })
	}

	pub fn try_get_sync(&self, id: &tg::object::Id) -> tg::Result<Option<Bytes>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let id_bytes = id.to_bytes();
		let key = Key {
			id: id_bytes.as_ref(),
			kind: KeyKind::Bytes,
		};
		let Some(bytes) = self
			.db
			.get(&transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
		else {
			return Ok(None);
		};
		let bytes = Bytes::copy_from_slice(bytes);
		Ok::<_, tg::Error>(Some(bytes))
	}

	pub fn try_get_batch_sync(&self, ids: &[tg::object::Id]) -> tg::Result<Vec<Option<Bytes>>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let mut outputs = Vec::with_capacity(ids.len());
		for id in ids {
			let id_bytes = id.to_bytes();
			let key = Key {
				id: id_bytes.as_ref(),
				kind: KeyKind::Bytes,
			};
			let bytes = self
				.db
				.get(&transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
				.map(Bytes::copy_from_slice);
			outputs.push(bytes);
		}
		Ok::<_, tg::Error>(outputs)
	}

	pub fn try_get_object_data_sync(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let transaction = self.env.read_txn().unwrap();
		let kind = id.kind();
		let id_bytes = id.to_bytes();
		let key = Key {
			id: id_bytes.as_ref(),
			kind: KeyKind::Bytes,
		};
		let Some(bytes) = self
			.db
			.get(&transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
		else {
			return Ok(None);
		};
		let size = bytes.len().to_u64().unwrap();
		let data = tg::object::Data::deserialize(kind, bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to deserialize the object"))?;
		Ok(Some((size, data)))
	}

	pub fn try_get_cache_reference_sync(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<CacheReference>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let id_bytes = id.to_bytes();
		let key = Key {
			id: id_bytes.as_ref(),
			kind: KeyKind::CacheReference,
		};
		let Some(bytes) = self
			.db
			.get(&transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, %id, "failed to get the cache reference"))?
		else {
			return Ok(None);
		};
		let reference = CacheReference::deserialize(bytes).map_err(
			|source| tg::error!(!source, %id, "failed to deserialize the cache reference"),
		)?;
		Ok::<_, tg::Error>(Some(reference))
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
					Request::Put(request) => {
						let result = Self::task_put(env, db, &mut transaction, request);
						responses.push(result);
					},
					Request::PutLog(request) => {
						let result = Self::task_put_log(env, db, &mut transaction, request);
						responses.push(result);
					},
					Request::PutBatch(requests) => {
						for request in requests {
							let result = Self::task_put(env, db, &mut transaction, request);
							responses.push(result);
						}
					},
					Request::Delete(request) => {
						let result = Self::task_delete(env, db, &mut transaction, request);
						responses.push(result);
					},
					Request::DeleteLog(request) => {
						let result = Self::task_delete_log(env, db, &mut transaction, request);
						responses.push(result);
					},
					Request::DeleteBatch(requests) => {
						for request in requests {
							let result = Self::task_delete(env, db, &mut transaction, request);
							responses.push(result);
						}
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

	fn task_put(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: Put,
	) -> tg::Result<()> {
		let id = &request.id;
		if let Some(bytes) = request.bytes {
			let id_bytes = id.to_bytes();
			let key = Key {
				id: id_bytes.as_ref(),
				kind: KeyKind::Bytes,
			};
			let flags = lmdb::PutFlags::NO_OVERWRITE;
			let result = db.put_with_flags(transaction, flags, &key.pack_to_vec(), &bytes);
			match result {
				Ok(()) | Err(lmdb::Error::Mdb(lmdb::MdbError::KeyExist)) => (),
				Err(error) => {
					return Err(tg::error!(!error, %id, "failed to put the object"));
				},
			}
		}
		let id_bytes = id.to_bytes();
		let key = Key {
			id: id_bytes.as_ref(),
			kind: KeyKind::TouchedAt,
		};
		let touched_at = request.touched_at.to_le_bytes();
		db.put(transaction, &key.pack_to_vec(), &touched_at)
			.map_err(|source| tg::error!(!source, %id, "failed to put the touched_at"))?;
		if let Some(cache_reference) = request.cache_reference {
			let id_bytes = id.to_bytes();
			let key = Key {
				id: id_bytes.as_ref(),
				kind: KeyKind::CacheReference,
			};
			let value = cache_reference.serialize().unwrap();
			db.put(transaction, &key.pack_to_vec(), &value)
				.map_err(|source| tg::error!(!source, %id, "failed to put the cache reference"))?;
		}
		Ok(())
	}

	#[expect(clippy::needless_pass_by_value)]
	fn task_delete(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: Delete,
	) -> tg::Result<()> {
		let id = &request.id;
		let id_bytes = id.to_bytes();
		let key = Key {
			id: id_bytes.as_ref(),
			kind: KeyKind::TouchedAt,
		};
		let Some(touched_at) = db
			.get(transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, %id, "failed to get the touched_at"))?
		else {
			return Ok(());
		};
		let touched_at = touched_at
			.try_into()
			.map_err(|source| tg::error!(!source, %id, "invalid touched_at"))?;
		let touched_at = i64::from_le_bytes(touched_at);
		if request.now - touched_at >= request.ttl.to_i64().unwrap() {
			let id_bytes = id.to_bytes();
			let key = Key {
				id: id_bytes.as_ref(),
				kind: KeyKind::Bytes,
			};
			db.delete(transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, %id, "failed to delete the object"))?;
			let id_bytes = id.to_bytes();
			let key = Key {
				id: id_bytes.as_ref(),
				kind: KeyKind::TouchedAt,
			};
			db.delete(transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, %id, "failed to delete the touched_at"))?;
			let id_bytes = id.to_bytes();
			let key = Key {
				id: id_bytes.as_ref(),
				kind: KeyKind::CacheReference,
			};
			db.delete(transaction, &key.pack_to_vec()).map_err(
				|source| tg::error!(!source, %id, "failed to delete the cache reference"),
			)?;
		}
		Ok(())
	}

	#[allow(clippy::needless_pass_by_value)]
	fn task_put_log(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: PutLog,
	) -> tg::Result<()> {
		// Get the log index.
		let id = request.id.to_bytes();
		let key = Key {
			id: &id,
			kind: KeyKind::LogIndex,
		};
		let key = key.pack_to_vec();
		let index = db
			.get_or_put(transaction, &key, &[0u8; 8])
			.map_err(|source| tg::error!(!source, "failed to get the log index"))?
			.unwrap_or(&[0u8; 8]);
		let index = u64::from_le_bytes(
			index
				.try_into()
				.map_err(|_| tg::error!("expected 8 bytes"))?,
		);

		// Get the combined position
		let key = Key {
			id: &id,
			kind: KeyKind::LogPosition(None),
		};
		let key = key.pack_to_vec();
		let combined_position = db
			.get_or_put(transaction, &key, &[0u8; 8])
			.map_err(|source| tg::error!(!source, "failed to get the log position"))?
			.unwrap_or(&[0u8; 8]);
		let combined_position = u64::from_le_bytes(
			combined_position
				.try_into()
				.map_err(|_| tg::error!("expected 8 bytes"))?,
		);

		// Get the stream position
		let key = Key {
			id: &id,
			kind: KeyKind::LogPosition(Some(request.stream)),
		};
		let key = key.pack_to_vec();
		let stream_position = db
			.get_or_put(transaction, &key, &[0u8; 8])
			.map_err(|source| tg::error!(!source, "failed to get the log position"))?
			.unwrap_or(&[0u8; 8]);
		let stream_position = u64::from_le_bytes(
			stream_position
				.try_into()
				.map_err(|_| tg::error!("expected 8 bytes"))?,
		);

		// Store the log position
		let key = Key {
			id: &id,
			kind: KeyKind::Log(None, combined_position),
		};
		let key = key.pack_to_vec();
		let val = index.to_le_bytes();
		db.put(transaction, &key, &val).map_err(|source| {
			tg::error!(!source, "failed to update the log's combined position")
		})?;

		// Store the stream position
		let key = Key {
			id: &id,
			kind: KeyKind::Log(Some(request.stream), stream_position),
		};
		let key = key.pack_to_vec();
		let val = index.to_le_bytes();
		db.put(transaction, &key, &val)
			.map_err(|source| tg::error!(!source, "failed to update the log's stream position"))?;

		// Create the log entry.
		let entry = crate::log::Chunk {
			bytes: request.bytes.clone(),
			combined_position,
			stream_position,
			stream: request.stream,
			timestamp: request.timestamp,
		};
		// Store the log entry.
		let key = Key {
			id: &id,
			kind: KeyKind::LogBytes(index),
		};
		let key = key.pack_to_vec();
		let val = tangram_serialize::to_vec(&entry).unwrap();
		db.put(transaction, &key, &val)
			.map_err(|source| tg::error!(!source, "failed to update the log bytes"))?;

		// Store the index.
		let key = Key {
			id: &id,
			kind: KeyKind::LogIndex,
		};
		let key = key.pack_to_vec();
		let index = (index + 1).to_le_bytes();
		db.put(transaction, &key, &index)
			.map_err(|source| tg::error!(!source, "failed to update the log index"))?;

		// Store the combined position.
		let key = Key {
			id: &id,
			kind: KeyKind::LogPosition(None),
		};
		let key = key.pack_to_vec();
		let combined_position =
			(combined_position + request.bytes.len().to_u64().unwrap()).to_le_bytes();
		db.put(transaction, &key, &combined_position)
			.map_err(|source| tg::error!(!source, "failed to update the log combined position"))?;

		// Store the stream position.
		let key = Key {
			id: &id,
			kind: KeyKind::LogPosition(Some(request.stream)),
		};
		let key = key.pack_to_vec();
		let stream_position =
			(stream_position + request.bytes.len().to_u64().unwrap()).to_le_bytes();
		db.put(transaction, &key, &stream_position)
			.map_err(|source| tg::error!(!source, "failed to update the log stream position"))?;

		Ok(())
	}

	#[expect(clippy::needless_pass_by_value)]
	fn task_delete_log(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: DeleteLog,
	) -> tg::Result<()> {
		let id = request.id.to_bytes();

		// Get the log index to know how many LogBytes entries exist.
		let key = Key {
			id: &id,
			kind: KeyKind::LogIndex,
		};
		let index = db
			.get(transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, "failed to get the log index"))?
			.map_or(0, |bytes| u64::from_le_bytes(bytes.try_into().unwrap_or([0u8; 8])));

		// First, read all LogBytes entries to collect positions for Log entry deletion.
		let mut entries = Vec::new();
		for i in 0..index {
			let key = Key {
				id: &id,
				kind: KeyKind::LogBytes(i),
			};
			if let Some(val) = db
				.get(transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to get the log entry"))?
				&& let Ok(entry) = tangram_serialize::from_slice::<crate::log::Chunk>(val) {
					entries.push(entry);
				}
		}

		// Delete Log entries using the collected positions.
		for entry in &entries {
			// Delete the combined position entry.
			let key = Key {
				id: &id,
				kind: KeyKind::Log(None, entry.combined_position),
			};
			db.delete(transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to delete the log entry"))?;

			// Delete the stream-specific position entry.
			let key = Key {
				id: &id,
				kind: KeyKind::Log(Some(entry.stream), entry.combined_position),
			};
			db.delete(transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to delete the log entry"))?;
		}

		// Delete all LogBytes entries.
		for i in 0..index {
			let key = Key {
				id: &id,
				kind: KeyKind::LogBytes(i),
			};
			db.delete(transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to delete the log bytes"))?;
		}

		// Delete LogPosition entries.
		for stream in [
			None,
			Some(tg::process::log::Stream::Stdout),
			Some(tg::process::log::Stream::Stderr),
		] {
			let key = Key {
				id: &id,
				kind: KeyKind::LogPosition(stream),
			};
			db.delete(transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to delete the log position"))?;
		}

		// Delete LogIndex.
		let key = Key {
			id: &id,
			kind: KeyKind::LogIndex,
		};
		db.delete(transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, "failed to delete the log index"))?;

		Ok(())
	}

	pub fn put_sync(&self, arg: PutArg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let request = Put {
			bytes: arg.bytes,
			cache_reference: arg.cache_reference,
			id: arg.id,
			touched_at: arg.touched_at,
		};
		Self::task_put(&self.env, &self.db, &mut transaction, request)?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn put_batch_sync(&self, args: Vec<PutArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		for arg in args {
			let request = Put {
				bytes: arg.bytes,
				cache_reference: arg.cache_reference,
				id: arg.id,
				touched_at: arg.touched_at,
			};
			Self::task_put(&self.env, &self.db, &mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn delete_sync(&self, arg: DeleteArg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let request = Delete {
			id: arg.id,
			now: arg.now,
			ttl: arg.ttl,
		};
		Self::task_delete(&self.env, &self.db, &mut transaction, request)?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn delete_batch_sync(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		for arg in args {
			let request = Delete {
				id: arg.id,
				now: arg.now,
				ttl: arg.ttl,
			};
			Self::task_delete(&self.env, &self.db, &mut transaction, request)?;
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

impl crate::Error for Error {
	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}

impl crate::Store for Store {
	type Error = Error;

	async fn try_get(&self, id: &tg::object::Id) -> Result<Option<Bytes>, Self::Error> {
		let bytes = tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let id = id.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let id_bytes = id.to_bytes();
				let key = Key {
					id: id_bytes.as_ref(),
					kind: KeyKind::Bytes,
				};
				let Some(bytes) = db
					.get(&transaction, &key.pack_to_vec())
					.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
				else {
					return Ok(None);
				};
				let bytes = Bytes::copy_from_slice(bytes);
				Ok::<_, tg::Error>(Some(bytes))
			}
		})
		.await
		.map_err(|source| Error::other(tg::error!(!source, "failed to join the task")))?
		.map_err(Error::other)?;
		Ok(bytes)
	}

	async fn try_read_log(&self, arg: ReadLogArg) -> Result<Option<Bytes>, Self::Error> {
		let bytes = tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let id = arg.process.to_bytes();
				let key = Key {
					id: id.as_ref(),
					kind: KeyKind::Log(arg.stream, arg.position),
				};
				let key = key.pack_to_vec();
				let index = if arg.position == 0 {
					db.get(&transaction, &key)
						.map_err(|source| tg::error!(!source, "failed to get the log entry"))?
				} else {
					db.get_lower_than_or_equal_to(&transaction, &key)
						.map_err(|source| tg::error!(!source, "failed to get the log entry"))?
						.map(|(_k, v)| v)
				};
				let Some(index) = index else {
					return Ok(None);
				};
				let mut index = u64::from_le_bytes(
					index
						.try_into()
						.map_err(|_| tg::error!("expected 8 bytes"))?,
				);
				let mut output = Vec::new();
				while output.len().to_u64().unwrap() < arg.length {
					let key = Key {
						id: id.as_ref(),
						kind: KeyKind::LogBytes(index),
					};
					let key = key.pack_to_vec();
					let Some(val) = db
						.get(&transaction, &key)
						.map_err(|source| tg::error!(!source, "failed to get the log entry"))?
					else {
						break;
					};
					let entry = tangram_serialize::from_slice::<crate::log::Chunk>(val).map_err(
						|source| tg::error!(!source, "failed to deserialize the log entry"),
					)?;

					// Get the position based on whether we are filtering by stream.
					let position = match arg.stream {
						None => entry.combined_position,
						Some(stream) if stream == entry.stream => entry.stream_position,
						Some(_stream) => {
							index += 1;
							continue;
						},
					};

					// Calculate the offset into this entry's bytes.
					let offset = arg.position.saturating_sub(position).to_usize().unwrap();

					// Calculate how many bytes to take from this entry.
					let remaining = arg.length.to_usize().unwrap() - output.len();
					let available = entry.bytes.len().saturating_sub(offset);
					let length = remaining.min(available);

					// Extend output with the bytes from this entry.
					if length > 0 {
						output.extend_from_slice(&entry.bytes[offset..(offset + length)]);
					}

					index += 1;
				}

				Ok::<_, tg::Error>(Some(output.into()))
			}
		})
		.await
		.map_err(Error::other)?
		.map_err(Error::other)?;
		Ok(bytes)
	}

	async fn try_get_log_length(
		&self,
		id: &tg::process::Id,
		stream: Option<tg::process::log::Stream>,
	) -> Result<Option<u64>, Self::Error> {
		let length = tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let id = id.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let id = id.to_bytes();

				// Get the log position for this stream.
				let key = Key {
					id: id.as_ref(),
					kind: KeyKind::LogPosition(stream),
				};
				let Some(bytes) = db
					.get(&transaction, &key.pack_to_vec())
					.map_err(|source| tg::error!(!source, "failed to get the log position"))?
				else {
					return Ok(None);
				};
				let length = u64::from_le_bytes(
					bytes
						.try_into()
						.map_err(|_| tg::error!("expected 8 bytes"))?,
				);

				Ok::<_, tg::Error>(Some(length))
			}
		})
		.await
		.map_err(Error::other)?
		.map_err(Error::other)?;
		Ok(length)
	}

	async fn try_get_log_entry(
		&self,
		id: &tg::process::Id,
		index: u64,
	) -> Result<Option<crate::log::Chunk>, Self::Error> {
		let entry = tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let id = id.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let id = id.to_bytes();

				let key = Key {
					id: id.as_ref(),
					kind: KeyKind::LogBytes(index),
				};
				let Some(val) = db
					.get(&transaction, &key.pack_to_vec())
					.map_err(|source| tg::error!(!source, "failed to get the log entry"))?
				else {
					return Ok(None);
				};
				let entry = tangram_serialize::from_slice::<crate::log::Chunk>(val)
					.map_err(|source| tg::error!(!source, "failed to deserialize the log entry"))?;
				Ok::<_, tg::Error>(Some(entry))
			}
		})
		.await
		.map_err(Error::other)?
		.map_err(Error::other)?;
		Ok(entry)
	}

	async fn try_get_num_log_entries(
		&self,
		id: &tg::process::Id,
	) -> Result<Option<u64>, Self::Error> {
		let count = tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let id = id.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let id = id.to_bytes();

				let key = Key {
					id: id.as_ref(),
					kind: KeyKind::LogIndex,
				};
				let Some(bytes) = db
					.get(&transaction, &key.pack_to_vec())
					.map_err(|source| tg::error!(!source, "failed to get the log index"))?
				else {
					return Ok(None);
				};
				let count = u64::from_le_bytes(
					bytes
						.try_into()
						.map_err(|_| tg::error!("expected 8 bytes"))?,
				);

				Ok::<_, tg::Error>(Some(count))
			}
		})
		.await
		.map_err(Error::other)?
		.map_err(Error::other)?;
		Ok(count)
	}

	async fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
	) -> Result<Vec<Option<Bytes>>, Self::Error> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let bytes = tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let ids = ids.to_owned();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let mut outputs = Vec::with_capacity(ids.len());
				for id in ids {
					let id_bytes = id.to_bytes();
					let key = Key {
						id: id_bytes.as_ref(),
						kind: KeyKind::Bytes,
					};
					let bytes = db
						.get(&transaction, &key.pack_to_vec())
						.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
						.map(Bytes::copy_from_slice);
					outputs.push(bytes);
				}
				Ok::<_, tg::Error>(outputs)
			}
		})
		.await
		.map_err(|source| Error::other(tg::error!(!source, "failed to join the task")))?
		.map_err(Error::other)?;
		Ok(bytes)
	}

	async fn try_get_cache_reference(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<CacheReference>, Self::Error> {
		let reference = tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let id = id.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let id_bytes = id.to_bytes();
				let key = Key {
					id: id_bytes.as_ref(),
					kind: KeyKind::CacheReference,
				};
				let Some(bytes) = db.get(&transaction, &key.pack_to_vec()).map_err(
					|source| tg::error!(!source, %id, "failed to get the cache reference"),
				)?
				else {
					return Ok(None);
				};
				let reference = CacheReference::deserialize(bytes).map_err(
					|source| tg::error!(!source, %id, "failed to deserialize the cache reference"),
				)?;
				Ok::<_, tg::Error>(Some(reference))
			}
		})
		.await
		.map_err(|source| Error::other(tg::error!(!source, "failed to join the task")))?
		.map_err(Error::other)?;
		Ok(reference)
	}

	async fn put(&self, arg: PutArg) -> Result<(), Self::Error> {
		let id = arg.id.clone();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Put(Put {
			bytes: arg.bytes,
			cache_reference: arg.cache_reference,
			id: arg.id,
			touched_at: arg.touched_at,
		});
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| {
				Error::other(tg::error!(!source, %id, "failed to send the request"))
			})?;
		receiver
			.await
			.map_err(|_| Error::other(tg::error!(%id, "the task panicked")))?
			.map_err(Error::other)?;
		Ok(())
	}

	async fn put_log(&self, arg: PutLogArg) -> Result<(), Self::Error> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::PutLog(PutLog {
			bytes: arg.bytes,
			id: arg.process,
			stream: arg.stream,
			timestamp: arg.timestamp,
		});
		self.sender
			.send((request, sender))
			.await
			.map_err(Error::other)?;
		receiver
			.await
			.map_err(|_| Error::other("task panicked"))?
			.map_err(Error::other)?;
		Ok(())
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> Result<(), Self::Error> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::PutBatch(
			args.into_iter()
				.map(|arg| Put {
					bytes: arg.bytes,
					cache_reference: arg.cache_reference,
					id: arg.id,
					touched_at: arg.touched_at,
				})
				.collect(),
		);
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| Error::other(tg::error!(!source, "failed to send the request")))?;
		receiver
			.await
			.map_err(|_| Error::other(tg::error!("the task panicked")))?
			.map_err(Error::other)?;
		Ok(())
	}

	async fn delete(&self, arg: DeleteArg) -> Result<(), Self::Error> {
		let id = arg.id.clone();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Delete(Delete {
			id: arg.id,
			now: arg.now,
			ttl: arg.ttl,
		});
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| {
				Error::other(tg::error!(!source, %id, "failed to send the request"))
			})?;
		receiver
			.await
			.map_err(|_| Error::other(tg::error!(%id, "the task panicked")))?
			.map_err(Error::other)?;
		Ok(())
	}

	async fn delete_log(&self, arg: DeleteLogArg) -> Result<(), Self::Error> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteLog(DeleteLog { id: arg.process });
		self.sender
			.send((request, sender))
			.await
			.map_err(Error::other)?;
		receiver
			.await
			.map_err(|_| Error::other("task panicked"))?
			.map_err(Error::other)?;
		Ok(())
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> Result<(), Self::Error> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteBatch(
			args.into_iter()
				.map(|arg| Delete {
					id: arg.id,
					now: arg.now,
					ttl: arg.ttl,
				})
				.collect(),
		);
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| Error::other(tg::error!(!source, "failed to send the request")))?;
		receiver
			.await
			.map_err(|_| Error::other(tg::error!("the task panicked")))?
			.map_err(Error::other)?;
		Ok(())
	}

	async fn flush(&self) -> Result<(), Self::Error> {
		tokio::task::spawn_blocking({
			let env = self.env.clone();
			move || {
				env.force_sync()
					.map_err(|source| tg::error!(!source, "failed to sync"))
			}
		})
		.await
		.map_err(|source| Error::other(tg::error!(!source, "failed to join the task")))?
		.map_err(Error::other)?;
		Ok(())
	}
}

impl foundationdb_tuple::TuplePack for Key<'_> {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: foundationdb_tuple::TupleDepth,
	) -> std::io::Result<foundationdb_tuple::VersionstampOffset> {
		let (kind, subkind, offset) = match self.kind {
			KeyKind::Bytes => (0, 0, 0),
			KeyKind::TouchedAt => (1, 0, 0),
			KeyKind::CacheReference => (2, 0, 0),
			KeyKind::Log(None, position) => (3, 0, position),
			KeyKind::Log(Some(tg::process::log::Stream::Stdout), position) => (3, 1, position),
			KeyKind::Log(Some(tg::process::log::Stream::Stderr), position) => (3, 2, position),
			KeyKind::LogPosition(None) => (4, 0, 0),
			KeyKind::LogPosition(Some(tg::process::log::Stream::Stdout)) => (4, 1, 0),
			KeyKind::LogPosition(Some(tg::process::log::Stream::Stderr)) => (4, 2, 0),
			KeyKind::LogIndex => (5, 0, 0),
			KeyKind::LogBytes(index) => (6, 0, index),
		};
		(0, self.id, kind, subkind, offset).pack(w, tuple_depth)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::Store as _;

	#[tokio::test]
	async fn test_put_and_read_log_single_chunk() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10, // 10 MB
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		let process = tg::process::Id::new();

		// Insert a single chunk.
		store
			.put_log(PutLogArg {
				bytes: Bytes::from("hello world"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1000,
			})
			.await
			.unwrap();

		// Read the entire chunk.
		let result = store
			.try_read_log(ReadLogArg {
				process: process.clone(),
				position: 0,
				length: 11,
				stream: Some(tg::process::log::Stream::Stdout),
			})
			.await
			.unwrap();
		assert_eq!(result, Some(Bytes::from("hello world")));

		// Read a subset of the chunk.
		let result = store
			.try_read_log(ReadLogArg {
				process: process.clone(),
				position: 6,
				length: 5,
				stream: Some(tg::process::log::Stream::Stdout),
			})
			.await
			.unwrap();
		assert_eq!(result, Some(Bytes::from("world")));
	}

	#[tokio::test]
	async fn test_put_and_read_log_multiple_chunks() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10, // 10 mb
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		let process = tg::process::Id::new();

		// Insert multiple chunks.
		store
			.put_log(PutLogArg {
				bytes: Bytes::from("hello"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1000,
			})
			.await
			.unwrap();
		store
			.put_log(PutLogArg {
				bytes: Bytes::from(" "),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1001,
			})
			.await
			.unwrap();
		store
			.put_log(PutLogArg {
				bytes: Bytes::from("world"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1002,
			})
			.await
			.unwrap();

		// Read across all chunks.
		let result = store
			.try_read_log(ReadLogArg {
				process: process.clone(),
				position: 0,
				length: 11,
				stream: Some(tg::process::log::Stream::Stdout),
			})
			.await
			.unwrap();
		assert_eq!(result, Some(Bytes::from("hello world")));
	}

	#[tokio::test]
	async fn test_read_log_across_chunk_boundaries() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10, // 10 mb
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		let process = tg::process::Id::new();

		// Insert chunks: "AAAA" (0-3), "BBBB" (4-7), "CCCC" (8-11).
		store
			.put_log(PutLogArg {
				bytes: Bytes::from("AAAA"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1000,
			})
			.await
			.unwrap();
		store
			.put_log(PutLogArg {
				bytes: Bytes::from("BBBB"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1001,
			})
			.await
			.unwrap();
		store
			.put_log(PutLogArg {
				bytes: Bytes::from("CCCC"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1002,
			})
			.await
			.unwrap();

		// Read starting in the middle of the first chunk, across into the second.
		let result = store
			.try_read_log(ReadLogArg {
				process: process.clone(),
				position: 2,
				length: 4,
				stream: Some(tg::process::log::Stream::Stdout),
			})
			.await
			.unwrap();
		assert_eq!(result, Some(Bytes::from("AABB")));

		// Read starting in the middle of the second chunk, across into the third.
		let result = store
			.try_read_log(ReadLogArg {
				process: process.clone(),
				position: 6,
				length: 4,
				stream: Some(tg::process::log::Stream::Stdout),
			})
			.await
			.unwrap();
		assert_eq!(result, Some(Bytes::from("BBCC")));

		// Read spanning all three chunks.
		let result = store
			.try_read_log(ReadLogArg {
				process: process.clone(),
				position: 2,
				length: 8,
				stream: Some(tg::process::log::Stream::Stdout),
			})
			.await
			.unwrap();
		assert_eq!(result, Some(Bytes::from("AABBBBCC")));
	}

	#[tokio::test]
	async fn test_read_log_combined_stream() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10, // 10 mb
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		let process = tg::process::Id::new();

		// Insert interleaved stdout and stderr chunks.
		store
			.put_log(PutLogArg {
				bytes: Bytes::from("out1"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1000,
			})
			.await
			.unwrap();
		store
			.put_log(PutLogArg {
				bytes: Bytes::from("err1"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stderr,
				timestamp: 1001,
			})
			.await
			.unwrap();
		store
			.put_log(PutLogArg {
				bytes: Bytes::from("out2"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1002,
			})
			.await
			.unwrap();

		// Read the combined stream.
		let result = store
			.try_read_log(ReadLogArg {
				process: process.clone(),
				position: 0,
				length: 12,
				stream: None,
			})
			.await
			.unwrap();
		assert_eq!(result, Some(Bytes::from("out1err1out2")));

		// Read only stdout.
		let result = store
			.try_read_log(ReadLogArg {
				process: process.clone(),
				position: 0,
				length: 8,
				stream: Some(tg::process::log::Stream::Stdout),
			})
			.await
			.unwrap();
		assert_eq!(result, Some(Bytes::from("out1out2")));

		// Read only stderr.
		let result = store
			.try_read_log(ReadLogArg {
				process: process.clone(),
				position: 0,
				length: 4,
				stream: Some(tg::process::log::Stream::Stderr),
			})
			.await
			.unwrap();
		assert_eq!(result, Some(Bytes::from("err1")));
	}

	#[tokio::test]
	async fn test_delete_log_removes_all_chunks() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10, // 10 mb
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		let process = tg::process::Id::new();

		// Insert some chunks.
		store
			.put_log(PutLogArg {
				bytes: Bytes::from("hello"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1000,
			})
			.await
			.unwrap();
		store
			.put_log(PutLogArg {
				bytes: Bytes::from("world"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stderr,
				timestamp: 1001,
			})
			.await
			.unwrap();

		// Verify the log exists.
		let result = store
			.try_read_log(ReadLogArg {
				process: process.clone(),
				position: 0,
				length: 10,
				stream: None,
			})
			.await
			.unwrap();
		assert_eq!(result, Some(Bytes::from("helloworld")));

		// Delete the log.
		store
			.delete_log(DeleteLogArg {
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				stream_position: 0,
			})
			.await
			.unwrap();

		// Verify the log no longer exists.
		let result = store
			.try_read_log(ReadLogArg {
				process: process.clone(),
				position: 0,
				length: 10,
				stream: None,
			})
			.await
			.unwrap();
		assert_eq!(result, None);
	}

	#[tokio::test]
	async fn test_try_get_log_length() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10, // 10 mb
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		let process = tg::process::Id::new();

		// Insert chunks.
		store
			.put_log(PutLogArg {
				bytes: Bytes::from("hello"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1000,
			})
			.await
			.unwrap();
		store
			.put_log(PutLogArg {
				bytes: Bytes::from("err"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stderr,
				timestamp: 1001,
			})
			.await
			.unwrap();
		store
			.put_log(PutLogArg {
				bytes: Bytes::from("world"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1002,
			})
			.await
			.unwrap();

		// Check lengths.
		assert_eq!(
			store.try_get_log_length(&process, None).await.unwrap(),
			Some(13)
		); // 5 + 3 + 5
		assert_eq!(
			store
				.try_get_log_length(&process, Some(tg::process::log::Stream::Stdout))
				.await
				.unwrap(),
			Some(10)
		); // 5 + 5
		assert_eq!(
			store
				.try_get_log_length(&process, Some(tg::process::log::Stream::Stderr))
				.await
				.unwrap(),
			Some(3)
		);
	}

	#[tokio::test]
	async fn test_read_log_at_end_returns_empty() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10, // 10 mb
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		let process = tg::process::Id::new();

		// Insert a chunk.
		store
			.put_log(PutLogArg {
				bytes: Bytes::from("hello"),
				process: process.clone(),
				stream: tg::process::log::Stream::Stdout,
				timestamp: 1000,
			})
			.await
			.unwrap();

		// Read at the end position.
		let result = store
			.try_read_log(ReadLogArg {
				process: process.clone(),
				position: 5,
				length: 10,
				stream: Some(tg::process::log::Stream::Stdout),
			})
			.await
			.unwrap();
		assert_eq!(result, Some(Bytes::new()));
	}

	#[tokio::test]
	async fn test_read_log_nonexistent_process() {
		let temp_dir = tempfile::tempdir().unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10, // 10 mb
			path: temp_dir.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		let process = tg::process::Id::new();

		// Try to read from a process that does not exist.
		let result = store
			.try_read_log(ReadLogArg {
				process,
				position: 0,
				length: 10,
				stream: None,
			})
			.await
			.unwrap();
		assert_eq!(result, None);
	}
}
