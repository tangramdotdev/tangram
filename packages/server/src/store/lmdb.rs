use super::CacheReference;
use bytes::Bytes;
use foundationdb_tuple::TuplePack as _;
use heed as lmdb;
use num::ToPrimitive;
use tangram_client as tg;

pub struct Lmdb {
	pub db: lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>,
	pub env: lmdb::Env,
	sender: tokio::sync::mpsc::Sender<Message>,
	task: tokio::task::JoinHandle<()>,
}

enum Message {
	Delete(Delete),
	Put(Put),
	Touch(Touch),
}

struct Delete {
	ids: Vec<tg::object::Id>,
	now: i64,
	response_sender: tokio::sync::oneshot::Sender<tg::Result<()>>,
	ttl: u64,
}

struct Put {
	items: Vec<(tg::object::Id, Option<Bytes>, Option<CacheReference>)>,
	touched_at: i64,
	response_sender: tokio::sync::oneshot::Sender<tg::Result<()>>,
}

struct Touch {
	ids: Vec<tg::object::Id>,
	touched_at: i64,
	response_sender: tokio::sync::oneshot::Sender<tg::Result<()>>,
}

impl Lmdb {
	pub fn new(config: &crate::config::LmdbStore) -> tg::Result<Self> {
		std::fs::OpenOptions::new()
			.create(true)
			.truncate(false)
			.read(true)
			.write(true)
			.open(&config.path)
			.map_err(|source| tg::error!(!source, "failed to create or open the database file"))?;
		let env = unsafe {
			heed::EnvOpenOptions::new()
				.map_size(1_099_511_627_776)
				.max_dbs(3)
				.max_readers(1_000)
				.flags(heed::EnvFlags::NO_SUB_DIR)
				.open(&config.path)
				.map_err(|source| tg::error!(!source, "failed to open the database"))?
		};
		let mut transaction = env.write_txn().unwrap();
		let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = env
			.create_database(&mut transaction, None)
			.map_err(|source| tg::error!(!source, "failed to open the database"))?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Create the task.
		let (sender, receiver) = tokio::sync::mpsc::channel::<Message>(256);
		let task = tokio::spawn({
			let env = env.clone();
			async move { Self::task(env, db, receiver).await }
		});

		Ok(Self {
			db,
			env,
			sender,
			task,
		})
	}

	pub async fn try_get(
		&self,
		id: &tangram_client::object::Id,
	) -> Result<Option<Bytes>, tangram_client::Error> {
		let bytes = tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let id = id.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let key = (0, id.to_bytes(), 0);
				let Some(bytes) = db
					.get(&transaction, &key.pack_to_vec())
					.map_err(|source| tg::error!(!source, "failed to get the value"))?
				else {
					return Ok(None);
				};
				let bytes = Bytes::copy_from_slice(bytes);
				Ok::<_, tg::Error>(Some(bytes))
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "the task panicked"))?
		.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?;
		Ok(bytes)
	}

	pub async fn try_get_cache_reference(
		&self,
		id: &tangram_client::object::Id,
	) -> Result<Option<CacheReference>, tangram_client::Error> {
		let reference = tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let id = id.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let key = (0, id.to_bytes(), 2);
				let Some(bytes) = db
					.get(&transaction, &key.pack_to_vec())
					.map_err(|source| tg::error!(!source, "failed to get the value"))?
				else {
					return Ok(None);
				};
				let reference = serde_json::from_slice(bytes)
					.map_err(|source| tg::error!(!source, "failed to deserialize the reference"))?;
				Ok::<_, tg::Error>(Some(reference))
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "the task panicked"))?
		.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?;
		Ok(reference)
	}

	pub async fn put(&self, arg: super::PutArg) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = Message::Put(Put {
			items: vec![(arg.id, arg.bytes, arg.cache_reference)],
			touched_at: arg.touched_at,
			response_sender: sender,
		});
		self.sender
			.send(message)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the message"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub async fn touch(&self, id: &tg::object::Id, touched_at: i64) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = Message::Touch(Touch {
			ids: vec![id.clone()],
			touched_at,
			response_sender: sender,
		});
		self.sender
			.send(message)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the message"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub async fn put_batch(&self, arg: super::PutBatchArg) -> tg::Result<()> {
		if arg.objects.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = Message::Put(Put {
			items: arg.objects.clone(),
			touched_at: arg.touched_at,
			response_sender: sender,
		});
		self.sender
			.send(message)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the message"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub async fn delete_batch(&self, arg: super::DeleteBatchArg) -> tg::Result<()> {
		if arg.ids.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = Message::Delete(Delete {
			ids: arg.ids,
			now: arg.now,
			ttl: arg.ttl,
			response_sender: sender,
		});
		self.sender
			.send(message)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the message"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	async fn task(
		env: lmdb::Env,
		db: lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>,
		mut receiver: tokio::sync::mpsc::Receiver<Message>,
	) {
		while let Some(message) = receiver.recv().await {
			match message {
				Message::Delete(message) => {
					let result = async {
						let mut transaction = env.write_txn().map_err(|source| {
							tg::error!(!source, "failed to begin a transaction")
						})?;
						for id in message.ids {
							let key = (0, id.to_bytes(), 1);
							let Some(touched_at) =
								db.get(&transaction, &key.pack_to_vec()).map_err(|source| {
									tg::error!(!source, "failed to get the touch time")
								})?
							else {
								continue;
							};
							let touched_at = touched_at
								.try_into()
								.map_err(|source| tg::error!(!source, "invalid touch time"))?;
							let touched_at = i64::from_le_bytes(touched_at);
							if message.now - touched_at >= message.ttl.to_i64().unwrap() {
								let key = (0, id.to_bytes(), 0);
								db.delete(&mut transaction, &key.pack_to_vec()).map_err(
									|source| tg::error!(!source, "failed to delete the object"),
								)?;
								let key = (0, id.to_bytes(), 1);
								db.delete(&mut transaction, &key.pack_to_vec()).map_err(
									|source| tg::error!(!source, "failed to delete the object"),
								)?;
								let key = (0, id.to_bytes(), 2);
								db.delete(&mut transaction, &key.pack_to_vec()).map_err(
									|source| tg::error!(!source, "failed to delete the object"),
								)?;
							}
						}
						transaction.commit().map_err(|source| {
							tg::error!(!source, "failed to commit the transaction")
						})?;
						Ok::<_, tg::Error>(())
					}
					.await;
					message.response_sender.send(result).ok();
				},
				Message::Put(message) => {
					let result = async {
						let mut transaction = env.write_txn().map_err(|source| {
							tg::error!(!source, "failed to begin a transaction")
						})?;
						for (id, bytes, reference) in message.items {
							if let Some(bytes) = bytes {
								let key = (0, id.to_bytes(), 0);
								let flags = lmdb::PutFlags::NO_OVERWRITE;
								let result = db.put_with_flags(
									&mut transaction,
									flags,
									&key.pack_to_vec(),
									&bytes,
								);
								match result {
									Ok(()) | Err(lmdb::Error::Mdb(lmdb::MdbError::KeyExist)) => (),
									Err(error) => {
										return Err(tg::error!(!error, "failed to put the value"));
									},
								}
							}
							let key = (0, id.to_bytes(), 1);
							let touched_at = message.touched_at.to_le_bytes();
							db.put(&mut transaction, &key.pack_to_vec(), &touched_at)
								.map_err(|source| tg::error!(!source, "failed to put the value"))?;
							if let Some(reference) = reference {
								let key = (0, id.to_bytes(), 2);
								let value = serde_json::to_vec(&reference).unwrap();
								db.put(&mut transaction, &key.pack_to_vec(), &value)
									.map_err(|source| {
										tg::error!(!source, "failed to put the value")
									})?;
							}
						}
						transaction.commit().map_err(|source| {
							tg::error!(!source, "failed to commit the transaction")
						})?;
						Ok::<_, tg::Error>(())
					}
					.await;
					message.response_sender.send(result).ok();
				},
				Message::Touch(message) => {
					let result = async {
						let mut transaction = env.write_txn().map_err(|source| {
							tg::error!(!source, "failed to begin a transaction")
						})?;
						for id in message.ids {
							let key = (0, id.to_bytes(), 1);
							let touched_at = message.touched_at.to_le_bytes();
							db.put(&mut transaction, &key.pack_to_vec(), &touched_at)
								.map_err(|source| tg::error!(!source, "failed to put the value"))?;
						}
						transaction.commit().map_err(|source| {
							tg::error!(!source, "failed to commit the transaction")
						})?;
						Ok::<_, tg::Error>(())
					}
					.await;
					message.response_sender.send(result).ok();
				},
			}
		}
	}

	pub fn try_get_object_data(&self, id: &tg::object::Id) -> tg::Result<Option<tg::object::Data>> {
		let transaction = self.env.read_txn().unwrap();
		let key = (0, id.to_bytes(), 0);
		let Some(bytes) = self
			.db
			.get(&transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, "failed to get the value"))?
		else {
			return Ok(None);
		};
		let data = tg::object::Data::deserialize(id.kind(), bytes)?;
		Ok(Some(data))
	}
}

impl Drop for Lmdb {
	fn drop(&mut self) {
		self.task.abort();
	}
}
