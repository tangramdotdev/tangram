use bytes::Bytes;
use foundationdb_tuple::TuplePack as _;
use heed as lmdb;
use tangram_client as tg;

pub struct Lmdb {
	db: lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>,
	env: lmdb::Env,
	sender: tokio::sync::mpsc::Sender<Message>,
	task: tokio::task::JoinHandle<()>,
}

enum Message {
	Delete(Delete),
	Put(Put),
}

struct Delete {
	ids: Vec<tg::object::Id>,
	response_sender: tokio::sync::oneshot::Sender<tg::Result<()>>,
}

struct Put {
	items: Vec<(tg::object::Id, Bytes)>,
	response_sender: tokio::sync::oneshot::Sender<tg::Result<()>>,
}

impl Lmdb {
	pub fn new(config: &crate::config::LmdbStore) -> tg::Result<Self> {
		if !config.path.exists() {
			std::fs::File::create(&config.path)
				.map_err(|source| tg::error!(!source, "failed to create the database file"))?;
		}
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
				drop(transaction);
				Ok::<_, tg::Error>(Some(bytes))
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "the task panicked"))?
		.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?;
		Ok(bytes)
	}

	pub async fn put(&self, id: &tangram_client::object::Id, bytes: Bytes) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = Message::Put(Put {
			items: vec![(id.clone(), bytes)],
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

	pub async fn put_batch(&self, items: &[(tg::object::Id, Bytes)]) -> tg::Result<()> {
		if items.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = Message::Put(Put {
			items: items.to_owned(),
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

	pub async fn delete_batch(&self, ids: &[tg::object::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = Message::Delete(Delete {
			ids: ids.to_owned(),
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
									tg::error!(!source, "failed to get The touch time")
								})?
							else {
								continue;
							};
							let touched_at = touched_at
								.try_into()
								.map_err(|source| tg::error!(!source, "invalid touch time"))?;
							let touched_at = i64::from_le_bytes(touched_at);
							let touched_at = time::OffsetDateTime::from_unix_timestamp(touched_at)
								.map_err(|source| tg::error!(!source, "invalid touch time"))?;
							let now = time::OffsetDateTime::now_utc();
							if now - touched_at > time::Duration::hours(1) {
								let key = (0, id.to_bytes(), 0);
								db.delete(&mut transaction, &key.pack_to_vec()).map_err(
									|source| tg::error!(!source, "failed to delete the object"),
								)?;
								let key = (0, id.to_bytes(), 1);
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
						for (id, bytes) in message.items {
							let key = (0, id.to_bytes(), 0);
							db.put(&mut transaction, &key.pack_to_vec(), &bytes)
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
}

impl Drop for Lmdb {
	fn drop(&mut self) {
		self.task.abort();
	}
}
