use bytes::Bytes;
use heed as lmdb;
use tangram_client as tg;

pub struct Lmdb {
	db: lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>,
	env: lmdb::Env,
	write_sender: tokio::sync::mpsc::Sender<WriteMessage>,
	write_task: tokio::task::JoinHandle<tg::Result<()>>,
}

struct WriteMessage {
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

		// Create the write task.
		let (write_sender, mut write_receiver) = tokio::sync::mpsc::channel::<WriteMessage>(256);
		let write_task = tokio::spawn({
			let env = env.clone();
			async move {
				while let Some(message) = write_receiver.recv().await {
					let result = async {
						let mut transaction = env.write_txn().map_err(|source| {
							tg::error!(!source, "failed to begin a transaction")
						})?;
						for (id, bytes) in message.items {
							db.put(&mut transaction, id.to_string().as_bytes(), &bytes)
								.map_err(|source| tg::error!(!source, "failed to put the value"))?;
						}
						transaction.commit().map_err(|source| {
							tg::error!(!source, "failed to commit the transaction")
						})?;
						Ok::<_, tg::Error>(())
					}
					.await;
					message.response_sender.send(result).ok();
				}
				Ok::<_, tg::Error>(())
			}
		});

		Ok(Self {
			db,
			env,
			write_sender,
			write_task,
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
				let Some(bytes) = db
					.get(&transaction, id.to_string().as_bytes())
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
		let message = WriteMessage {
			items: vec![(id.clone(), bytes)],
			response_sender: sender,
		};
		self.write_sender
			.send(message)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the message"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the write task panicked"))??;
		Ok(())
	}

	pub async fn put_batch(&self, items: &[(tg::object::Id, Bytes)]) -> tg::Result<()> {
		if items.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = WriteMessage {
			items: items.into(),
			response_sender: sender,
		};
		self.write_sender
			.send(message)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the message"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the write task panicked"))??;
		Ok(())
	}
}

impl Drop for Lmdb {
	fn drop(&mut self) {
		self.write_task.abort();
	}
}
