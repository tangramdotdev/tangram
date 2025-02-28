use bytes::Bytes;
use heed as lmdb;
use tangram_client as tg;
use tokio_stream::{StreamExt as _, wrappers::ReceiverStream};

pub struct Lmdb {
	db: lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>,
	env: lmdb::Env,
	write_handle: tokio::task::JoinHandle<tg::Result<()>>,
	write_sender: tokio::sync::mpsc::Sender<WriteMessage>,
}

struct WriteMessage {
	items: Vec<(tg::object::Id, Bytes)>,
	response: tokio::sync::oneshot::Sender<()>,
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

		// Create task to handle writes.
		let (write_sender, write_receiver) = tokio::sync::mpsc::channel::<WriteMessage>(256);
		let write_handle = tokio::spawn({
			let env = env.clone();
			let mut receiver_stream = ReceiverStream::new(write_receiver);
			async move {
				while let Some(message) = receiver_stream.next().await {
					let mut transaction = env
						.write_txn()
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
					for (id, bytes) in message.items {
						db.put(&mut transaction, id.to_string().as_bytes(), &bytes)
							.map_err(|source| tg::error!(!source, "failed to put the value"))?;
					}
					transaction.commit().map_err(|source| {
						tg::error!(!source, "failed to commit the transaction")
					})?;
					message
						.response
						.send(())
						.map_err(|()| tg::error!("failed to send response"))?;
				}
				Ok::<_, tg::Error>(())
			}
		});

		Ok(Self {
			db,
			env,
			write_handle,
			write_sender,
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
		.map_err(|source| tg::error!(!source, "failed to join task"))?
		.map_err(|source| tg::error!(!source, %id, "failed to get object"))?;
		Ok(bytes)
	}

	pub async fn put(&self, id: &tangram_client::object::Id, bytes: Bytes) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = WriteMessage {
			items: vec![(id.clone(), bytes)],
			response: sender,
		};
		self.write_sender
			.send(message)
			.await
			.map_err(|source| tg::error!(!source, "failed to send put message"))?;
		receiver
			.await
			.map_err(|_| tg::error!("write task failed to respond"))?;
		Ok(())
	}

	pub async fn put_batch(&self, items: &[(tg::object::Id, Bytes)]) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = WriteMessage {
			items: items.into(),
			response: sender,
		};
		self.write_sender
			.send(message)
			.await
			.map_err(|source| tg::error!(!source, "failed to send put message"))?;
		receiver
			.await
			.map_err(|_| tg::error!("write task failed to respond"))?;
		Ok(())
	}
}

impl Drop for Lmdb {
	fn drop(&mut self) {
		self.write_handle.abort();
	}
}
