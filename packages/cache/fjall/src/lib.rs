use {
	self::key::{Key, Kind},
	std::path::PathBuf,
	tangram_client::prelude::*,
};

mod archive;
mod capacity;
mod database;
mod flush;
mod index;
mod key;
mod log;
mod object;
mod read;
mod reader;
mod request;
#[cfg(test)]
mod tests;
mod writer;

pub mod transaction;

#[derive(Clone, Debug)]
pub struct Config {
	pub path: PathBuf,
	pub read_batch_size: usize,
	pub read_concurrency: usize,
	pub write_batch_size: usize,
}

pub struct Cache {
	db: std::sync::Arc<database::Database>,
	reader_handles: Vec<std::thread::JoinHandle<()>>,
	reader_sender: Option<crate::read::Sender>,
	writer_handle: Option<std::thread::JoinHandle<()>>,
	writer_sender: Option<writer::RequestSender>,
}

impl Cache {
	pub fn new(config: &Config) -> tg::Result<Self> {
		Self::validate_config(config)?;

		// Open the database.
		let db = fjall::Database::builder(&config.path)
			.manual_journal_persist(true)
			.open()
			.map_err(
				|error| tg::error!(!error, path = %config.path.display(), "failed to open the fjall cache"),
			)?;
		let keyspace = db
			.keyspace("cache", fjall::KeyspaceCreateOptions::default)
			.map_err(|error| tg::error!(!error, "failed to open the cache keyspace"))?;
		let db = database::Database {
			db,
			keyspace,
			writer_lock: std::sync::Mutex::new(()),
		};
		let db = std::sync::Arc::new(db);

		// Spawn the reader tasks.
		let (reader_sender, reader_handles) = Self::spawn_readers(config, &db);

		// Spawn the writer task.
		let (writer_sender, writer_receiver) = tokio::sync::mpsc::channel(writer::CHANNEL_CAPACITY);
		let writer_handle = std::thread::spawn({
			let db = db.clone();
			let write_batch_size = config.write_batch_size;
			move || {
				let arg = writer::Arg {
					db: &db,
					receiver: writer_receiver,
					write_batch_size,
				};
				Self::writer_task(arg);
			}
		});

		Ok(Self {
			db,
			reader_handles,
			reader_sender: Some(reader_sender),
			writer_handle: Some(writer_handle),
			writer_sender: Some(writer_sender),
		})
	}

	fn validate_config(config: &Config) -> tg::Result<()> {
		if config.read_batch_size == 0 {
			return Err(tg::error!(
				"the Fjall cache read batch size must be greater than zero"
			));
		}
		if config.read_concurrency == 0 {
			return Err(tg::error!(
				"the Fjall cache read concurrency must be greater than zero"
			));
		}
		if config.write_batch_size == 0 {
			return Err(tg::error!(
				"the Fjall cache write batch size must be greater than zero"
			));
		}

		Ok(())
	}

	fn spawn_readers(
		config: &Config,
		db: &std::sync::Arc<database::Database>,
	) -> (crate::read::Sender, Vec<std::thread::JoinHandle<()>>) {
		let (reader_sender, reader_receiver) =
			tokio::sync::mpsc::channel(crate::read::CHANNEL_CAPACITY);
		let reader_receiver = std::sync::Arc::new(std::sync::Mutex::new(reader_receiver));
		let reader_handles = (0..config.read_concurrency)
			.map(|_| {
				let db = db.clone();
				let read_batch_size = config.read_batch_size;
				let receiver = reader_receiver.clone();
				std::thread::spawn(move || {
					let arg = reader::Arg {
						db,
						read_batch_size,
						receiver,
						#[cfg(test)]
						test_hook: None,
					};
					Self::reader_task(&arg);
				})
			})
			.collect();

		(reader_sender, reader_handles)
	}

	#[must_use]
	pub fn read_transaction(&self) -> transaction::Transaction<'_> {
		self.db.read_transaction()
	}
}

impl Drop for Cache {
	fn drop(&mut self) {
		drop(self.reader_sender.take());
		drop(self.writer_sender.take());
		for handle in self.reader_handles.drain(..) {
			handle.join().ok();
		}
		if let Some(handle) = self.writer_handle.take() {
			handle.join().ok();
		}
	}
}

impl tangram_cache::Cache for Cache {
	async fn flush(&self) -> tg::Result<()> {
		self.flush().await?;
		Ok(())
	}

	async fn try_get_capacity(&self) -> tg::Result<Option<tangram_cache::capacity::Capacity>> {
		self.try_get_capacity().map(Some)
	}
}
