use {
	self::key::{Key, Kind},
	heed as lmdb,
	std::path::PathBuf,
	tangram_client::prelude::*,
};

mod archive;
mod capacity;
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

#[derive(Clone, Debug)]
pub struct Config {
	pub map_size: usize,
	pub path: PathBuf,
	pub posix_sem_prefix: Option<String>,
	pub read_batch_size: usize,
	pub read_concurrency: usize,
	pub write_batch_size: usize,
}

pub struct Cache {
	db: Db,
	env: lmdb::Env,
	reader_handles: Vec<std::thread::JoinHandle<()>>,
	reader_sender: Option<crate::read::Sender>,
	writer_handle: Option<std::thread::JoinHandle<()>>,
	writer_sender: Option<writer::RequestSender>,
}

pub type Db = lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>;

impl Cache {
	pub fn new(config: &Config) -> tg::Result<Self> {
		Self::validate_config(config)?;

		std::fs::OpenOptions::new()
			.create(true)
			.truncate(false)
			.read(true)
			.write(true)
			.open(&config.path)
			.map_err(
				|error| tg::error!(!error, path = %config.path.display(), "failed to open the lmdb file"),
			)?;
		let mut options = lmdb::EnvOpenOptions::new();
		options.map_size(config.map_size).max_readers(1_000);
		unsafe {
			options.flags(
				lmdb::EnvFlags::NO_SUB_DIR | lmdb::EnvFlags::NO_SYNC | lmdb::EnvFlags::WRITE_MAP,
			);
		}
		if let Some(prefix) = &config.posix_sem_prefix {
			options.semaphore_name(prefix.clone());
		}
		let env = unsafe {
			options.open(&config.path).map_err(|error| {
				tg::error!(!error, path = %config.path.display(), "failed to open the lmdb environment")
			})?
		};
		let mut transaction = env.write_txn().unwrap();
		let db = env
			.create_database(&mut transaction, None)
			.map_err(|error| tg::error!(!error, "failed to create the database"))?;
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;

		// Spawn the reader tasks.
		let (reader_sender, reader_handles) = Self::spawn_readers(config, db, &env);

		// Spawn the writer task.
		let (writer_sender, writer_receiver) = tokio::sync::mpsc::channel(writer::CHANNEL_CAPACITY);
		let writer_handle = std::thread::spawn({
			let env = env.clone();
			let write_batch_size = config.write_batch_size;
			move || {
				Self::writer_task(writer::Arg {
					db: &db,
					env: &env,
					receiver: writer_receiver,
					write_batch_size,
				});
			}
		});

		Ok(Self {
			db,
			env,
			reader_handles,
			reader_sender: Some(reader_sender),
			writer_handle: Some(writer_handle),
			writer_sender: Some(writer_sender),
		})
	}

	pub fn new_readonly(config: &Config) -> tg::Result<Self> {
		Self::validate_config(config)?;

		if !std::fs::exists(&config.path).unwrap_or(false) {
			return Err(tg::error!(path = %config.path.display(), "the lmdb file does not exist"));
		}
		let mut options = lmdb::EnvOpenOptions::new();
		options.map_size(config.map_size).max_readers(1_000);
		unsafe {
			options.flags(lmdb::EnvFlags::NO_SUB_DIR | lmdb::EnvFlags::READ_ONLY);
		}
		if let Some(prefix) = &config.posix_sem_prefix {
			options.semaphore_name(prefix.clone());
		}
		let env = unsafe {
			options.open(&config.path).map_err(|error| {
				tg::error!(!error, path = %config.path.display(), "failed to open the lmdb environment read only")
			})?
		};
		let transaction = env
			.read_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let db = env
			.open_database(&transaction, None)
			.map_err(|error| tg::error!(!error, "failed to open the database"))?
			.ok_or_else(|| tg::error!("the database does not exist"))?;
		drop(transaction);

		// Spawn the reader tasks.
		let (reader_sender, reader_handles) = Self::spawn_readers(config, db, &env);

		Ok(Self {
			db,
			env,
			reader_handles,
			reader_sender: Some(reader_sender),
			writer_handle: None,
			writer_sender: None,
		})
	}

	fn validate_config(config: &Config) -> tg::Result<()> {
		if config.read_batch_size == 0 {
			return Err(tg::error!(
				"the LMDB cache read batch size must be greater than zero"
			));
		}
		if config.read_concurrency == 0 {
			return Err(tg::error!(
				"the LMDB cache read concurrency must be greater than zero"
			));
		}
		if config.write_batch_size == 0 {
			return Err(tg::error!(
				"the LMDB cache write batch size must be greater than zero"
			));
		}

		Ok(())
	}

	fn spawn_readers(
		config: &Config,
		db: Db,
		env: &lmdb::Env,
	) -> (crate::read::Sender, Vec<std::thread::JoinHandle<()>>) {
		let (reader_sender, reader_receiver) =
			tokio::sync::mpsc::channel(crate::read::CHANNEL_CAPACITY);
		let reader_receiver = std::sync::Arc::new(std::sync::Mutex::new(reader_receiver));
		let reader_handles = (0..config.read_concurrency)
			.map(|_| {
				let env = env.clone();
				let read_batch_size = config.read_batch_size;
				let receiver = reader_receiver.clone();
				std::thread::spawn(move || {
					Self::reader_task(&reader::Arg {
						db,
						env,
						read_batch_size,
						receiver,
						#[cfg(test)]
						test_hook: None,
					});
				})
			})
			.collect();

		(reader_sender, reader_handles)
	}

	#[must_use]
	pub fn db(&self) -> Db {
		self.db
	}

	#[must_use]
	pub fn env(&self) -> &lmdb::Env {
		&self.env
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
		self.flush().await
	}

	async fn try_get_capacity(&self) -> tg::Result<Option<tangram_cache::capacity::Capacity>> {
		self.try_get_capacity().map(Some)
	}
}
