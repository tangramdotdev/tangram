use {heed as lmdb, std::path::PathBuf, tangram_client::prelude::*};

mod archive;
mod cache;
mod capacity;
mod delete;
mod flush;
mod get;
mod key;
mod log;
mod object;
mod outbox;
mod put;
mod reader;
mod request;
mod writer;

use key::{Key, Kind};

#[derive(Clone, Debug)]
pub struct Config {
	pub map_size: usize,
	pub path: PathBuf,
	pub posix_sem_prefix: Option<String>,
	pub read_batch_size: usize,
	pub read_concurrency: usize,
	pub write_batch_size: usize,
}

pub struct Store {
	db: Db,
	env: lmdb::Env,
	reader_handles: Vec<std::thread::JoinHandle<()>>,
	reader_sender: Option<crate::read::Sender>,
	writer_handle: Option<std::thread::JoinHandle<()>>,
	writer_sender: Option<writer::RequestSender>,
}

pub type Db = lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>;

impl Store {
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
				lmdb::EnvFlags::NO_SUB_DIR | lmdb::EnvFlags::WRITE_MAP | lmdb::EnvFlags::MAP_ASYNC,
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
				"the LMDB store read batch size must be greater than zero"
			));
		}
		if config.read_concurrency == 0 {
			return Err(tg::error!(
				"the LMDB store read concurrency must be greater than zero"
			));
		}
		if config.write_batch_size == 0 {
			return Err(tg::error!(
				"the LMDB store write batch size must be greater than zero"
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

impl Drop for Store {
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

impl crate::Store for Store {
	async fn delete_object_cache_entry(
		&self,
		arg: crate::object::cache::delete::Arg,
	) -> tg::Result<()> {
		self.delete_object_cache_entry(arg).await
	}

	async fn delete_object_archive_outbox_entries(
		&self,
		arg: crate::object::archive::outbox::delete::Arg,
	) -> tg::Result<()> {
		self.delete_object_archive_outbox_entries(arg).await
	}

	async fn delete_log(&self, arg: crate::log::delete::Arg) -> tg::Result<()> {
		self.delete_log(arg).await
	}

	async fn delete_object(&self, arg: crate::object::delete::Arg) -> tg::Result<()> {
		self.delete_object(arg).await
	}

	async fn delete_object_batch(&self, args: Vec<crate::object::delete::Arg>) -> tg::Result<()> {
		self.delete_object_batch(args).await
	}

	async fn delete_object_index_outbox_fragments(
		&self,
		arg: crate::object::index::outbox::fragment::delete::Arg,
	) -> tg::Result<()> {
		self.delete_object_index_outbox_fragments(arg).await
	}

	async fn dequeue_object_index_outbox_fragments(
		&self,
		arg: crate::object::index::outbox::fragment::dequeue::Arg,
	) -> tg::Result<Vec<crate::object::index::outbox::fragment::Fragment>> {
		self.dequeue_object_index_outbox_fragments(arg).await
	}

	async fn dequeue_object_archive_outbox_entries(
		&self,
		arg: crate::object::archive::outbox::dequeue::Arg,
	) -> tg::Result<Vec<crate::object::archive::outbox::Entry>> {
		self.dequeue_object_archive_outbox_entries(arg).await
	}

	async fn get_object_cache_entries(
		&self,
		arg: crate::object::cache::get::Arg,
	) -> tg::Result<Vec<crate::object::cache::Entry>> {
		self.get_object_cache_entries(arg).await
	}

	async fn put_object_cache_entry(&self, arg: crate::object::cache::put::Arg) -> tg::Result<()> {
		self.put_object_cache_entry(arg).await
	}

	async fn put_object_cache_entry_with_object(
		&self,
		arg: crate::object::cache::put::object::Arg,
	) -> tg::Result<()> {
		self.put_object_cache_entry_with_object(arg).await
	}

	async fn put_object_archive_outbox_entries(
		&self,
		arg: crate::object::archive::outbox::put::Arg,
	) -> tg::Result<()> {
		self.put_object_archive_outbox_entries(arg).await
	}

	async fn enqueue_object_index_outbox_batch(
		&self,
		arg: crate::object::index::outbox::batch::enqueue::Arg,
	) -> tg::Result<()> {
		self.enqueue_object_index_outbox_batch(arg).await
	}

	async fn flush(&self) -> tg::Result<()> {
		self.flush().await
	}

	async fn put_log(&self, arg: crate::log::put::Arg) -> tg::Result<()> {
		self.put_log(arg).await
	}

	async fn put_log_batch(&self, args: Vec<crate::log::put::Arg>) -> tg::Result<()> {
		self.put_log_batch(args).await
	}

	async fn put_object(&self, arg: crate::object::put::Arg) -> tg::Result<()> {
		self.put_object(arg).await
	}

	async fn put_object_batch(&self, args: Vec<crate::object::put::Arg>) -> tg::Result<()> {
		self.put_object_batch(args).await
	}

	async fn try_get_log_length(&self, arg: crate::log::length::Arg) -> tg::Result<Option<u64>> {
		self.try_get_log_length(arg).await
	}

	async fn try_get_object(
		&self,
		arg: crate::object::get::Arg,
	) -> tg::Result<crate::object::get::Output> {
		self.try_get_object(arg).await
	}

	async fn try_get_object_batch(
		&self,
		arg: crate::object::get::batch::Arg,
	) -> tg::Result<Vec<crate::object::get::Output>> {
		self.try_get_object_batch(arg).await
	}

	async fn try_get_object_index_outbox_batch_at_or_before(
		&self,
		arg: crate::object::index::outbox::batch::get::Arg,
	) -> tg::Result<Option<crate::object::index::outbox::batch::Id>> {
		self.try_get_object_index_outbox_batch_at_or_before(arg)
			.await
	}

	async fn try_get_capacity(&self) -> tg::Result<Option<crate::capacity::Capacity>> {
		self.try_get_capacity().map(Some)
	}

	async fn try_read_log(
		&self,
		arg: crate::log::read::Arg,
	) -> tg::Result<Vec<crate::log::read::Entry<'static>>> {
		self.try_read_log(arg).await
	}
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes, num::ToPrimitive as _, std::borrow::Cow};

	mod reader;

	// An object put with bytes can be retrieved with the same bytes.
	#[tokio::test]
	async fn test_put_and_get_object() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp.path().join("test.lmdb"),
			posix_sem_prefix: None,
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 8_000,
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
			.put_object(crate::object::put::Arg {
				bytes: Some(bytes.clone()),
				checkout_pointer: None,
				id: id.clone(),
				length: Some(content.len().to_u64().unwrap()),
				stored_at: 12345,
			})
			.await
			.unwrap();

		// Get the object.
		let arg = crate::object::get::Arg { id: id.clone() };
		let result = store.try_get_object(arg).await.unwrap().object;
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}

	// An object first put without bytes stores no bytes and a later put with bytes makes the bytes retrievable.
	#[tokio::test]
	async fn test_put_object_without_bytes_then_with_bytes() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp.path().join("test.lmdb"),
			posix_sem_prefix: None,
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 8_000,
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
			.put_object(crate::object::put::Arg {
				bytes: None,
				checkout_pointer: None,
				id: id.clone(),
				length: None,
				stored_at: 12345,
			})
			.await
			.unwrap();

		// Verify object bytes do not exist (object may exist with bytes=None).
		let arg = crate::object::get::Arg { id: id.clone() };
		let result = store.try_get_object(arg).await.unwrap().object;
		assert!(
			result.is_none()
				|| result
					.as_ref()
					.and_then(|object| object.bytes.as_ref())
					.is_none()
		);

		// Put with bytes.
		store
			.put_object(crate::object::put::Arg {
				bytes: Some(bytes.clone()),
				checkout_pointer: None,
				id: id.clone(),
				length: Some(content.len().to_u64().unwrap()),
				stored_at: 12346,
			})
			.await
			.unwrap();

		// Verify object now exists.
		let arg = crate::object::get::Arg { id: id.clone() };
		let result = store.try_get_object(arg).await.unwrap().object;
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}

	// An object put and retrieved through the synchronous functions, as the server uses them, round-trips the bytes.
	#[tokio::test]
	async fn test_put_and_get_object_sync() {
		// This test mimics what the server does using sync functions.
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp.path().join("test.lmdb"),
			posix_sem_prefix: None,
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 8_000,
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
			.put_object_sync(crate::object::put::Arg {
				bytes: Some(bytes.clone()),
				checkout_pointer: None,
				id: id.clone(),
				length: Some(content.len().to_u64().unwrap()),
				stored_at: 12345,
			})
			.unwrap();

		// Get the object using sync function.
		let arg = crate::object::get::Arg { id: id.clone() };
		let result = store.try_get_object_sync(&arg).unwrap().object;
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}

	// An object batch split across write transactions can be retrieved with the same bytes.
	#[tokio::test]
	async fn test_put_batch_and_get_object() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp.path().join("test.lmdb"),
			posix_sem_prefix: None,
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 1,
		};
		let store = Store::new(&config).unwrap();

		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);
		let other_content = b"goodbye world";
		let other_data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(other_content),
		}));
		let other_bytes = other_data.serialize().unwrap();
		let other_id = tg::object::Id::new(tg::object::Kind::Blob, &other_bytes);

		store
			.put_object_batch(vec![
				crate::object::put::Arg {
					bytes: Some(bytes.clone()),
					checkout_pointer: None,
					id: id.clone(),
					length: Some(content.len().to_u64().unwrap()),
					stored_at: 12345,
				},
				crate::object::put::Arg {
					bytes: Some(other_bytes.clone()),
					checkout_pointer: None,
					id: other_id.clone(),
					length: Some(other_content.len().to_u64().unwrap()),
					stored_at: 12345,
				},
			])
			.await
			.unwrap();

		let arg = crate::object::get::Arg { id: id.clone() };
		let result = store.try_get_object(arg).await.unwrap().object;
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
		let arg = crate::object::get::Arg { id: other_id };
		let result = store.try_get_object(arg).await.unwrap().object;
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(other_bytes.to_vec()))
		);
	}

	// An object's length is persisted and replaced by later puts.
	#[tokio::test]
	async fn test_put_and_get_object_length() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp.path().join("test.lmdb"),
			posix_sem_prefix: None,
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 8_000,
		};
		let store = Store::new(&config).unwrap();

		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		// Put an object with a length.
		store
			.put_object(crate::object::put::Arg {
				bytes: Some(bytes.clone()),
				checkout_pointer: None,
				id: id.clone(),
				length: Some(content.len().to_u64().unwrap()),
				stored_at: 12345,
			})
			.await
			.unwrap();
		let object = store
			.try_get_object(crate::object::get::Arg { id: id.clone() })
			.await
			.unwrap()
			.object
			.unwrap();
		assert_eq!(object.length, Some(content.len().to_u64().unwrap()));

		// A later put without a length replaces the length.
		store
			.put_object(crate::object::put::Arg {
				bytes: Some(bytes.clone()),
				checkout_pointer: None,
				id: id.clone(),
				length: None,
				stored_at: 12346,
			})
			.await
			.unwrap();
		let object = store
			.try_get_object(crate::object::get::Arg { id: id.clone() })
			.await
			.unwrap()
			.object
			.unwrap();
		assert_eq!(object.length, None);

		// An object put without a length has no length.
		let other = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"other"));
		store
			.put_object(crate::object::put::Arg {
				bytes: Some(bytes.clone()),
				checkout_pointer: None,
				id: other.clone(),
				length: None,
				stored_at: 12345,
			})
			.await
			.unwrap();
		let object = store
			.try_get_object(crate::object::get::Arg { id: other })
			.await
			.unwrap()
			.object
			.unwrap();
		assert_eq!(object.length, None);

		// An absent object has no length.
		let absent = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"absent"));
		let output = store
			.try_get_object(crate::object::get::Arg { id: absent })
			.await
			.unwrap();
		assert!(output.object.is_none());
	}

	// Deleting an object removes the object.
	#[tokio::test]
	async fn test_delete_removes_object() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp.path().join("test.lmdb"),
			posix_sem_prefix: None,
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 8_000,
		};
		let store = Store::new(&config).unwrap();

		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		store
			.put_object(crate::object::put::Arg {
				bytes: Some(bytes.clone()),
				checkout_pointer: None,
				id: id.clone(),
				length: Some(content.len().to_u64().unwrap()),
				stored_at: 10,
			})
			.await
			.unwrap();
		store
			.put_object(crate::object::put::Arg {
				bytes: Some(Bytes::from_static(b"stale")),
				checkout_pointer: None,
				id: id.clone(),
				length: None,
				stored_at: 9,
			})
			.await
			.unwrap();

		let output = store
			.try_get_object(crate::object::get::Arg { id: id.clone() })
			.await
			.unwrap();
		let object = output.object.unwrap();
		assert_eq!(object.bytes, Some(Cow::Owned(bytes.to_vec())));
		assert_eq!(object.stored_at, 10);

		store
			.delete_object(crate::object::delete::Arg {
				id: id.clone(),
				touched_at: 9,
			})
			.await
			.unwrap();
		let output = store
			.try_get_object(crate::object::get::Arg { id: id.clone() })
			.await
			.unwrap();
		assert!(output.object.is_some());

		store
			.delete_object(crate::object::delete::Arg {
				id: id.clone(),
				touched_at: 11,
			})
			.await
			.unwrap();

		let output = store
			.try_get_object(crate::object::get::Arg { id })
			.await
			.unwrap();
		assert!(output.object.is_none());
	}
}
