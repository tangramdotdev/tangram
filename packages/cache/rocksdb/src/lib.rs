use {
	self::key::{Key, Kind},
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

mod cache;
mod capacity;
mod database;
mod delete;
mod flush;
mod get;
mod key;
mod log;
mod object;
mod put;
mod queue;
mod read;
mod reader;
mod request;
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
		let mut options = rocksdb::Options::default();
		options.create_if_missing(true);
		options.set_compression_type(rocksdb::DBCompressionType::Lz4);
		let db = rocksdb::DB::open(&options, &config.path).map_err(
			|error| tg::error!(!error, path = %config.path.display(), "failed to open the rocksdb cache"),
		)?;
		let db = database::Database {
			catch_up_lock: std::sync::Mutex::new(()),
			db,
			secondary: false,
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
				"the RocksDB cache read batch size must be greater than zero"
			));
		}
		if config.read_concurrency == 0 {
			return Err(tg::error!(
				"the RocksDB cache read concurrency must be greater than zero"
			));
		}
		if config.write_batch_size == 0 {
			return Err(tg::error!(
				"the RocksDB cache write batch size must be greater than zero"
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

	/// Opens a secondary that can read only objects flushed by the primary.
	pub fn new_readonly(config: &Config, secondary_path: &Path) -> tg::Result<Self> {
		Self::validate_config(config)?;

		// Open the secondary to read the primary's flushed SST files.
		let mut options = rocksdb::Options::default();
		options.set_max_open_files(-1);
		let db = rocksdb::DB::open_as_secondary(&options, config.path.as_path(), secondary_path)
			.map_err(|error| {
				tg::error!(
					!error,
					path = %config.path.display(),
					"failed to open the rocksdb secondary"
				)
			})?;
		let db = database::Database {
			catch_up_lock: std::sync::Mutex::new(()),
			db,
			secondary: true,
			writer_lock: std::sync::Mutex::new(()),
		};
		let db = std::sync::Arc::new(db);

		// Spawn the reader tasks.
		let (reader_sender, reader_handles) = Self::spawn_readers(config, &db);

		Ok(Self {
			db,
			reader_handles,
			reader_sender: Some(reader_sender),
			writer_handle: None,
			writer_sender: None,
		})
	}

	#[must_use]
	pub fn read_transaction(&self) -> transaction::Transaction<'_> {
		self.db.read_transaction()
	}

	pub fn catch_up(&self) -> tg::Result<()> {
		self.db.catch_up()?;
		Ok(())
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
	async fn contains_object(&self, arg: tangram_cache::object::contains::Arg) -> tg::Result<bool> {
		let arg = tangram_cache::object::get::Arg {
			bytes: false,
			id: arg.id,
			put: Some(arg.put),
		};
		let output = self.try_get_object(arg).await?;

		Ok(output.object.is_some())
	}

	async fn delete_object_cache_entry(
		&self,
		arg: tangram_cache::object::cache::delete::Arg,
	) -> tg::Result<()> {
		self.delete_object_cache_entry(arg).await?;
		Ok(())
	}

	async fn delete_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_archive_queue_entry(arg).await?;
		Ok(())
	}

	async fn delete_log(&self, arg: tangram_cache::log::delete::Arg) -> tg::Result<()> {
		self.delete_log(arg).await?;
		Ok(())
	}

	async fn delete_object(&self, arg: tangram_cache::object::delete::Arg) -> tg::Result<()> {
		self.delete_object(arg).await?;
		Ok(())
	}

	async fn delete_object_batch(
		&self,
		args: Vec<tangram_cache::object::delete::Arg>,
	) -> tg::Result<()> {
		self.delete_object_batch(args).await?;
		Ok(())
	}

	async fn delete_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_index_queue_fragment(arg).await?;
		Ok(())
	}

	async fn get_object_cache_entries(
		&self,
		arg: tangram_cache::object::cache::get::Arg,
	) -> tg::Result<Vec<tangram_cache::object::cache::Entry>> {
		self.get_object_cache_entries(arg).await
	}

	async fn get_archive_queue_entries(
		&self,
		arg: tangram_cache::archive::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::archive::queue::Entry>> {
		self.get_archive_queue_entries(arg).await
	}

	async fn get_index_queue_fragments(
		&self,
		arg: tangram_cache::index::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::index::queue::Fragment>> {
		self.get_index_queue_fragments(arg).await
	}

	async fn put_object_cache_entry(
		&self,
		arg: tangram_cache::object::cache::put::Arg,
	) -> tg::Result<()> {
		self.put_object_cache_entry(arg).await?;
		Ok(())
	}

	async fn put_object_cache_entry_with_object(
		&self,
		arg: tangram_cache::object::cache::put::object::Arg,
	) -> tg::Result<()> {
		self.put_object_cache_entry_with_object(arg).await?;
		Ok(())
	}

	async fn put_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_archive_queue_entry(arg).await?;
		Ok(())
	}

	async fn put_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_index_queue_fragment(arg).await?;
		Ok(())
	}

	async fn flush(&self) -> tg::Result<()> {
		self.flush().await?;
		Ok(())
	}

	async fn put_log(&self, arg: tangram_cache::log::put::Arg) -> tg::Result<()> {
		self.put_log(arg).await?;
		Ok(())
	}

	async fn put_log_batch(&self, args: Vec<tangram_cache::log::put::Arg>) -> tg::Result<()> {
		self.put_log_batch(args).await?;
		Ok(())
	}

	async fn put_log_end(&self, arg: tangram_cache::log::end::Arg) -> tg::Result<()> {
		self.put_log_end(arg).await?;
		Ok(())
	}

	async fn try_get_log_end(
		&self,
		process: &tg::process::Id,
	) -> tg::Result<Option<tg::process::log::End>> {
		self.try_get_log_end(process).await
	}

	async fn put_object(&self, arg: tangram_cache::object::put::Arg) -> tg::Result<()> {
		self.put_object(arg).await?;
		Ok(())
	}

	async fn put_object_batch(&self, args: Vec<tangram_cache::object::put::Arg>) -> tg::Result<()> {
		self.put_object_batch(args).await?;
		Ok(())
	}

	async fn try_get_log_length(
		&self,
		arg: tangram_cache::log::length::Arg,
	) -> tg::Result<Option<u64>> {
		self.try_get_log_length(arg).await
	}

	async fn try_get_object(
		&self,
		arg: tangram_cache::object::get::Arg,
	) -> tg::Result<tangram_cache::object::get::Output> {
		self.try_get_object(arg).await
	}

	async fn try_get_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::archive::queue::Entry>> {
		self.try_get_archive_queue_entry(arg).await
	}

	async fn try_get_object_batch(
		&self,
		arg: tangram_cache::object::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::object::get::Output>> {
		self.try_get_object_batch(arg).await
	}

	async fn try_get_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::index::queue::Fragment>> {
		self.try_get_index_queue_fragment(arg).await
	}

	async fn try_get_capacity(&self) -> tg::Result<Option<tangram_cache::capacity::Capacity>> {
		self.try_get_capacity().map(Some)
	}

	async fn try_read_log(
		&self,
		arg: tangram_cache::log::read::Arg,
	) -> tg::Result<Vec<tangram_cache::log::read::Entry<'static>>> {
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
			path: temp.path().join("test.rocksdb"),
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 8_000,
		};
		let cache = Cache::new(&config).unwrap();

		// Create the object data and ID.
		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		// Put the object.
		let arg = tangram_cache::object::put::Arg {
			bytes: Some(bytes.clone()),
			checkout_pointer: None,
			id: id.clone(),
			length: Some(content.len().to_u64().unwrap()),
			put: [1; 16],
		};
		cache.put_object(arg).await.unwrap();

		// Get the object.
		let arg = tangram_cache::object::get::Arg {
			bytes: true,
			id: id.clone(),
			put: None,
		};
		let result = cache.try_get_object(arg).await.unwrap().object;
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);

		// Get the object without copying its bytes.
		let arg = tangram_cache::object::get::Arg {
			bytes: false,
			id: id.clone(),
			put: Some([1; 16]),
		};
		let object = cache.try_get_object(arg).await.unwrap().object.unwrap();
		assert!(object.bytes.is_none());
		assert_eq!(object.put, [1; 16]);
		let arg = tangram_cache::object::get::batch::Arg {
			bytes: false,
			ids: vec![id.clone()],
		};
		let objects = cache.try_get_object_batch(arg).await.unwrap();
		let object = objects[0].object.as_ref().unwrap();
		assert!(object.bytes.is_none());
		assert_eq!(object.put, [1; 16]);

		// Get the object by its exact put.
		let arg = tangram_cache::object::get::Arg {
			bytes: true,
			id: id.clone(),
			put: Some([0; 16]),
		};
		let result = cache.try_get_object(arg).await.unwrap().object;
		assert!(result.is_none());
		let arg = tangram_cache::object::contains::Arg {
			id: id.clone(),
			put: [0; 16],
		};
		let contains = tangram_cache::Cache::contains_object(&cache, arg)
			.await
			.unwrap();
		assert!(!contains);
		let arg = tangram_cache::object::get::Arg {
			bytes: true,
			id: id.clone(),
			put: Some([1; 16]),
		};
		let result = cache.try_get_object(arg).await.unwrap().object;
		assert_eq!(result.unwrap().put, [1; 16]);
		let arg = tangram_cache::object::contains::Arg { id, put: [1; 16] };
		let contains = tangram_cache::Cache::contains_object(&cache, arg)
			.await
			.unwrap();
		assert!(contains);
	}

	// An object first put without bytes stores no bytes and a later put with bytes makes the bytes retrievable.
	#[tokio::test]
	async fn test_put_object_without_bytes_then_with_bytes() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			path: temp.path().join("test.rocksdb"),
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 8_000,
		};
		let cache = Cache::new(&config).unwrap();

		// Create the object data and ID.
		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		// Put without bytes first (should not cache anything).
		let arg = tangram_cache::object::put::Arg {
			bytes: None,
			checkout_pointer: None,
			id: id.clone(),
			length: None,
			put: [1; 16],
		};
		cache.put_object(arg).await.unwrap();

		// Verify object bytes do not exist (object may exist with bytes=None).
		let arg = tangram_cache::object::get::Arg {
			bytes: true,
			id: id.clone(),
			put: None,
		};
		let result = cache.try_get_object(arg).await.unwrap().object;
		assert!(
			result.is_none()
				|| result
					.as_ref()
					.and_then(|object| object.bytes.as_ref())
					.is_none()
		);

		// Put with bytes.
		let arg = tangram_cache::object::put::Arg {
			bytes: Some(bytes.clone()),
			checkout_pointer: None,
			id: id.clone(),
			length: Some(content.len().to_u64().unwrap()),
			put: [2; 16],
		};
		cache.put_object(arg).await.unwrap();

		// Verify object now exists.
		let arg = tangram_cache::object::get::Arg {
			bytes: true,
			id: id.clone(),
			put: None,
		};
		let result = cache.try_get_object(arg).await.unwrap().object;
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
			path: temp.path().join("test.rocksdb"),
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 8_000,
		};
		let cache = Cache::new(&config).unwrap();

		// Create object data and ID similar to server's write.rs.
		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		// Put the object using sync function (like server does).
		let arg = tangram_cache::object::put::Arg {
			bytes: Some(bytes.clone()),
			checkout_pointer: None,
			id: id.clone(),
			length: Some(content.len().to_u64().unwrap()),
			put: [1; 16],
		};
		cache.put_object_sync(arg).unwrap();

		// Get the object using sync function.
		let arg = tangram_cache::object::get::Arg {
			bytes: true,
			id: id.clone(),
			put: None,
		};
		let result = cache.try_get_object_sync(&arg).unwrap().object;
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
			path: temp.path().join("test.rocksdb"),
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 1,
		};
		let cache = Cache::new(&config).unwrap();

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

		cache
			.put_object_batch(vec![
				tangram_cache::object::put::Arg {
					bytes: Some(bytes.clone()),
					checkout_pointer: None,
					id: id.clone(),
					length: Some(content.len().to_u64().unwrap()),
					put: [1; 16],
				},
				tangram_cache::object::put::Arg {
					bytes: Some(other_bytes.clone()),
					checkout_pointer: None,
					id: other_id.clone(),
					length: Some(other_content.len().to_u64().unwrap()),
					put: [1; 16],
				},
			])
			.await
			.unwrap();

		let arg = tangram_cache::object::get::Arg {
			bytes: true,
			id: id.clone(),
			put: None,
		};
		let result = cache.try_get_object(arg).await.unwrap().object;
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
		let arg = tangram_cache::object::get::Arg {
			bytes: true,
			id: other_id,
			put: None,
		};
		let result = cache.try_get_object(arg).await.unwrap().object;
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
			path: temp.path().join("test.rocksdb"),
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 8_000,
		};
		let cache = Cache::new(&config).unwrap();

		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		// Put an object with a length.
		let arg = tangram_cache::object::put::Arg {
			bytes: Some(bytes.clone()),
			checkout_pointer: None,
			id: id.clone(),
			length: Some(content.len().to_u64().unwrap()),
			put: [1; 16],
		};
		cache.put_object(arg).await.unwrap();
		let arg = tangram_cache::object::get::Arg {
			bytes: true,
			id: id.clone(),
			put: None,
		};
		let object = cache.try_get_object(arg).await.unwrap().object.unwrap();
		assert_eq!(object.length, Some(content.len().to_u64().unwrap()));

		// A later put without a length replaces the length.
		let arg = tangram_cache::object::put::Arg {
			bytes: Some(bytes.clone()),
			checkout_pointer: None,
			id: id.clone(),
			length: None,
			put: [2; 16],
		};
		cache.put_object(arg).await.unwrap();
		let arg = tangram_cache::object::get::Arg {
			bytes: true,
			id: id.clone(),
			put: None,
		};
		let object = cache.try_get_object(arg).await.unwrap().object.unwrap();
		assert_eq!(object.length, None);

		// An object put without a length has no length.
		let other = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"other"));
		let arg = tangram_cache::object::put::Arg {
			bytes: Some(bytes.clone()),
			checkout_pointer: None,
			id: other.clone(),
			length: None,
			put: [1; 16],
		};
		cache.put_object(arg).await.unwrap();
		let arg = tangram_cache::object::get::Arg {
			bytes: true,
			id: other,
			put: None,
		};
		let object = cache.try_get_object(arg).await.unwrap().object.unwrap();
		assert_eq!(object.length, None);

		// An absent object has no length.
		let absent = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"absent"));
		let arg = tangram_cache::object::get::Arg {
			bytes: true,
			id: absent,
			put: None,
		};
		let output = cache.try_get_object(arg).await.unwrap();
		assert!(output.object.is_none());
	}

	// Deleting an object removes the object.
	#[tokio::test]
	async fn test_delete_removes_object() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			path: temp.path().join("test.rocksdb"),
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 8_000,
		};
		let cache = Cache::new(&config).unwrap();

		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		let arg = tangram_cache::object::put::Arg {
			bytes: Some(bytes.clone()),
			checkout_pointer: None,
			id: id.clone(),
			length: Some(content.len().to_u64().unwrap()),
			put: [10; 16],
		};
		cache.put_object(arg).await.unwrap();
		let arg = tangram_cache::object::put::Arg {
			bytes: Some(Bytes::from_static(b"stale")),
			checkout_pointer: None,
			id: id.clone(),
			length: None,
			put: [9; 16],
		};
		cache.put_object(arg).await.unwrap();

		let arg = tangram_cache::object::get::Arg {
			bytes: true,
			id: id.clone(),
			put: None,
		};
		let output = cache.try_get_object(arg).await.unwrap();
		let object = output.object.unwrap();
		assert_eq!(object.bytes, Some(Cow::Owned(bytes.to_vec())));
		assert_eq!(object.put, [10; 16]);

		let arg = tangram_cache::object::delete::Arg {
			id: id.clone(),
			put: [9; 16],
		};
		cache.delete_object(arg).await.unwrap();
		let arg = tangram_cache::object::get::Arg {
			bytes: true,
			id: id.clone(),
			put: None,
		};
		let output = cache.try_get_object(arg).await.unwrap();
		assert!(output.object.is_some());

		let arg = tangram_cache::object::delete::Arg {
			id: id.clone(),
			put: [10; 16],
		};
		cache.delete_object(arg).await.unwrap();

		let arg = tangram_cache::object::get::Arg {
			bytes: true,
			id,
			put: None,
		};
		let output = cache.try_get_object(arg).await.unwrap();
		assert!(output.object.is_none());
	}
}
