#[cfg(feature = "v8")]
use self::compiler::Compiler;
use self::{
	database::Database, index::Index, messenger::Messenger, runtime::Runtime, store::Store,
	util::fs::remove,
};
#[cfg(feature = "nats")]
use async_nats as nats;
use dashmap::{DashMap, DashSet};
use futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered};
use indoc::{formatdoc, indoc};
use rusqlite as sqlite;
use std::{
	collections::HashMap,
	ops::Deref,
	os::fd::AsRawFd as _,
	path::PathBuf,
	sync::{Arc, Mutex, RwLock},
};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_futures::task::{Task, TaskMap};
use tangram_messenger::prelude::*;
use tokio::{io::AsyncWriteExt as _, task::JoinSet};
use url::Url;

mod blob;
mod cache;
#[cfg(feature = "v8")]
mod check;
mod checkin;
mod checkout;
mod checksum;
mod clean;
#[cfg(feature = "v8")]
mod compiler;
mod database;
#[cfg(feature = "v8")]
mod document;
mod export;
#[cfg(feature = "v8")]
mod format;
mod handle;
mod health;
mod http;
mod import;
mod index;
mod messenger;
mod module;
mod object;
mod pipe;
mod process;
mod progress;
mod pty;
mod pull;
mod push;
mod reference;
mod remote;
mod runner;
mod runtime;
mod store;
mod tag;
mod temp;
mod user;
mod util;
mod vfs;
mod watchdog;

pub use self::config::Config;

pub mod config;

#[derive(Clone)]
pub struct Server(pub Arc<Inner>);

pub struct Inner {
	cache_task_map: CacheTaskMap,
	#[cfg(feature = "v8")]
	compilers: RwLock<Vec<Compiler>>,
	config: Config,
	database: Database,
	diagnostics: Mutex<Vec<tg::Diagnostic>>,
	http: Option<Http>,
	import_index_tasks: Mutex<Option<JoinSet<()>>>,
	index: Index,
	lock_file: Mutex<Option<tokio::fs::File>>,
	messenger: Messenger,
	path: PathBuf,
	pipes: DashMap<tg::pipe::Id, pipe::Pipe>,
	process_permits: ProcessPermits,
	process_semaphore: Arc<tokio::sync::Semaphore>,
	process_task_map: ProcessTaskMap,
	ptys: DashMap<tg::pty::Id, pty::Pty>,
	remotes: DashMap<String, tg::Client, fnv::FnvBuildHasher>,
	runtimes: RwLock<HashMap<String, Runtime>>,
	store: Store,
	task: Mutex<Option<Task<()>>>,
	temp_paths: DashSet<PathBuf, fnv::FnvBuildHasher>,
	version: String,
	vfs: Mutex<Option<self::vfs::Server>>,
}

type CacheTaskMap = TaskMap<tg::artifact::Id, tg::Result<()>, fnv::FnvBuildHasher>;

struct Http {
	url: Url,
}

type ProcessPermits =
	DashMap<tg::process::Id, Arc<tokio::sync::Mutex<Option<ProcessPermit>>>, fnv::FnvBuildHasher>;

struct ProcessPermit(
	#[allow(dead_code)]
	Either<tokio::sync::OwnedSemaphorePermit, tokio::sync::OwnedMutexGuard<Option<Self>>>,
);

type ProcessTaskMap = TaskMap<tg::process::Id, (), fnv::FnvBuildHasher>;

impl Server {
	pub async fn start(config: Config) -> tg::Result<Server> {
		// Ensure the directory exists.
		let directory = config.directory.clone();
		tokio::fs::create_dir_all(&directory)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
		let directory = tokio::fs::canonicalize(&directory).await.map_err(
			|source| tg::error!(!source, %path = directory.display(), "failed to canonicalize directory path"),
		)?;

		// Lock the lock file.
		let lock_path = directory.join("lock");
		let mut lock_file = tokio::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(false)
			.open(lock_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the lock file"))?;
		let ret = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
		if ret != 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to lock the lock file"
			));
		}
		let pid = std::process::id();
		lock_file
			.set_len(0)
			.await
			.map_err(|source| tg::error!(!source, "failed to truncate the lock file"))?;
		lock_file
			.write_all(pid.to_string().as_bytes())
			.await
			.map_err(|source| tg::error!(!source, "failed to write the pid to the lock file"))?;
		let lock_file = Mutex::new(Some(lock_file));

		// Verify the version file.
		let version_path = directory.join("version");
		let version = match tokio::fs::read_to_string(&version_path).await {
			Ok(string) => Some(
				string
					.parse::<u64>()
					.ok()
					.ok_or_else(|| tg::error!("invalid version file"))?,
			),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
			Err(error) => {
				return Err(tg::error!(!error, "failed to read the version file"));
			},
		};
		match version {
			Some(0) => (),
			Some(_) => {
				return Err(tg::error!(
					"the data directory was created with a newer version of tangram"
				));
			},
			None => {
				tokio::fs::write(&version_path, b"0")
					.await
					.map_err(|source| tg::error!(!source, "failed to write the version file"))?;
			},
		}

		// Ensure the logs directory exists.
		let logs_path = directory.join("logs");
		tokio::fs::create_dir_all(&logs_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the logs directory"))?;

		// Ensure the temp directory exists.
		let temp_path = directory.join("tmp");
		tokio::fs::create_dir_all(&temp_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the temp directory"))?;

		// Remove an existing socket file.
		let socket_path = directory.join("socket");
		tokio::fs::remove_file(&socket_path).await.ok();

		// Create the artifact cache task map.
		let cache_task_map = TaskMap::default();

		// Create the HTTP configuration.
		let http = config.http.as_ref().map(|config| {
			let url = config.url.clone().unwrap_or_else(|| {
				let path = directory.join("socket");
				let path = path.to_str().unwrap();
				let path = urlencoding::encode(path);
				format!("http+unix://{path}").parse().unwrap()
			});
			Http { url }
		});

		// Create the import index tasks.
		let import_index_tasks = Mutex::new(Some(JoinSet::new()));

		// Create the process permits.
		let process_permits = DashMap::default();

		// Create the process semaphore.
		let permits = config
			.runner
			.as_ref()
			.map_or(0, |process| process.concurrency);
		let process_semaphore = Arc::new(tokio::sync::Semaphore::new(permits));

		// Create the process task map.
		let process_task_map = TaskMap::default();

		// Create the compilers.
		#[cfg(feature = "v8")]
		let compilers = RwLock::new(Vec::new());

		// Create the database.
		let database = match &config.database {
			self::config::Database::Sqlite(config) => {
				let initialize = Arc::new(self::database::initialize);
				let options = db::sqlite::DatabaseOptions {
					connections: config.connections,
					initialize,
					path: config.path.clone(),
				};
				let database = db::sqlite::Database::new(options)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the database"))?;
				Database::Sqlite(database)
			},
			self::config::Database::Postgres(options) => {
				#[cfg(not(feature = "postgres"))]
				{
					let _ = options;
					return Err(tg::error!(
						"this version of tangram was not compiled with postgres support"
					));
				}
				#[cfg(feature = "postgres")]
				{
					let options = db::postgres::DatabaseOptions {
						connections: options.connections,
						url: options.url.clone(),
					};
					let database = db::postgres::Database::new(options)
						.await
						.map_err(|source| tg::error!(!source, "failed to create the database"))?;
					Database::Postgres(database)
				}
			},
		};

		// Create the diagnostics.
		let diagnostics = Mutex::new(Vec::new());

		// Create the index.
		let index = match &config.index {
			self::config::Index::Postgres(options) => {
				#[cfg(not(feature = "postgres"))]
				{
					let _ = options;
					return Err(tg::error!(
						"this version of tangram was not compiled with postgres support"
					));
				}
				#[cfg(feature = "postgres")]
				{
					let options = db::postgres::DatabaseOptions {
						url: options.url.clone(),
						connections: options.connections,
					};
					let database = db::postgres::Database::new(options)
						.await
						.map_err(|source| tg::error!(!source, "failed to create the index"))?;
					Index::Postgres(database)
				}
			},
			self::config::Index::Sqlite(options) => {
				let initialize = Arc::new(|connection: &sqlite::Connection| {
					connection.pragma_update(None, "auto_vaccum", "incremental")?;
					connection.pragma_update(None, "busy_timeout", "5000")?;
					connection.pragma_update(None, "cache_size", "-20000")?;
					connection.pragma_update(None, "foreign_keys", "on")?;
					connection.pragma_update(None, "journal_mode", "wal")?;
					connection.pragma_update(None, "mmap_size", "2147483648")?;
					connection.pragma_update(None, "recursive_triggers", "on")?;
					connection.pragma_update(None, "synchronous", "normal")?;
					connection.pragma_update(None, "temp_store", "memory")?;
					Ok(())
				});
				let options = db::sqlite::DatabaseOptions {
					connections: options.connections,
					initialize,
					path: directory.join("index"),
				};
				let database = db::sqlite::Database::new(options)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the index"))?;
				Index::Sqlite(database)
			},
		};

		// Create the messenger.
		let messenger = match &config.messenger {
			self::config::Messenger::Memory => {
				Messenger::Memory(tangram_messenger::memory::Messenger::new())
			},
			self::config::Messenger::Nats(nats) => {
				#[cfg(not(feature = "nats"))]
				{
					let _ = nats;
					return Err(tg::error!(
						"this version of tangram was not compiled with nats support"
					));
				}
				#[cfg(feature = "nats")]
				{
					let client = nats::connect(nats.url.to_string())
						.await
						.map_err(|source| {
							tg::error!(!source, "failed to create the NATS client")
						})?;
					Messenger::Nats(tangram_messenger::nats::Messenger::new(client))
				}
			},
		};

		// Create the index stream and consumer if the messenger is memory.
		if messenger.is_memory() {
			let stream_config = tangram_messenger::StreamConfig {
				discard: tangram_messenger::DiscardPolicy::New,
				max_bytes: None,
				max_messages: None,
				retention: tangram_messenger::RetentionPolicy::WorkQueue,
			};
			let stream = messenger
				.create_stream("index".to_owned(), stream_config)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to ensure the index stream exists")
				})?;
			let consumer_config = tangram_messenger::ConsumerConfig {
				deliver: tangram_messenger::DeliverPolicy::All,
			};
			stream
				.create_consumer("index".to_owned(), consumer_config)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to ensure the index stream exists")
				})?;
		}

		// Create the remotes.
		let remotes = DashMap::default();

		// Create the runtimes.
		let runtimes = RwLock::new(HashMap::default());

		// Create the store.
		let store = match &config.store {
			config::Store::Memory => Store::new_memory(),
			config::Store::Fdb(fdb) => {
				#[cfg(not(feature = "foundationdb"))]
				{
					let _ = fdb;
					return Err(tg::error!(
						"this version of tangram was not compiled with foundationdb support"
					));
				}
				#[cfg(feature = "foundationdb")]
				{
					Store::new_fdb(fdb)?
				}
			},
			config::Store::Lmdb(lmdb) => Store::new_lmdb(lmdb)?,
			config::Store::S3(s3) => Store::new_s3(s3),
		};

		// Create the task.
		let task = Mutex::new(None);

		// Create the temp paths.
		let temp_paths = DashSet::default();

		// Get the version.
		let version = config
			.version
			.clone()
			.unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());

		// Create the vfs.
		let vfs = Mutex::new(None);

		let pipes = DashMap::new();
		let ptys = DashMap::new();

		// Create the server.
		let server = Self(Arc::new(Inner {
			cache_task_map,
			#[cfg(feature = "v8")]
			compilers,
			config,
			database,
			diagnostics,
			http,
			import_index_tasks,
			index,
			lock_file,
			messenger,
			path: directory,
			pipes,
			process_permits,
			process_semaphore,
			process_task_map,
			ptys,
			remotes,
			runtimes,
			store,
			task,
			temp_paths,
			version,
			vfs,
		}));

		// Migrate the database.
		self::database::migrate(&server.database)
			.await
			.map_err(|source| tg::error!(!source, "failed to migrate the database"))?;

		// Migrate the index.
		if let Ok(database) = server.index.try_unwrap_sqlite_ref() {
			self::index::sqlite::migrate(database)
				.await
				.map_err(|source| tg::error!(!source, "failed to migrate the index"))?;
		}

		// Set the remotes if specified in the config.
		if let Some(remotes) = &server.config.remotes {
			let connection = server
				.database
				.write_connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
			let statement = indoc!(
				"
					delete from remotes;
				",
			);
			let params = db::params![];
			connection
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to delete the remotes"))?;
			for remote in remotes {
				let p = connection.p();
				let statement = formatdoc!(
					"
						insert into remotes (name, url)
						values ({p}1, {p}2);
					",
				);
				let params = db::params![&remote.name, &remote.url];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to insert the remote"))?;
			}
		}

		// Start the VFS if enabled.
		let artifacts_path = server.path.join("artifacts");
		let cache_path = server.path.join("cache");
		let artifacts_exists = match tokio::fs::try_exists(&artifacts_path).await {
			Ok(exists) => exists,
			Err(error) if error.raw_os_error() == Some(libc::ENOTCONN) => {
				if cfg!(target_os = "macos") {
					tangram_vfs::nfs::unmount(&artifacts_path)
						.await
						.map_err(|source| tg::error!(!source, "failed to unmount"))?;
				} else if cfg!(target_os = "linux") {
					tangram_vfs::fuse::unmount(&artifacts_path)
						.await
						.map_err(|source| tg::error!(!source, "failed to unmount"))?;
				} else {
					return Err(tg::error!("unsupported operating system"));
				}
				true
			},
			Err(source) => {
				return Err(tg::error!(!source, "failed to stat the path"));
			},
		};
		let cache_exists = tokio::fs::try_exists(&cache_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to stat the path"))?;
		if let Some(options) = server.config.vfs {
			if artifacts_exists && !cache_exists {
				tokio::fs::rename(&artifacts_path, &cache_path)
					.await
					.map_err(|source| {
						tg::error!(
							!source,
							"failed to move the artifacts directory to the cache path"
						)
					})?;
			}
			tokio::fs::create_dir_all(&artifacts_path)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to create the artifacts directory")
				})?;
			tokio::fs::create_dir_all(&cache_path)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the cache directory"))?;
			let kind = if cfg!(target_os = "macos") {
				vfs::Kind::Nfs
			} else if cfg!(target_os = "linux") {
				vfs::Kind::Fuse
			} else {
				unreachable!()
			};
			let artifacts_path = server.artifacts_path();
			let vfs = self::vfs::Server::start(&server, kind, &artifacts_path, options)
				.await
				.inspect_err(|source| {
					tracing::error!(?source, "failed to start the VFS");
				})
				.ok();
			if let Some(vfs) = vfs {
				server.vfs.lock().unwrap().replace(vfs);
			}
		} else {
			if cache_exists {
				tokio::fs::rename(&cache_path, &artifacts_path)
					.await
					.map_err(|source| {
						tg::error!(
							!source,
							"failed to move the artifacts directory to the cache directory"
						)
					})?;
			}
			tokio::fs::create_dir_all(&artifacts_path)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to create the artifacts directory")
				})?;
		}

		// Add the runtimes if the runner is enabled.
		if server.config.runner.is_some() {
			{
				let triple = "builtin".to_owned();
				let runtime = self::runtime::builtin::Runtime::new(&server);
				let runtime = self::runtime::Runtime::Builtin(runtime);
				server.runtimes.write().unwrap().insert(triple, runtime);
			}
			#[cfg(feature = "v8")]
			{
				let triple = "js".to_owned();
				let runtime = self::runtime::js::Runtime::new(&server);
				let runtime = self::runtime::Runtime::Js(runtime);
				server.runtimes.write().unwrap().insert(triple, runtime);
			}
			#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
			{
				let triple = "aarch64-darwin".to_owned();
				let runtime = self::runtime::darwin::Runtime::new(&server);
				let runtime = self::runtime::Runtime::Darwin(runtime);
				server.runtimes.write().unwrap().insert(triple, runtime);
			}
			#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
			{
				let triple = "aarch64-linux".to_owned();
				let runtime = self::runtime::linux::Runtime::new(&server).await?;
				let runtime = self::runtime::Runtime::Linux(runtime);
				server.runtimes.write().unwrap().insert(triple, runtime);
			}
			#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
			{
				let triple = "x86_64-darwin".to_owned();
				let runtime = self::runtime::darwin::Runtime::new(&server);
				let runtime = self::runtime::Runtime::Darwin(runtime);
				server.runtimes.write().unwrap().insert(triple, runtime);
			}
			#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
			{
				let triple = "x86_64-linux".to_owned();
				let runtime = self::runtime::linux::Runtime::new(&server)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the linux runtime"))?;
				let runtime = self::runtime::Runtime::Linux(runtime);
				server.runtimes.write().unwrap().insert(triple, runtime);
			}
		}

		// Spawn the diagnostics task.
		let diagnostics_task = Some(tokio::spawn({
			let server = server.clone();
			async move {
				server
					.diagnostics_task()
					.await
					.inspect_err(|error| tracing::error!(?error))
					.ok();
			}
		}));

		// Spawn the cleaner task.
		let cleaner_task = server.config.cleaner.clone().map(|config| {
			tokio::spawn({
				let server = server.clone();
				async move {
					server
						.cleaner_task(&config)
						.await
						.inspect_err(|error| {
							tracing::error!(?error);
						})
						.ok();
				}
			})
		});

		// Spawn the indexer task.
		let indexer_task = server.config.indexer.clone().map(|config| {
			tokio::spawn({
				let server = server.clone();
				async move {
					server
						.indexer_task(&config)
						.await
						.inspect_err(|error| {
							tracing::error!(?error);
						})
						.ok();
				}
			})
		});

		// Spawn the watchdog task.
		let watchdog_task = server.config.watchdog.as_ref().map(|config| {
			tokio::spawn({
				let server = server.clone();
				let config = config.clone();
				async move {
					server
						.watchdog_task(&config)
						.await
						.inspect_err(|error| tracing::error!(?error, "the watchdog task failed"))
						.ok();
				}
			})
		});

		// Spawn the runner task.
		let runner_task = if server.config.runner.is_some() {
			Some(tokio::spawn({
				let server = server.clone();
				async move {
					server.runner_task().await;
				}
			}))
		} else {
			None
		};

		// Spawn the HTTP task.
		let http_task = if let Some(http) = &server.http {
			let listener = Self::listen(&http.url).await?;
			tracing::trace!("listening on {}", http.url);
			Some(Task::spawn(|stop| {
				let server = server.clone();
				async move {
					Self::serve(server.clone(), listener, stop).await;
				}
			}))
		} else {
			None
		};

		let shutdown = {
			let server = server.clone();
			async move {
				tracing::trace!("started shutdown");

				// Abort the runner task.
				if let Some(task) = runner_task {
					task.abort();
					let result = task.await;
					if let Err(error) = result {
						if !error.is_cancelled() {
							tracing::error!(?error, "the runner task panicked");
						}
					}
					tracing::trace!("shutdown runner task");
				}

				// Abort the process tasks.
				server.process_task_map.abort_all();
				let results = server.process_task_map.wait().await;
				for result in results {
					if let Err(error) = result {
						if !error.is_cancelled() {
							tracing::error!(?error, "a process task panicked");
						}
					}
				}
				tracing::trace!("shutdown process tasks");

				// Stop the compilers.
				#[cfg(feature = "v8")]
				{
					let compilers = server.compilers.read().unwrap().clone();
					for compiler in compilers {
						compiler.stop();
						compiler.wait().await;
					}
					tracing::trace!("shutdown compiler tasks");
				}

				// Stop the HTTP task.
				if let Some(task) = http_task {
					task.stop();
					let result = task.wait().await;
					if let Err(error) = result {
						if !error.is_cancelled() {
							tracing::error!(?error, "the http task panicked");
						}
					}
					tracing::trace!("shutdown http task");
				}

				// Abort the diagnostics task.
				if let Some(task) = diagnostics_task {
					task.abort();
				}

				// Abort the cleaner task.
				if let Some(task) = cleaner_task {
					task.abort();
					let result = task.await;
					if let Err(error) = result {
						if !error.is_cancelled() {
							tracing::error!(?error, "the clean task panicked");
						}
					}
					tracing::trace!("shutdown cleaner task");
				}

				// Abort the indexer task.
				if let Some(task) = indexer_task {
					task.abort();
					let result = task.await;
					if let Err(error) = result {
						if !error.is_cancelled() {
							tracing::error!(?error, "the index task panicked");
						}
					}
					tracing::trace!("shutdown indexer task");
				}

				// Abort the watchdog task.
				if let Some(task) = watchdog_task {
					task.abort();
					let result = task.await;
					if let Err(error) = result {
						if !error.is_cancelled() {
							tracing::error!(?error, "the watchdog task panicked");
						}
					}
					tracing::trace!("shutdown watchdog task");
				}

				// Remove the runtimes.
				server.runtimes.write().unwrap().clear();

				// Abort the artifact cache tasks.
				server.cache_task_map.abort_all();
				let results = server.cache_task_map.wait().await;
				for result in results {
					if let Err(error) = result {
						if !error.is_cancelled() {
							tracing::error!(?error, "an artifact cache task panicked");
						}
					}
				}
				tracing::trace!("shutdown artifact cache tasks");

				// Await the import index tasks.
				let import_index_tasks = server.import_index_tasks.lock().unwrap().take();
				if let Some(import_index_tasks) = import_index_tasks {
					import_index_tasks.join_all().await;
				}

				// Stop the VFS.
				let vfs = server.vfs.lock().unwrap().take();
				if let Some(vfs) = vfs {
					vfs.stop();
					vfs.wait().await;
					tracing::trace!("shutdown vfs task");
				}

				// Remove the temp paths.
				server
					.temp_paths
					.iter()
					.map(|entry| remove(entry.key().clone()).map(|_| ()))
					.collect::<FuturesUnordered<_>>()
					.collect::<()>()
					.await;
				tracing::trace!("removed temps");

				// Unlock the lock file.
				let lock_file = server.lock_file.lock().unwrap().take();
				if let Some(lock_file) = lock_file {
					lock_file.set_len(0).await.ok();
					tracing::trace!("released lock file");
				}

				tracing::trace!("finished shutdown");
			}
		};

		// Spawn the task.
		let task = Task::spawn(|stop| async move {
			stop.wait().await;
			shutdown.await;
		});
		server.task.lock().unwrap().replace(task);

		Ok(server)
	}

	pub fn stop(&self) {
		self.task.lock().unwrap().as_ref().unwrap().stop();
	}

	pub async fn wait(&self) {
		let task = self.task.lock().unwrap().clone().unwrap();
		task.wait().await.unwrap();
	}

	#[must_use]
	pub fn config(&self) -> &Config {
		&self.config
	}

	#[must_use]
	pub fn url(&self) -> Option<&Url> {
		self.http.as_ref().map(|http| &http.url)
	}

	#[must_use]
	pub fn artifacts_path(&self) -> PathBuf {
		self.path.join("artifacts")
	}

	#[must_use]
	pub fn cache_path(&self) -> PathBuf {
		if self.vfs.lock().unwrap().is_some() {
			self.path.join("cache")
		} else {
			self.artifacts_path()
		}
	}

	#[must_use]
	pub fn database_path(&self) -> PathBuf {
		self.path.join("database")
	}

	#[must_use]
	pub fn index_path(&self) -> PathBuf {
		self.path.join("index")
	}

	#[must_use]
	pub fn logs_path(&self) -> PathBuf {
		self.path.join("logs")
	}

	#[must_use]
	pub fn temp_path(&self) -> PathBuf {
		self.path.join("tmp")
	}
}

impl Deref for Server {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
