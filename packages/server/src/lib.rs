use self::{
	database::Database, messenger::Messenger, runtime::Runtime, store::Store, util::fs::remove,
};
use async_nats as nats;
use compiler::Compiler;
use dashmap::{DashMap, DashSet};
use futures::{FutureExt as _, Stream, StreamExt as _, future, stream::FuturesUnordered};
use http_body_util::BodyExt as _;
use hyper_util::rt::{TokioExecutor, TokioIo};
use indoc::{formatdoc, indoc};
use itertools::Itertools as _;
use rusqlite as sqlite;
use std::{
	collections::HashMap,
	convert::Infallible,
	ops::Deref,
	os::fd::AsRawFd as _,
	path::{Path, PathBuf},
	pin::{Pin, pin},
	sync::{Arc, Mutex, RwLock},
	time::Duration,
};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_futures::task::{Stop, Task, TaskMap};
use tangram_http::{Body, response::builder::Ext as _};
use tangram_messenger::prelude::*;
use tokio::{
	io::{AsyncBufRead, AsyncRead, AsyncWrite, AsyncWriteExt as _},
	net::{TcpListener, UnixListener},
};
use tower::ServiceExt as _;
use tower_http::ServiceBuilderExt as _;
use url::Url;

mod blob;
mod cache;
mod check;
mod checkin;
mod checkout;
mod checksum;
mod clean;
mod compiler;
mod database;
mod document;
mod export;
mod format;
mod health;
mod import;
mod index;
mod lockfile;
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
pub mod test;

#[derive(Clone)]
pub struct Server(pub Arc<Inner>);

pub struct Inner {
	cache_task_map: ArtifactCacheTaskMap,
	compilers: RwLock<Vec<Compiler>>,
	config: Config,
	database: Database,
	diagnostics: Mutex<Vec<tg::Diagnostic>>,
	file_descriptor_semaphore: tokio::sync::Semaphore,
	http: Option<Http>,
	index: Database,
	local_pool_handle: Option<tokio_util::task::LocalPoolHandle>,
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

type ArtifactCacheTaskMap = TaskMap<tg::artifact::Id, tg::Result<()>, fnv::FnvBuildHasher>;

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
		let compilers = RwLock::new(Vec::new());

		// Create the database.
		let database = match &config.database {
			self::config::Database::Sqlite(options) => {
				let initialize = Arc::new(self::database::initialize);
				let options = db::sqlite::DatabaseOptions {
					connections: options.connections,
					initialize,
					path: directory.join("database"),
				};
				let database = db::sqlite::Database::new(options)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the database"))?;
				Either::Left(database)
			},
			self::config::Database::Postgres(options) => {
				let options = db::postgres::DatabaseOptions {
					url: options.url.clone(),
					connections: options.connections,
				};
				let database = db::postgres::Database::new(options)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the database"))?;
				Either::Right(database)
			},
		};

		// Create the diagnostics.
		let diagnostics = Mutex::new(Vec::new());

		// Create the file system semaphore.
		let file_descriptor_semaphore =
			tokio::sync::Semaphore::new(config.advanced.file_descriptor_semaphore_size);

		// Create the index.
		let index = match &config.index {
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
				Either::Left(database)
			},
			self::config::Index::Postgres(options) => {
				let options = db::postgres::DatabaseOptions {
					url: options.url.clone(),
					connections: options.connections,
				};
				let database = db::postgres::Database::new(options)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the index"))?;
				Either::Right(database)
			},
		};

		// Create the local pool handle.
		let local_pool_handle = config
			.runner
			.as_ref()
			.map(|process| tokio_util::task::LocalPoolHandle::new(process.concurrency));

		// Create the messenger.
		let messenger = match &config.messenger {
			self::config::Messenger::Memory => {
				Messenger::Left(tangram_messenger::memory::Messenger::new())
			},
			self::config::Messenger::Nats(nats) => {
				let client = nats::connect(nats.url.to_string())
					.await
					.map_err(|source| tg::error!(!source, "failed to create the NATS client"))?;
				Messenger::Right(tangram_messenger::nats::Messenger::new(client))
			},
		};

		// Get or create the index stream and consumer.
		let stream_config = tangram_messenger::StreamConfig {
			discard: tangram_messenger::DiscardPolicy::New,
			max_bytes: None,
			max_messages: None,
			retention: tangram_messenger::RetentionPolicy::WorkQueue,
		};
		let stream = messenger
			.get_or_create_stream("index".to_owned(), stream_config)
			.await
			.map_err(|source| tg::error!(!source, "failed to ensure the index stream exists"))?;
		let consumer_config = tangram_messenger::ConsumerConfig {
			deliver: tangram_messenger::DeliverPolicy::All,
		};
		stream
			.get_or_create_consumer("index".to_owned(), consumer_config)
			.await
			.map_err(|source| tg::error!(!source, "failed to ensure the index stream exists"))?;

		// Create the remotes.
		let remotes = DashMap::default();

		// Create the runtimes.
		let runtimes = RwLock::new(HashMap::default());

		// Create the store.
		let store = match &config.store {
			config::Store::Memory => Store::new_memory(),
			#[cfg(feature = "foundationdb")]
			config::Store::Fdb(fdb) => Store::new_fdb(fdb)?,
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
			compilers,
			config,
			database,
			diagnostics,
			file_descriptor_semaphore,
			http,
			index,
			local_pool_handle,
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
		self::index::migrate(&server.index)
			.await
			.map_err(|source| tg::error!(!source, "failed to migrate the index"))?;

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
			Err(source) => return Err(tg::error!(!source, "failed to stat the path")),
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
				let compilers = server.compilers.read().unwrap().clone();
				for compiler in compilers {
					compiler.stop();
					compiler.wait().await;
				}
				tracing::trace!("shutdown compiler tasks");

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

				// Release the lock file.
				let lock_file = server.lock_file.lock().unwrap().take();
				if let Some(lock_file) = lock_file {
					lock_file.set_len(0).await.ok();
					tracing::trace!("released lockfile");
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

impl Server {
	async fn listen(
		url: &Url,
	) -> tg::Result<tokio_util::either::Either<tokio::net::UnixListener, tokio::net::TcpListener>>
	{
		let listener = match url.scheme() {
			"http+unix" => {
				let path = url.host_str().ok_or_else(|| tg::error!("invalid url"))?;
				let path = urlencoding::decode(path)
					.map_err(|source| tg::error!(!source, "invalid url"))?;
				let path = Path::new(path.as_ref());
				let listener = UnixListener::bind(path).map_err(
					|source| tg::error!(!source, %path = path.display(), "failed to bind"),
				)?;
				tokio_util::either::Either::Left(listener)
			},
			"http" => {
				let host = url.host().ok_or_else(|| tg::error!("invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!("invalid url"))?;
				let listener = TcpListener::bind(format!("{host}:{port}"))
					.await
					.map_err(|source| tg::error!(!source, "failed to bind"))?;
				tokio_util::either::Either::Right(listener)
			},
			_ => {
				return Err(tg::error!("invalid url"));
			},
		};
		Ok(listener)
	}

	async fn serve<H>(
		handle: H,
		listener: tokio_util::either::Either<tokio::net::UnixListener, tokio::net::TcpListener>,
		stop: Stop,
	) where
		H: tg::Handle,
	{
		// Create the task tracker.
		let task_tracker = tokio_util::task::TaskTracker::new();

		loop {
			// Accept a new connection.
			let accept = async {
				let stream = match &listener {
					tokio_util::either::Either::Left(listener) => {
						tokio_util::either::Either::Left(listener.accept().await?.0)
					},
					tokio_util::either::Either::Right(listener) => {
						tokio_util::either::Either::Right(listener.accept().await?.0)
					},
				};
				Ok::<_, std::io::Error>(TokioIo::new(stream))
			};
			let stream = match future::select(pin!(accept), pin!(stop.wait())).await {
				future::Either::Left((result, _)) => match result {
					Ok(stream) => stream,
					Err(error) => {
						tracing::error!(?error, "failed to accept a connection");
						continue;
					},
				},
				future::Either::Right(((), _)) => {
					break;
				},
			};

			// Create the service.
			let idle = tangram_http::idle::Idle::new(Duration::from_secs(30));
			let service = tower::ServiceBuilder::new()
				.layer(tangram_http::layer::tracing::TracingLayer::new())
				.layer(tower_http::timeout::TimeoutLayer::new(Duration::from_secs(
					60,
				)))
				.add_extension(stop.clone())
				.map_response_body({
					let idle = idle.clone();
					move |body: Body| Body::new(tangram_http::idle::Body::new(idle.token(), body))
				})
				.layer(tangram_http::layer::compression::RequestDecompressionLayer)
				.layer(
					tangram_http::layer::compression::ResponseCompressionLayer::new(
						|accept_encoding, parts, _| {
							let has_content_length =
								parts.headers.get(http::header::CONTENT_LENGTH).is_some();
							let is_import_or_export = parts
								.headers
								.get(http::header::CONTENT_TYPE)
								.is_some_and(|content_type| {
									matches!(
										content_type.to_str(),
										Ok(tg::import::CONTENT_TYPE | tg::export::CONTENT_TYPE)
									)
								});
							if (has_content_length || is_import_or_export)
								&& accept_encoding.is_some_and(|accept_encoding| {
									accept_encoding.preferences.iter().any(|preference| {
										preference.encoding == tangram_http::header::content_encoding::ContentEncoding::Zstd
									})
								}) {
								Some((tangram_http::body::compression::Algorithm::Zstd, 3))
							} else {
								None
							}
						},
					),
				)
				.service_fn({
					let handle = handle.clone();
					move |request| {
						let handle = handle.clone();
						async move {
							let response = Self::handle_request(&handle, request).await;
							Ok::<_, Infallible>(response)
						}
					}
				});

			// Spawn a task to serve the connection.
			task_tracker.spawn({
				let idle = idle.clone();
				let stop = stop.clone();
				async move {
					let mut builder =
						hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
					builder.http2().max_concurrent_streams(None);
					let service =
						service.map_request(|request: http::Request<hyper::body::Incoming>| {
							request.map(Body::new)
						});
					let service = hyper_util::service::TowerToHyperService::new(service);
					let connection = builder.serve_connection_with_upgrades(stream, service);
					let result = match future::select(
						pin!(connection),
						future::select(pin!(idle.wait()), pin!(stop.wait())),
					)
					.await
					{
						future::Either::Left((result, _)) => result,
						future::Either::Right((_, mut connection)) => {
							connection.as_mut().graceful_shutdown();
							connection.await
						},
					};
					result.ok();
				}
			});
		}

		// Wait for all tasks to complete.
		task_tracker.close();
		task_tracker.wait().await;
	}

	async fn handle_request<H>(handle: &H, mut request: http::Request<Body>) -> http::Response<Body>
	where
		H: tg::Handle,
	{
		let id = tg::Id::new_uuidv7(tg::id::Kind::Request);
		request.extensions_mut().insert(id.clone());

		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect_vec();
		let response = match (method, path_components.as_slice()) {
			(http::Method::POST, ["check"]) => Self::handle_check_request(handle, request).boxed(),
			(http::Method::POST, ["checkin"]) => {
				Self::handle_checkin_request(handle, request).boxed()
			},
			(http::Method::POST, ["checkout"]) => {
				Self::handle_checkout_request(handle, request).boxed()
			},
			(http::Method::POST, ["clean"]) => {
				Self::handle_server_clean_request(handle, request).boxed()
			},
			(http::Method::POST, ["document"]) => {
				Self::handle_document_request(handle, request).boxed()
			},
			(http::Method::POST, ["export"]) => {
				Self::handle_export_request(handle, request).boxed()
			},
			(http::Method::POST, ["format"]) => {
				Self::handle_format_request(handle, request).boxed()
			},
			(http::Method::GET, ["health"]) => {
				Self::handle_server_health_request(handle, request).boxed()
			},
			(http::Method::POST, ["import"]) => {
				Self::handle_import_request(handle, request).boxed()
			},
			(http::Method::POST, ["index"]) => Self::handle_index_request(handle, request).boxed(),
			(http::Method::POST, ["lsp"]) => Self::handle_lsp_request(handle, request).boxed(),
			(http::Method::POST, ["pull"]) => Self::handle_pull_request(handle, request).boxed(),
			(http::Method::POST, ["push"]) => Self::handle_push_request(handle, request).boxed(),
			(http::Method::POST, ["blobs"]) => Self::handle_blob_request(handle, request).boxed(),
			(http::Method::GET, ["blobs", blob, "read"]) => {
				Self::handle_read_request(handle, request, blob).boxed()
			},
			(http::Method::GET, ["_", path @ ..]) => {
				Self::handle_get_request(handle, request, path).boxed()
			},

			// Objects.
			(http::Method::GET, ["objects", object, "metadata"]) => {
				Self::handle_get_object_metadata_request(handle, request, object).boxed()
			},
			(http::Method::GET, ["objects", object]) => {
				Self::handle_get_object_request(handle, request, object).boxed()
			},
			(http::Method::PUT, ["objects", object]) => {
				Self::handle_put_object_request(handle, request, object).boxed()
			},
			(http::Method::POST, ["objects", object, "touch"]) => {
				Self::handle_touch_object_request(handle, request, object).boxed()
			},

			// Processes.
			(http::Method::POST, ["processes", "spawn"]) => {
				Self::handle_spawn_process_request(handle, request).boxed()
			},
			(http::Method::GET, ["processes", process, "metadata"]) => {
				Self::handle_get_process_metadata_request(handle, request, process).boxed()
			},
			(http::Method::GET, ["processes", process]) => {
				Self::handle_get_process_request(handle, request, process).boxed()
			},
			(http::Method::PUT, ["processes", process]) => {
				Self::handle_put_process_request(handle, request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "cancel"]) => {
				Self::handle_cancel_process_request(handle, request, process).boxed()
			},
			(http::Method::POST, ["processes", "dequeue"]) => {
				Self::handle_dequeue_process_request(handle, request).boxed()
			},
			(http::Method::POST, ["processes", process, "start"]) => {
				Self::handle_start_process_request(handle, request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "signal"]) => {
				Self::handle_post_process_signal_request(handle, request, process).boxed()
			},
			(http::Method::GET, ["processes", process, "signal"]) => {
				Self::handle_get_process_signal_request(handle, request, process).boxed()
			},
			(http::Method::GET, ["processes", process, "status"]) => {
				Self::handle_get_process_status_request(handle, request, process).boxed()
			},
			(http::Method::GET, ["processes", process, "children"]) => {
				Self::handle_get_process_children_request(handle, request, process).boxed()
			},
			(http::Method::GET, ["processes", process, "log"]) => {
				Self::handle_get_process_log_request(handle, request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "log"]) => {
				Self::handle_post_process_log_request(handle, request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "finish"]) => {
				Self::handle_finish_process_request(handle, request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "touch"]) => {
				Self::handle_touch_process_request(handle, request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "heartbeat"]) => {
				Self::handle_heartbeat_process_request(handle, request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "wait"]) => {
				Self::handle_post_process_wait_request(handle, request, process).boxed()
			},

			// Pipes.
			(http::Method::POST, ["pipes"]) => {
				Self::handle_create_pipe_request(handle, request).boxed()
			},
			(http::Method::POST, ["pipes", pipe, "close"]) => {
				Self::handle_close_pipe_request(handle, request, pipe).boxed()
			},
			(http::Method::GET, ["pipes", pipe, "read"]) => {
				Self::handle_read_pipe_request(handle, request, pipe).boxed()
			},
			(http::Method::POST, ["pipes", pipe, "write"]) => {
				Self::handle_write_pipe_request(handle, request, pipe).boxed()
			},

			// Ptys.
			(http::Method::POST, ["ptys"]) => {
				Self::handle_create_pty_request(handle, request).boxed()
			},
			(http::Method::POST, ["ptys", pty, "close"]) => {
				Self::handle_close_pty_request(handle, request, pty).boxed()
			},
			(http::Method::GET, ["ptys", pty, "size"]) => {
				Self::handle_get_pty_size_request(handle, request, pty).boxed()
			},
			(http::Method::GET, ["ptys", pty, "read"]) => {
				Self::handle_read_pty_request(handle, request, pty).boxed()
			},
			(http::Method::POST, ["ptys", pty, "write"]) => {
				Self::handle_write_pty_request(handle, request, pty).boxed()
			},

			// Remotes.
			(http::Method::GET, ["remotes"]) => {
				Self::handle_list_remotes_request(handle, request).boxed()
			},
			(http::Method::GET, ["remotes", name]) => {
				Self::handle_get_remote_request(handle, request, name).boxed()
			},
			(http::Method::PUT, ["remotes", name]) => {
				Self::handle_put_remote_request(handle, request, name).boxed()
			},
			(http::Method::DELETE, ["remotes", name]) => {
				Self::handle_delete_remote_request(handle, request, name).boxed()
			},

			// Tags.
			(http::Method::GET, ["tags"]) => {
				Self::handle_list_tags_request(handle, request).boxed()
			},
			(http::Method::GET, ["tags", pattern @ ..]) => {
				Self::handle_get_tag_request(handle, request, pattern).boxed()
			},
			(http::Method::PUT, ["tags", tag @ ..]) => {
				Self::handle_put_tag_request(handle, request, tag).boxed()
			},
			(http::Method::DELETE, ["tags", tag @ ..]) => {
				Self::handle_delete_tag_request(handle, request, tag).boxed()
			},

			// Users.
			(http::Method::GET, ["user"]) => Self::handle_get_user_request(handle, request).boxed(),

			(_, _) => future::ok(
				http::Response::builder()
					.status(http::StatusCode::NOT_FOUND)
					.bytes("not found")
					.unwrap(),
			)
			.boxed(),
		}
		.await;

		// Handle an error.
		let mut response = response.unwrap_or_else(|error| {
			tracing::error!(?error);
			let bytes = serde_json::to_string(&error.to_data())
				.ok()
				.unwrap_or_default();
			http::Response::builder()
				.status(http::StatusCode::INTERNAL_SERVER_ERROR)
				.bytes(bytes)
				.unwrap()
		});

		// Add the request ID to the response.
		let key = http::HeaderName::from_static("x-tg-request-id");
		let value = http::HeaderValue::from_str(&id.to_string()).unwrap();
		response.headers_mut().insert(key, value);

		response.map(|body| {
			Body::new(body.map_err(|error| {
				tracing::error!(?error, "response body error");
				error
			}))
		})
	}
}

impl tg::Handle for Server {
	fn check(&self, arg: tg::check::Arg) -> impl Future<Output = tg::Result<tg::check::Output>> {
		self.check(arg)
	}

	fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
		>,
	> {
		self.checkin(arg)
	}

	fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
		>,
	> {
		self.checkout(arg)
	}

	fn clean(
		&self,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> + Send {
		self.clean()
	}

	fn document(
		&self,
		arg: tg::document::Arg,
	) -> impl Future<Output = tg::Result<serde_json::Value>> {
		self.document(arg)
	}

	fn export(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Complete>> + Send + 'static>>,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::export::Event>> + Send + 'static>,
	> {
		self.export(arg, stream)
	}

	fn format(&self, arg: tg::format::Arg) -> impl Future<Output = tg::Result<()>> {
		self.format(arg)
	}

	fn health(&self) -> impl Future<Output = tg::Result<tg::Health>> {
		self.health()
	}

	fn import(
		&self,
		arg: tg::import::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::export::Item>> + Send + 'static>>,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::import::Event>> + Send + 'static>,
	> {
		self.import(arg, stream)
	}

	fn index(
		&self,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> + Send {
		self.index()
	}

	fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> impl Future<Output = tg::Result<()>> {
		self.lsp(input, output)
	}

	fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		self.pull(arg)
	}

	fn push(
		&self,
		arg: tg::push::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		self.push(arg)
	}

	fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<tg::blob::create::Output>> {
		self.create_blob(reader)
	}

	fn try_read_blob_stream(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::blob::read::Event>> + Send + 'static>,
		>,
	> {
		self.try_read_blob_stream(id, arg)
	}

	fn try_get(
		&self,
		reference: &tg::Reference,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>>
			+ Send
			+ 'static,
		>,
	> + Send {
		self.try_get(reference)
	}
}

impl tg::handle::Object for Server {
	fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::Metadata>>> {
		self.try_get_object_metadata(id)
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::get::Output>>> {
		self.try_get_object(id)
	}

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_object(id, arg)
	}

	fn touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.touch_object(id, arg)
	}
}

impl tg::handle::Process for Server {
	fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::spawn::Output>>> {
		self.try_spawn_process(arg)
	}

	fn try_wait_process_future(
		&self,
		id: &tg::process::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static,
			>,
		>,
	> {
		self.try_wait_process_future(id)
	}

	fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<tg::process::Metadata>>> {
		self.try_get_process_metadata(id)
	}

	fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<tg::process::get::Output>>> {
		self.try_get_process(id)
	}

	fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_process(id, arg)
	}

	fn cancel_process(
		&self,
		id: &tangram_client::process::Id,
		arg: tangram_client::process::cancel::Arg,
	) -> impl Future<Output = tangram_client::Result<()>> + Send {
		self.cancel_process(id, arg)
	}

	fn try_dequeue_process(
		&self,
		arg: tg::process::dequeue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::dequeue::Output>>> {
		self.try_dequeue_process(arg)
	}

	fn start_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::start::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.start_process(id, arg)
	}

	fn heartbeat_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::process::heartbeat::Output>> {
		self.heartbeat_process(id, arg)
	}

	fn signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.post_process_signal(id, arg)
	}

	fn try_get_process_signal_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::signal::get::Event>> + Send + 'static,
			>,
		>,
	> {
		self.try_get_process_signal_stream(id, arg)
	}

	fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
		>,
	> {
		self.try_get_process_status_stream(id)
	}

	fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static,
			>,
		>,
	> {
		self.try_get_process_children_stream(id, arg)
	}

	fn try_get_process_log_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static>,
		>,
	> {
		self.try_get_process_log_stream(id, arg)
	}

	fn post_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.post_process_log(id, arg)
	}

	fn finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.finish_process(id, arg)
	}

	fn touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.touch_process(id, arg)
	}
}

impl tg::handle::Pipe for Server {
	fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pipe::create::Output>> {
		self.create_pipe(arg)
	}

	fn close_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::close::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.close_pipe(id, arg)
	}

	fn read_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> impl Future<Output = tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>>
	{
		self.read_pipe(id, arg)
	}

	fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::write::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>>,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.write_pipe(id, arg, stream)
	}
}

impl tg::handle::Pty for Server {
	fn create_pty(
		&self,
		arg: tg::pty::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pty::create::Output>> {
		self.create_pty(arg)
	}

	fn close_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::close::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.close_pty(id, arg)
	}

	fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::pty::Size>>> {
		self.get_pty_size(id, arg)
	}

	fn read_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> impl Future<Output = tg::Result<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>>
	{
		self.read_pty(id, arg)
	}

	fn write_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::write::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>>,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.write_pty(id, arg, stream)
	}
}

impl tg::handle::Remote for Server {
	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> impl Future<Output = tg::Result<tg::remote::list::Output>> {
		self.list_remotes(arg)
	}

	fn try_get_remote(
		&self,
		name: &str,
	) -> impl Future<Output = tg::Result<Option<tg::remote::get::Output>>> {
		self.try_get_remote(name)
	}

	fn put_remote(
		&self,
		name: &str,
		arg: tg::remote::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_remote(name, arg)
	}

	fn delete_remote(&self, name: &str) -> impl Future<Output = tg::Result<()>> {
		self.delete_remote(name)
	}
}

impl tg::handle::Tag for Server {
	fn list_tags(
		&self,
		arg: tg::tag::list::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::list::Output>> {
		self.list_tags(arg)
	}

	fn try_get_tag(
		&self,
		pattern: &tg::tag::Pattern,
	) -> impl Future<Output = tg::Result<Option<tg::tag::get::Output>>> {
		self.try_get_tag(pattern)
	}

	fn put_tag(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_tag(tag, arg)
	}

	fn delete_tag(&self, tag: &tg::Tag) -> impl Future<Output = tg::Result<()>> {
		self.delete_tag(tag)
	}
}

impl tg::handle::User for Server {
	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		self.get_user(token)
	}
}

impl Deref for Server {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
