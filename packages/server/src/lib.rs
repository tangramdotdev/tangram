use {
	self::{
		context::Context, database::Database, index::Index, messenger::Messenger, store::Store,
	},
	crate::{temp::Temp, watch::Watch},
	dashmap::{DashMap, DashSet},
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	indoc::{formatdoc, indoc},
	std::{
		ops::Deref,
		os::fd::AsRawFd as _,
		path::PathBuf,
		sync::{Arc, Mutex, OnceLock},
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::task::Task,
	tangram_messenger::prelude::*,
	tangram_uri::Uri,
	tangram_util::fs::remove,
	tokio::io::AsyncWriteExt as _,
	tracing::Instrument as _,
};

mod cache;
mod check;
mod checkin;
mod checkout;
mod checksum;
mod clean;
mod context;
mod database;
mod directory;
mod document;
mod format;
mod get;
mod handle;
mod health;
mod http;
mod index;
mod lsp;
mod messenger;
mod module;
mod object;
mod pipe;
mod process;
mod pty;
mod pull;
mod push;
mod read;
mod remote;
mod run;
mod store;
mod sync;
mod tag;
mod temp;
mod user;
mod vfs;
mod watch;
mod watchdog;
mod write;

pub use self::config::Config;

pub mod config;
pub mod progress;

#[derive(Clone)]
pub struct Owned(Arc<Inner>);

pub struct Inner {
	server: Server,
	task: tangram_futures::task::Shared<()>,
}

#[derive(Clone)]
pub struct Server(Arc<State>);

pub struct State {
	cache_graph_tasks: CacheGraphTasks,
	cache_tasks: CacheTasks,
	checkin_tasks: CheckinTasks,
	config: Config,
	database: Database,
	diagnostics: Mutex<Vec<tg::Diagnostic>>,
	http: Option<Http>,
	index: Index,
	index_tasks: tangram_futures::task::Set<()>,
	library: Mutex<Option<Arc<Temp>>>,
	#[cfg_attr(not(feature = "js"), expect(dead_code))]
	local_pool_handle: OnceLock<tokio_util::task::LocalPoolHandle>,
	lock: Mutex<Option<tokio::fs::File>>,
	messenger: Messenger,
	path: PathBuf,
	pipes: DashMap<tg::pipe::Id, pipe::Pipe, tg::id::BuildHasher>,
	process_permits: ProcessPermits,
	process_semaphore: Arc<tokio::sync::Semaphore>,
	process_tasks: ProcessTasks,
	ptys: DashMap<tg::pty::Id, pty::Pty, tg::id::BuildHasher>,
	remotes: DashMap<String, tg::Client, fnv::FnvBuildHasher>,
	store: Store,
	temps: DashSet<PathBuf, fnv::FnvBuildHasher>,
	version: String,
	vfs: Mutex<Option<self::vfs::Server>>,
	watches: DashMap<PathBuf, Watch, fnv::FnvBuildHasher>,
}

type CacheGraphTasks = tangram_futures::task::Map<
	tg::graph::Id,
	tg::Result<()>,
	crate::progress::Handle<()>,
	tg::id::BuildHasher,
>;

type CacheTasks = tangram_futures::task::Map<
	tg::artifact::Id,
	tg::Result<()>,
	crate::progress::Handle<()>,
	tg::id::BuildHasher,
>;

type CheckinTasks = tangram_futures::task::Map<
	crate::checkin::TaskKey,
	(),
	crate::progress::Handle<crate::checkin::TaskOutput>,
	fnv::FnvBuildHasher,
>;

struct Http {
	url: Uri,
}

type ProcessPermits =
	DashMap<tg::process::Id, Arc<tokio::sync::Mutex<Option<ProcessPermit>>>, tg::id::BuildHasher>;

struct ProcessPermit(
	#[expect(dead_code)]
	tg::Either<tokio::sync::OwnedSemaphorePermit, tokio::sync::OwnedMutexGuard<Option<Self>>>,
);

type ProcessTasks = tangram_futures::task::Map<tg::process::Id, (), (), tg::id::BuildHasher>;

impl Owned {
	pub fn stop(&self) {
		self.task.stop();
	}

	pub async fn wait(&self) -> tg::Result<()> {
		self.task
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "the server task panicked"))
	}
}

impl Server {
	pub async fn start(config: Config) -> tg::Result<Owned> {
		// Get or create the directory.
		let directory = config.directory.clone().unwrap_or_else(|| {
			let id = uuid::Uuid::now_v7();
			std::env::temp_dir().join(format!("tangram-{id}"))
		});

		// Ensure the directory exists.
		tokio::fs::create_dir_all(&directory)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
		let path = tokio::fs::canonicalize(&directory).await.map_err(
			|source| tg::error!(!source, path = %directory.display(), "failed to canonicalize directory path"),
		)?;

		// Lock.
		let lock_path = path.join("lock");
		let mut lock = tokio::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(false)
			.open(lock_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the lock file"))?;
		let ret = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
		if ret != 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to lock the lock file"
			));
		}
		let pid = std::process::id();
		lock.set_len(0)
			.await
			.map_err(|source| tg::error!(!source, "failed to truncate the lock file"))?;
		lock.write_all(pid.to_string().as_bytes())
			.await
			.map_err(|source| tg::error!(!source, "failed to write the pid to the lock file"))?;
		let lock = Mutex::new(Some(lock));

		// Verify the version file.
		let version_path = path.join("version");
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
		let logs_path = path.join("logs");
		tokio::fs::create_dir_all(&logs_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the logs directory"))?;

		// Ensure the temp directory exists.
		let temp_path = path.join("tmp");
		tokio::fs::create_dir_all(&temp_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the temp directory"))?;

		// Ensure the tags directory exists.
		let tags_path = path.join("tags");
		tokio::fs::create_dir_all(&tags_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the tags directory"))?;

		// Get the available parallelism.
		let parallelism = std::thread::available_parallelism()
			.map(std::num::NonZeroUsize::get)
			.unwrap_or(1);

		// Remove an existing socket file.
		let socket_path = path.join("socket");
		tokio::fs::remove_file(&socket_path).await.ok();

		// Create the cache graph tasks.
		let cache_graph_tasks = tangram_futures::task::Map::default();

		// Create the cache tasks.
		let cache_tasks = tangram_futures::task::Map::default();

		// Create the checkin tasks.
		let checkin_tasks = tangram_futures::task::Map::default();

		// Create the HTTP configuration.
		let http = config.http.as_ref().map(|config| {
			let url = config.url.clone().unwrap_or_else(|| {
				let path = path.join("socket");
				let path = path.to_str().unwrap();
				tangram_uri::Uri::builder()
					.scheme("http+unix")
					.authority(path)
					.path("")
					.build()
					.unwrap()
			});
			Http { url }
		});

		// Create the process permits.
		let process_permits = DashMap::default();

		// Create the process semaphore.
		let permits = config
			.runner
			.as_ref()
			.map_or(0, |runner| runner.concurrency.unwrap_or(parallelism));
		let process_semaphore = Arc::new(tokio::sync::Semaphore::new(permits));

		// Create the process tasks.
		let process_tasks = tangram_futures::task::Map::default();

		// Create the database.
		let database = match &config.database {
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
						connections: options.connections.unwrap_or(parallelism),
						url: options.url.clone(),
					};
					let database = db::postgres::Database::new(options)
						.await
						.map_err(|source| tg::error!(!source, "failed to create the database"))?;
					Database::Postgres(database)
				}
			},
			self::config::Database::Sqlite(config) => {
				#[cfg(not(feature = "sqlite"))]
				{
					let _ = config;
					return Err(tg::error!(
						"this version of tangram was not compiled with sqlite support"
					));
				}
				#[cfg(feature = "sqlite")]
				{
					let initialize = Arc::new(self::database::sqlite::initialize);
					let options = db::sqlite::DatabaseOptions {
						connections: config.connections.unwrap_or(parallelism),
						initialize,
						path: path.join(&config.path),
					};
					let database = db::sqlite::Database::new(options)
						.await
						.map_err(|source| tg::error!(!source, "failed to create the database"))?;
					Database::Sqlite(database)
				}
			},
		};

		// Create the diagnostics.
		let diagnostics = Mutex::new(Vec::new());

		// Create the index.
		let index = match &config.index {
			self::config::Index::Fdb(options) => {
				#[cfg(not(feature = "foundationdb"))]
				{
					let _ = options;
					return Err(tg::error!(
						"this version of tangram was not compiled with foundationdb support"
					));
				}
				#[cfg(feature = "foundationdb")]
				{
					Index::new_fdb(&options.cluster)
						.map_err(|source| tg::error!(!source, "failed to create the index"))?
				}
			},
			self::config::Index::Lmdb(options) => {
				#[cfg(not(feature = "lmdb"))]
				{
					let _ = options;
					return Err(tg::error!(
						"this version of tangram was not compiled with lmdb support"
					));
				}
				#[cfg(feature = "lmdb")]
				{
					let path = directory.join(&options.path);
					let config = tangram_index::lmdb::Config {
						map_size: options.map_size,
						path,
					};
					Index::new_lmdb(&config)
						.map_err(|source| tg::error!(!source, "failed to create the index"))?
				}
			},
		};

		// Create the index tasks.
		let index_tasks = tangram_futures::task::Set::default();

		// Create the library.
		let library = Mutex::new(None);

		// Create the local pool handle lazily.
		let local_pool_handle = OnceLock::new();

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
					let mut options = async_nats::ConnectOptions::new();
					if let Some(ref credentials) = nats.credentials {
						options =
							options
								.credentials_file(credentials)
								.await
								.map_err(|source| {
									tg::error!(!source, "failed to load the NATS credentials")
								})?;
					}
					let client = options
						.connect(nats.url.to_string())
						.await
						.map_err(|source| {
							tg::error!(!source, "failed to create the NATS client")
						})?;
					Messenger::Nats(tangram_messenger::nats::Messenger::new(
						client,
						nats.id.clone(),
					))
				}
			},
		};

		// Create the finish stream and consumer if the messenger is memory.
		if messenger.is_memory() {
			let stream_config = tangram_messenger::StreamConfig::default();
			let stream = messenger
				.create_stream("finish".to_owned(), stream_config)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the finish stream"))?;
			let consumer_config = tangram_messenger::ConsumerConfig {
				deliver: tangram_messenger::DeliverPolicy::All,
			};
			stream
				.create_consumer("finish".to_owned(), consumer_config)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the finish consumer"))?;
		}

		// Create the process stream if the messenger is memory.
		if messenger.is_memory() {
			let stream_config = tangram_messenger::StreamConfig {
				discard: tangram_messenger::DiscardPolicy::New,
				max_bytes: None,
				max_messages: None,
				retention: tangram_messenger::RetentionPolicy::WorkQueue,
			};
			let stream = messenger
				.create_stream("queue".to_owned(), stream_config)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the queue stream"))?;
			let consumer_config = tangram_messenger::ConsumerConfig {
				deliver: tangram_messenger::DeliverPolicy::All,
			};
			stream
				.create_consumer("queue".to_owned(), consumer_config)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the queue consumer"))?;
		}

		// Create the pipes and ptys.
		let pipes = DashMap::default();
		let ptys = DashMap::default();

		// Create the remotes.
		let remotes = DashMap::default();

		// Create the store.
		let store = match &config.store {
			config::Store::Lmdb(lmdb) => {
				#[cfg(not(feature = "lmdb"))]
				{
					let _ = lmdb;
					return Err(tg::error!(
						"this version of tangram was not compiled with lmdb support"
					));
				}
				#[cfg(feature = "lmdb")]
				{
					Store::new_lmdb(&path, lmdb)
						.map_err(|error| tg::error!(!error, "failed to create the store"))?
				}
			},

			config::Store::Memory => Store::new_memory(),

			config::Store::Scylla(scylla) => {
				#[cfg(not(feature = "scylla"))]
				{
					let _ = scylla;
					return Err(tg::error!(
						"this version of tangram was not compiled with scylla support"
					));
				}
				#[cfg(feature = "scylla")]
				{
					Store::new_scylla(scylla)
						.await
						.map_err(|error| tg::error!(!error, "failed to create the store"))?
				}
			},
		};

		// Create the temp paths.
		let temps = DashSet::default();

		// Get the version.
		let version = config
			.version
			.clone()
			.unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());

		// Create the vfs.
		let vfs = Mutex::new(None);

		// Create the watches.
		let watches = DashMap::default();

		// Create the server.
		let server = Self(Arc::new(State {
			cache_graph_tasks,
			cache_tasks,
			checkin_tasks,
			config,
			database,
			diagnostics,
			http,
			index,
			index_tasks,
			library,
			local_pool_handle,
			lock,
			messenger,
			path,
			pipes,
			process_permits,
			process_semaphore,
			process_tasks,
			ptys,
			remotes,
			store,
			temps,
			version,
			vfs,
			watches,
		}));

		// Migrate the database if necessary.
		#[cfg(feature = "sqlite")]
		if let Ok(database) = server.database.try_unwrap_sqlite_ref() {
			self::database::sqlite::migrate(database)
				.await
				.map_err(|source| tg::error!(!source, "failed to migrate the database"))?;
		}

		// Finish unfinished processes if single process mode is enabled.
		if server.config().advanced.single_process {
			let result = server.finish_unfinished_processes().await;
			if let Err(error) = result {
				tracing::error!(?error, "failed to finish unfinished processes");
			}
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
				let params = db::params![&remote.name, &remote.url.to_string()];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to insert the remote"))?;
			}
		}

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

		// Spawn the HTTP task.
		let http_task = if let Some(http) = &server.http {
			let listener = Self::listen(&http.url)
				.await
				.map_err(|source| tg::error!(!source, "failed to listen on the http url"))?;
			tracing::info!("listening on {}", http.url);
			Some(Task::spawn(|stop| {
				let server = server.clone();
				let context = Context::default();
				async move {
					server.serve(listener, context, stop).await;
				}
			}))
		} else {
			None
		};

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

		// Spawn the finisher task.
		let finisher_task = server.config.finisher.clone().map(|config| {
			tokio::spawn({
				let server = server.clone();
				async move {
					server
						.finisher_task(&config)
						.await
						.inspect_err(|error| {
							tracing::error!(?error, "the finisher task failed");
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

		let shutdown = {
			let server = server.clone();
			async move {
				tracing::trace!("started");

				// Abort the runner task.
				if let Some(task) = runner_task {
					task.abort();
					let result = task.await;
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "the runner task panicked");
					}
					tracing::trace!("runner task");
				}

				// Abort the process tasks.
				server.process_tasks.abort_all();
				let results = server.process_tasks.wait().await;
				for result in results {
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "a process task panicked");
					}
				}
				tracing::trace!("process tasks");

				// Close all PTYs.
				let pty_ids = server
					.ptys
					.iter()
					.map(|r| r.key().clone())
					.collect::<Vec<_>>();
				for pty in pty_ids {
					server
						.close_pty(&pty, tg::pty::close::Arg::default())
						.await
						.ok();
				}
				tracing::trace!("close ptys");

				// Stop the HTTP task.
				if let Some(task) = http_task {
					task.stop();
					let result = task.wait().await;
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "the http task panicked");
					}
					tracing::trace!("http task");
				}

				// Stop the VFS.
				let vfs = server.vfs.lock().unwrap().take();
				if let Some(vfs) = vfs {
					vfs.stop();
					vfs.wait().await;
					tracing::trace!("vfs task");
				}

				// Abort the diagnostics task.
				if let Some(task) = diagnostics_task {
					task.abort();
				}

				// Abort the finish task.
				if let Some(task) = finisher_task {
					task.abort();
					let result = task.await;
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "the finsher task panicked");
					}
					tracing::trace!("finsher task");
				}

				// Abort the watchdog task.
				if let Some(task) = watchdog_task {
					task.abort();
					let result = task.await;
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "the watchdog task panicked");
					}
					tracing::trace!("watchdog task");
				}

				// Remove the watches.
				server.watches.clear();

				// Abort the checkin tasks.
				server.checkin_tasks.abort_all();
				let results = server.checkin_tasks.wait().await;
				for result in results {
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "a checkin task panicked");
					}
				}
				tracing::trace!("checkin tasks");

				// Abort the cache graph tasks.
				server.cache_graph_tasks.abort_all();
				let results = server.cache_graph_tasks.wait().await;
				for result in results {
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "a cache graph task failed");
					}
				}
				tracing::trace!("cache graph tasks");

				// Abort the cache tasks.
				server.cache_tasks.abort_all();
				let results = server.cache_tasks.wait().await;
				for result in results {
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "a cache task panicked");
					}
				}
				tracing::trace!("cache tasks");

				// Abort the index tasks.
				server.index_tasks.abort_all();
				server.index_tasks.wait().await;

				// Abort the cleaner task.
				if let Some(task) = cleaner_task {
					task.abort();
					let result = task.await;
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "the clean task panicked");
					}
					tracing::trace!("cleaner task");
				}

				// Abort the indexer task.
				if let Some(task) = indexer_task {
					task.abort();
					let result = task.await;
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "the index task panicked");
					}
					tracing::trace!("indexer task");
				}

				// Remove the temp paths.
				server
					.temps
					.iter()
					.map(|entry| remove(entry.key().clone()).map(|_| ()))
					.collect::<FuturesUnordered<_>>()
					.collect::<()>()
					.await;
				tracing::trace!("temps");

				// Unlock.
				let lock = server.lock.lock().unwrap().take();
				if let Some(lock) = lock {
					lock.set_len(0).await.ok();
					tracing::trace!("released lock file");
				}

				tracing::trace!("finished");
			}
			.instrument(tracing::debug_span!("shutdown"))
		};

		// Spawn the task.
		let task = tangram_futures::task::Shared::spawn(|stop| async move {
			stop.wait().await;
			shutdown.await;
		});

		let handle = Owned(Arc::new(Inner { server, task }));

		Ok(handle)
	}

	async fn finish_unfinished_processes(&self) -> tg::Result<()> {
		let outputs = self
			.list_processes_local()
			.await
			.map_err(|source| tg::error!(!source, "failed to list processes"))?;
		outputs
			.into_iter()
			.filter(|output| !matches!(output.data.status, tg::process::Status::Finished))
			.map(|output| {
				let server = self.clone();
				async move {
					let error = tg::error::Data {
						code: Some(tg::error::Code::HeartbeatExpiration),
						message: Some("heartbeat expired".into()),
						..Default::default()
					};
					let error = Some(tg::Either::Left(error));
					let arg = tg::process::finish::Arg {
						checksum: None,
						error,
						exit: 1,
						local: None,
						output: None,
						remotes: None,
					};
					if let Err(error) = server.finish_process(&output.id, arg).await {
						tracing::error!(process = %output.id, ?error, "failed to finish the process");
					}
				}
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<()>()
			.await;
		Ok(())
	}

	fn create_compiler(&self) -> tangram_compiler::Handle {
		tangram_compiler::Compiler::start(
			tg::handle::dynamic::Handle::new(self.clone()),
			self.cache_path(),
			self.tags_path(),
			self.library_path(),
			tokio::runtime::Handle::current(),
			self.version.clone(),
		)
	}

	#[must_use]
	pub fn config(&self) -> &Config {
		&self.config
	}

	#[must_use]
	pub fn url(&self) -> Option<&Uri> {
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
	pub fn library_path(&self) -> PathBuf {
		let library = self
			.library
			.lock()
			.unwrap()
			.get_or_insert_with(|| Arc::new(Temp::new(self)))
			.clone();
		library.path().to_owned()
	}

	#[must_use]
	pub fn logs_path(&self) -> PathBuf {
		self.path.join("logs")
	}

	#[must_use]
	pub fn tags_path(&self) -> PathBuf {
		self.path.join("tags")
	}

	#[must_use]
	pub fn temp_path(&self) -> PathBuf {
		self.path.join("tmp")
	}

	#[must_use]
	pub fn local(local: Option<bool>, remotes: Option<&Vec<String>>) -> bool {
		match (local, &remotes) {
			(None, None) => true,
			(Some(local), _) => local,
			(None, Some(_)) => false,
		}
	}

	pub fn remote(
		local: Option<bool>,
		remotes: Option<&Vec<String>>,
	) -> tg::Result<Option<String>> {
		let remotes = remotes.map_or([].as_slice(), Vec::as_slice);
		let local = local.unwrap_or(remotes.is_empty());
		match (local, remotes) {
			(true, []) => Ok(None),
			(true, _) => Err(tg::error!("cannot specify both local and a remote")),
			(false, []) => Err(tg::error!("a remote is required when local is false")),
			(false, [remote]) => Ok(Some(remote.clone())),
			(false, _) => Err(tg::error!("only one remote is allowed")),
		}
	}

	pub async fn remotes(
		&self,
		local: Option<bool>,
		remotes: Option<Vec<String>>,
	) -> tg::Result<Vec<String>> {
		if local == Some(true) {
			return Ok(Vec::new());
		}
		if let Some(remotes) = remotes {
			return Ok(remotes);
		}
		let output = self.list_remotes(tg::remote::list::Arg::default()).await?;
		Ok(output.data.into_iter().map(|r| r.name).collect())
	}
}

impl Deref for Owned {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Deref for Inner {
	type Target = Server;

	fn deref(&self) -> &Self::Target {
		&self.server
	}
}

impl Deref for Server {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Drop for Owned {
	fn drop(&mut self) {
		self.cache_graph_tasks.abort_all();
		self.cache_tasks.abort_all();
		self.library.lock().unwrap().take();
		self.process_tasks.abort_all();
		self.index_tasks.abort_all();
		self.vfs.lock().unwrap().take();
		self.watches.clear();
	}
}
