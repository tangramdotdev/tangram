use {
	self::{context::Context, database::Database, index::Index, messenger::Messenger},
	crate::{temp::Temp, watch::Watch},
	dashmap::{DashMap, DashSet},
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	indoc::{formatdoc, indoc},
	std::{
		ops::Deref,
		os::fd::AsRawFd as _,
		path::{Path, PathBuf},
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::task::Task,
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
mod location;
mod log;
mod lsp;
mod messenger;
mod module;
mod object;
mod process;
mod pull;
mod push;
mod read;
mod region;
mod remote;
mod run;
mod sandbox;
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
pub struct Shared(Arc<Owned>);

pub struct Owned {
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
	index: Index,
	index_tasks: tangram_futures::task::Set<()>,
	library: Mutex<Option<Arc<Temp>>>,
	lock: Mutex<Option<tokio::fs::File>>,
	log_store: self::log::Store,
	messenger: Messenger,
	object_get_tasks: ObjectGetTasks,
	object_store: self::object::Store,
	path: PathBuf,
	regions: DashMap<String, tg::Client, fnv::FnvBuildHasher>,
	remote_list_tags_tasks: RemoteListTagsTasks,
	remotes: DashMap<String, tg::Client, fnv::FnvBuildHasher>,
	sandbox_permits: SandboxPermits,
	sandbox_rootfs: PathBuf,
	sandbox_semaphore: Arc<tokio::sync::Semaphore>,
	process_store: Database,
	sandboxes: Sandboxes,
	sandbox_tasks: SandboxTasks,
	tangram_path: PathBuf,
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

type SandboxPermits =
	DashMap<tg::sandbox::Id, Arc<tokio::sync::Mutex<Option<SandboxPermit>>>, tg::id::BuildHasher>;

type Sandboxes = DashMap<tg::sandbox::Id, tangram_sandbox::Sandbox, tg::id::BuildHasher>;

struct SandboxPermit(
	#[expect(dead_code)]
	tg::Either<tokio::sync::OwnedSemaphorePermit, tokio::sync::OwnedMutexGuard<Option<Self>>>,
);

type SandboxTasks = tangram_futures::task::Map<tg::sandbox::Id, (), (), tg::id::BuildHasher>;

type ObjectGetTasks = tangram_futures::task::Map<
	crate::object::get::ObjectGetTaskKey,
	tg::Result<Option<tg::object::get::Output>>,
	(),
	fnv::FnvBuildHasher,
>;

type RemoteListTagsTasks = tangram_futures::task::Map<
	crate::tag::list::RemoteTagListTaskKey,
	tg::Result<Vec<tg::tag::list::Entry>>,
	(),
	fnv::FnvBuildHasher,
>;

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
		let parallelism =
			std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);

		// Remove an existing socket file.
		let socket_path = path.join("socket");
		tokio::fs::remove_file(&socket_path).await.ok();

		// Create the cache graph tasks.
		let cache_graph_tasks = tangram_futures::task::Map::default();

		// Create the cache tasks.
		let cache_tasks = tangram_futures::task::Map::default();

		// Create the checkin tasks.
		let checkin_tasks = tangram_futures::task::Map::default();

		// Create the sandbox permits and semaphore.
		let sandboxes = DashMap::default();
		let sandbox_permits = DashMap::default();
		let permits = config
			.runner
			.as_ref()
			.map_or(0, |runner| runner.concurrency.unwrap_or(parallelism));
		let sandbox_semaphore = Arc::new(tokio::sync::Semaphore::new(permits));

		// Create the sandbox tasks.
		let sandbox_tasks = tangram_futures::task::Map::default();

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

		// Create the process store.
		let process_store = match &config.process.store {
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
					let process_store =
						db::postgres::Database::new(options)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to create the process store")
							})?;
					Database::Postgres(process_store)
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
					let process_store =
						db::sqlite::Database::new(options).await.map_err(|source| {
							tg::error!(!source, "failed to create the process store")
						})?;
					Database::Sqlite(process_store)
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
					let options = tangram_index::fdb::Options {
						cluster: options.cluster.clone(),
						concurrency: options.concurrency,
						max_items_per_transaction: options.max_items_per_transaction,
						partition_total: options.partition_total,
						prefix: options.prefix.clone(),
					};
					Index::new_fdb(&options)
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
						max_items_per_transaction: options.max_items_per_transaction,
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

		// Create the regions.
		let regions = DashMap::default();

		// Create the object get tasks.
		let object_get_tasks = tangram_futures::task::Map::default();

		// Create the remotes.
		let remotes = DashMap::default();

		// Create the remote list tags tasks.
		let remote_list_tags_tasks = tangram_futures::task::Map::default();

		// Create the sandbox rootfs.
		let sandbox_rootfs_path = path.join("rootfs");
		let tangram_path = tangram_util::env::current_exe()
			.map_err(|source| tg::error!(!source, "failed to get the tangram executable path"))?;
		let sandbox_rootfs = sandbox_rootfs_path.clone();
		tangram_sandbox::root::prepare(&tangram_sandbox::root::Arg {
			path: sandbox_rootfs.clone(),
			tangram_path: tangram_path.clone(),
		})?;

		// Create the log store.
		let log_store = match &config.log_store {
			config::LogStore::Fdb(options) => {
				#[cfg(not(feature = "foundationdb"))]
				{
					let _ = options;
					return Err(tg::error!(
						"this version of tangram was not compiled with foundationdb support"
					));
				}
				#[cfg(feature = "foundationdb")]
				{
					self::log::Store::new_fdb(options)
						.map_err(|error| tg::error!(!error, "failed to create the log store"))?
				}
			},
			config::LogStore::Lmdb(lmdb) => {
				#[cfg(not(feature = "lmdb"))]
				{
					let _ = lmdb;
					return Err(tg::error!(
						"this version of tangram was not compiled with lmdb support"
					));
				}
				#[cfg(feature = "lmdb")]
				{
					self::log::Store::new_lmdb(&path, lmdb)
						.map_err(|error| tg::error!(!error, "failed to create the log store"))?
				}
			},
			config::LogStore::Memory => self::log::Store::new_memory(),
		};

		// Create the object store.
		let object_store = match &config.object.store {
			config::ObjectStore::Lmdb(lmdb) => {
				#[cfg(not(feature = "lmdb"))]
				{
					let _ = lmdb;
					return Err(tg::error!(
						"this version of tangram was not compiled with lmdb support"
					));
				}
				#[cfg(feature = "lmdb")]
				{
					self::object::Store::new_lmdb(&path, lmdb)
						.map_err(|error| tg::error!(!error, "failed to create the object store"))?
				}
			},

			config::ObjectStore::Memory => self::object::Store::new_memory(),

			config::ObjectStore::Scylla(scylla) => {
				#[cfg(not(feature = "scylla"))]
				{
					let _ = scylla;
					return Err(tg::error!(
						"this version of tangram was not compiled with scylla support"
					));
				}
				#[cfg(feature = "scylla")]
				{
					self::object::Store::new_scylla(scylla)
						.await
						.map_err(|error| tg::error!(!error, "failed to create the object store"))?
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
			index,
			index_tasks,
			library,
			lock,
			log_store,
			messenger,
			object_get_tasks,
			object_store,
			path,
			regions,
			remote_list_tags_tasks,
			remotes,
			sandbox_permits,
			sandbox_rootfs,
			sandbox_semaphore,
			process_store,
			sandboxes,
			sandbox_tasks,
			tangram_path,
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

		// Migrate the process store if necessary.
		#[cfg(feature = "sqlite")]
		if let Ok(process_store) = server.process_store.try_unwrap_sqlite_ref() {
			self::process::store::sqlite::migrate(process_store)
				.await
				.map_err(|source| tg::error!(!source, "failed to migrate the process store"))?;
		}

		// Finish unfinished sandboxes if single process mode is enabled.
		if server.config().advanced.single_process {
			let result = server.finish_unfinished_sandboxes().await;
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), "failed to finish unfinished sandboxes");
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
		server.remotes.clear();
		let output = server
			.list_remotes(tg::remote::list::Arg::default())
			.await
			.map_err(|source| tg::error!(!source, "failed to list the remotes"))?;
		for remote in output.data {
			let client = server.create_remote_client(&remote.name, remote.url)?;
			server.remotes.insert(remote.name, client);
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
							tracing::error!(error = %error.trace());
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
							tracing::error!(error = %error.trace());
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
					self::vfs::Server::unmount(self::vfs::Kind::Nfs, &artifacts_path).await?;
				} else if cfg!(target_os = "linux") {
					self::vfs::Server::unmount(self::vfs::Kind::Fuse, &artifacts_path).await?;
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
		let http_listeners = server.config().http.as_ref().map(|config| {
			if config.listeners.is_empty() {
				let path = server.path.join("socket");
				let path = path.to_str().unwrap();
				let url = tangram_uri::Uri::builder()
					.scheme("http+unix")
					.authority(path)
					.path("")
					.build()
					.unwrap();
				vec![crate::config::HttpListener { url, tls: None }]
			} else {
				config.listeners.clone()
			}
		});
		let http_task = if let Some(http_listeners) = http_listeners {
			let http_server = server.clone();
			let mut listeners = Vec::with_capacity(http_listeners.len());
			for listener_config in &http_listeners {
				let listener = Self::listen(&listener_config.url).await.map_err(|source| {
					tg::error!(
						!source,
						url = %listener_config.url,
						"failed to listen on the http url"
					)
				})?;
				tracing::info!("listening on {}", listener_config.url);
				listeners.push((listener, listener_config.clone()));
			}
			Some(Task::spawn(move |stop| {
				let server = http_server.clone();
				async move {
					listeners
						.into_iter()
						.map(|(listener, listener_config)| {
							let server = server.clone();
							let stop = stop.clone();
							async move {
								server
									.serve(listener, listener_config, None, None, stop)
									.await;
							}
						})
						.collect::<FuturesUnordered<_>>()
						.collect::<Vec<_>>()
						.await;
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
					.inspect_err(|error| tracing::error!(error = %error.trace()))
					.ok();
			}
		}));

		// Spawn the process finalizer task.
		let process_finalizer_task = server.config.process.finalizer.clone().map(|config| {
			tokio::spawn({
				let server = server.clone();
				async move {
					server
						.finalizer_task(&config)
						.await
						.inspect_err(|error| {
							tracing::error!(error = %error.trace(), "the process finalizer task failed");
						})
						.ok();
				}
			})
		});

		// Spawn the sandbox finalizer task.
		let sandbox_finalizer_task = server.config.sandbox.finalizer.clone().map(|config| {
			tokio::spawn({
				let server = server.clone();
				async move {
					server
						.sandbox_finalizer_task(&config)
						.await
						.inspect_err(|error| {
							tracing::error!(error = %error.trace(), "the sandbox finalizer task failed");
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
						.inspect_err(
							|error| tracing::error!(error = %error.trace(), "the watchdog task failed"),
						)
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

				// Abort the sandbox tasks.
				server.sandbox_tasks.abort_all();
				let results = server.sandbox_tasks.wait().await;
				for result in results {
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "a sandbox task panicked");
					}
				}
				tracing::trace!("sandbox tasks");

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

				// Abort the process finalizer task.
				if let Some(task) = process_finalizer_task {
					task.abort();
					let result = task.await;
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "the process finalizer task panicked");
					}
					tracing::trace!("process finalizer task");
				}

				// Abort the sandbox finalizer task.
				if let Some(task) = sandbox_finalizer_task {
					task.abort();
					let result = task.await;
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "the sandbox finalizer task panicked");
					}
					tracing::trace!("sandbox finalizer task");
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

				// Abort the object get tasks.
				server.object_get_tasks.abort_all();
				let results = server.object_get_tasks.wait().await;
				for result in results {
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "an object get task panicked");
					}
				}
				tracing::trace!("object get tasks");

				// Abort the remote list tags tasks.
				server.remote_list_tags_tasks.abort_all();
				let results = server.remote_list_tags_tasks.wait().await;
				for result in results {
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "a remote list tags task panicked");
					}
				}
				tracing::trace!("remote list tags tasks");

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

		let handle = Owned { server, task };

		Ok(handle)
	}

	async fn finish_unfinished_sandboxes(&self) -> tg::Result<()> {
		let outputs = self
			.list_sandboxes_local()
			.await
			.map_err(|source| tg::error!(!source, "failed to list sandboxes"))?;
		outputs
			.into_iter()
			.map(|output| {
				let server = self.clone();
				async move {
					let error = tg::error::Data {
						code: Some(tg::error::Code::HeartbeatExpiration),
						message: Some("heartbeat expired".into()),
						..Default::default()
					};
					let error = Some(tg::Either::Left(error));
					if let Err(error) = server
						.try_finish_sandbox_local(&output.id, error, None)
						.await
					{
						tracing::error!(sandbox = %output.id, error = %error.trace(), "failed to finish the sandbox");
					}
				}
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<()>()
			.await;
		Ok(())
	}

	fn create_compiler(&self) -> tangram_compiler::Shared {
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
	pub fn arg(&self) -> tg::Arg {
		let url = self
			.config()
			.http
			.as_ref()
			.and_then(|http| http.listeners.first())
			.map_or_else(
				|| {
					let path = self.path.join("socket");
					let path = path.to_str().unwrap();
					Uri::builder()
						.scheme("http+unix")
						.authority(path)
						.path("")
						.build()
						.unwrap()
				},
				|listener| listener.url.clone(),
			);
		tg::Arg {
			url: Some(url),
			version: Some(self.version.clone()),
			..Default::default()
		}
	}

	#[must_use]
	pub fn config(&self) -> &Config {
		&self.config
	}

	fn host_path_for_guest_path(&self, context: &Context, path: &Path) -> tg::Result<PathBuf> {
		let Some(id) = &context.sandbox else {
			return Ok(path.to_owned());
		};
		let sandbox = self
			.sandboxes
			.get(id)
			.map(|sandbox| sandbox.value().clone())
			.ok_or_else(|| tg::error!(%id, "failed to get the sandbox"))?;
		sandbox
			.host_path_for_guest_path(path)
			.ok_or_else(|| tg::error!(path = %path.display(), "no host path for guest path"))
	}

	fn guest_path_for_host_path(&self, context: &Context, path: &Path) -> tg::Result<PathBuf> {
		let Some(id) = &context.sandbox else {
			return Ok(path.to_owned());
		};
		let sandbox = self
			.sandboxes
			.get(id)
			.map(|sandbox| sandbox.value().clone())
			.ok_or_else(|| tg::error!(%id, "failed to get the sandbox"))?;
		sandbox
			.guest_path_for_host_path(path)
			.ok_or_else(|| tg::error!(path = %path.display(), "no guest path for host path"))
	}

	#[must_use]
	fn artifacts_path(&self) -> PathBuf {
		self.path.join("artifacts")
	}

	#[must_use]
	fn cache_path(&self) -> PathBuf {
		if self.vfs.lock().unwrap().is_some() {
			self.path.join("cache")
		} else {
			self.artifacts_path()
		}
	}

	#[must_use]
	fn library_path(&self) -> PathBuf {
		let library = self
			.library
			.lock()
			.unwrap()
			.get_or_insert_with(|| Arc::new(Temp::new(self)))
			.clone();
		library.path().to_owned()
	}

	#[must_use]
	fn tags_path(&self) -> PathBuf {
		self.path.join("tags")
	}

	#[must_use]
	fn temp_path(&self) -> PathBuf {
		self.path.join("tmp")
	}
}

impl From<Owned> for Shared {
	fn from(value: Owned) -> Self {
		Self(Arc::new(value))
	}
}

impl Deref for Shared {
	type Target = Owned;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Deref for Owned {
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
		self.sandbox_tasks.abort_all();
		self.object_get_tasks.abort_all();
		self.remote_list_tags_tasks.abort_all();
		self.index_tasks.abort_all();
		self.vfs.lock().unwrap().take();
		self.watches.clear();
	}
}
