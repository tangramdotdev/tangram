use {
	self::{
		context::Context, database::Database, index::Index, messenger::Messenger, session::Session,
		temp::Temp, watch::Watch,
	},
	dashmap::{DashMap, DashSet},
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	indoc::{formatdoc, indoc},
	std::{
		collections::BTreeMap,
		ops::{ControlFlow, Deref},
		os::fd::AsRawFd as _,
		path::PathBuf,
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

mod authentication;
mod authorization;
mod cache;
mod check;
mod checkin;
mod checkout;
mod checksum;
mod clean;
mod compiler;
mod context;
mod database;
mod directory;
mod document;
mod format;
mod get;
mod grant;
mod group;
mod handle;
mod health;
mod http;
mod index;
mod list;
mod location;
mod log;
mod messenger;
mod module;
mod node;
mod object;
mod organization;
mod process;
mod pull;
mod push;
mod read;
mod region;
mod remote;
mod run;
mod sandbox;
mod session;
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
	cache_graph_tasks: self::cache::GraphTasks,
	cache_tasks: self::cache::Tasks,
	checkin_tasks: self::checkin::Tasks,
	config: Config,
	context: Context,
	database: Database,
	diagnostics: Mutex<Vec<tg::Diagnostic>>,
	index: Index,
	index_tasks: tangram_futures::task::Set<()>,
	#[cfg(target_os = "linux")]
	ip_pool: tangram_sandbox::network::ip::Pool,
	library: Mutex<Option<Arc<Temp>>>,
	lock: Mutex<Option<tokio::fs::File>>,
	log_store: self::log::Store,
	messenger: Messenger,
	object_get_tasks: self::object::get::Tasks,
	object_store: self::object::Store,
	path: PathBuf,
	process_store: Database,
	regions: DashMap<String, tg::Client, fnv::FnvBuildHasher>,
	remote_list_tasks: self::list::remote::Tasks,
	remote_clients: DashMap<Uri, tg::Client, fnv::FnvBuildHasher>,
	sandbox_container_root: PathBuf,
	sandbox_permits: self::sandbox::Permits,
	sandbox_seatbelt_root: PathBuf,
	sandbox_semaphore: Arc<tokio::sync::Semaphore>,
	sandbox_tasks: self::sandbox::Tasks,
	sandbox_vm_image: Option<PathBuf>,
	#[cfg(target_os = "linux")]
	sandbox_vm_image_lock: tokio::sync::Mutex<bool>,
	#[cfg(target_os = "linux")]
	sandbox_vm_snapshot_lock: tokio::sync::Mutex<()>,
	sandboxes: self::sandbox::Map,
	tangram_path: PathBuf,
	temps: DashSet<PathBuf, fnv::FnvBuildHasher>,
	pub tokens: Tokens,
	version: String,
	vfs: Mutex<Option<self::vfs::Server>>,
	watches: DashMap<PathBuf, Watch, fnv::FnvBuildHasher>,
}

pub struct Tokens {
	pub private_key: Option<tg::token::PrivateKey>,
	pub public_keys: BTreeMap<String, tg::token::PublicKey>,
}

impl Owned {
	pub fn stop(&self) {
		self.task.stop();
	}

	pub async fn wait(&self) -> tg::Result<()> {
		self.task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the server task panicked"))
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
			.map_err(|error| tg::error!(!error, "failed to create the directory"))?;
		let path = tokio::fs::canonicalize(&directory).await.map_err(
			|error| tg::error!(!error, path = %directory.display(), "failed to canonicalize directory path"),
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
			.map_err(|error| tg::error!(!error, "failed to open the lock file"))?;
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
			.map_err(|error| tg::error!(!error, "failed to truncate the lock file"))?;
		lock.write_all(pid.to_string().as_bytes())
			.await
			.map_err(|error| tg::error!(!error, "failed to write the pid to the lock file"))?;
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
					.map_err(|error| tg::error!(!error, "failed to write the version file"))?;
			},
		}

		// Ensure the temp directory exists.
		let temp_path = path.join("tmp");
		tokio::fs::create_dir_all(&temp_path)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the temp directory"))?;

		// Ensure the tags directory exists.
		let tags_path = path.join("tags");
		tokio::fs::create_dir_all(&tags_path)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the tags directory"))?;

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

		// Create the context.
		let context = Context::root();

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
						max: options.pool.max.unwrap_or(parallelism),
						min: options.pool.min.unwrap_or(0),
						retry: options.retry.clone().into(),
						ttl: options.pool.ttl,
						url: options.url.clone(),
					};
					let database = db::postgres::Database::new(options)
						.await
						.map_err(|error| tg::error!(!error, "failed to create the database"))?;
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
						initialize,
						max: config.pool.max.unwrap_or(parallelism),
						min: config.pool.min.unwrap_or(0),
						path: path.join(&config.path),
						retry: config.retry.clone().into(),
						ttl: config.pool.ttl,
					};
					let database = db::sqlite::Database::new(options)
						.await
						.map_err(|error| tg::error!(!error, "failed to create the database"))?;
					Database::Sqlite(database)
				}
			},
			self::config::Database::Turso(config) => {
				#[cfg(not(feature = "turso"))]
				{
					let _ = config;
					return Err(tg::error!(
						"this version of tangram was not compiled with turso support"
					));
				}
				#[cfg(feature = "turso")]
				{
					let initialize: db::turso::Initialize = Arc::new(|connection| {
						Box::pin(self::database::turso::initialize(connection))
					});
					let options = db::turso::DatabaseOptions {
						initialize,
						max: config.pool.max.unwrap_or(parallelism),
						min: config.pool.min.unwrap_or(0),
						path: path.join(&config.path),
						retry: config.retry.clone().into(),
						ttl: config.pool.ttl,
					};
					let database = db::turso::Database::new(options)
						.await
						.map_err(|error| tg::error!(!error, "failed to create the database"))?;
					Database::Turso(database)
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
						max: options.pool.max.unwrap_or(parallelism),
						min: options.pool.min.unwrap_or(0),
						retry: options.retry.clone().into(),
						ttl: options.pool.ttl,
						url: options.url.clone(),
					};
					let process_store =
						db::postgres::Database::new(options)
							.await
							.map_err(|error| {
								tg::error!(!error, "failed to create the process store")
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
						initialize,
						max: config.pool.max.unwrap_or(parallelism),
						min: config.pool.min.unwrap_or(0),
						path: path.join(&config.path),
						retry: config.retry.clone().into(),
						ttl: config.pool.ttl,
					};
					let process_store =
						db::sqlite::Database::new(options).await.map_err(|error| {
							tg::error!(!error, "failed to create the process store")
						})?;
					Database::Sqlite(process_store)
				}
			},
			self::config::Database::Turso(config) => {
				#[cfg(not(feature = "turso"))]
				{
					let _ = config;
					return Err(tg::error!(
						"this version of tangram was not compiled with turso support"
					));
				}
				#[cfg(feature = "turso")]
				{
					let initialize: db::turso::Initialize = Arc::new(|connection| {
						Box::pin(self::database::turso::initialize(connection))
					});
					let options = db::turso::DatabaseOptions {
						initialize,
						max: config.pool.max.unwrap_or(parallelism),
						min: config.pool.min.unwrap_or(0),
						path: path.join(&config.path),
						retry: config.retry.clone().into(),
						ttl: config.pool.ttl,
					};
					let process_store =
						db::turso::Database::new(options).await.map_err(|error| {
							tg::error!(!error, "failed to create the process store")
						})?;
					Database::Turso(process_store)
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
						authorization_concurrency: options.authorization_concurrency,
						cluster: options.cluster.clone(),
						concurrency: options.concurrency,
						max_items_per_transaction: options.max_items_per_transaction,
						partition_total: options.partition_total,
						prefix: options.prefix.clone(),
					};
					Index::new_fdb(&options)
						.map_err(|error| tg::error!(!error, "failed to create the index"))?
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
						.map_err(|error| tg::error!(!error, "failed to create the index"))?
				}
			},
		};

		// Create the index tasks.
		let index_tasks = tangram_futures::task::Set::default();

		// Create the library.
		let library = Mutex::new(None);

		// Create the messenger.
		let messenger = match &config.messenger {
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
						options = options
							.credentials_file(credentials)
							.await
							.map_err(|error| {
								tg::error!(!error, "failed to load the NATS credentials")
							})?;
					}
					let client = options
						.connect(nats.url.to_string())
						.await
						.map_err(|error| tg::error!(!error, "failed to create the NATS client"))?;
					Messenger::Nats(tangram_messenger::nats::Messenger::new(
						client,
						nats.id.clone(),
					))
				}
			},
			self::config::Messenger::Unix(unix) => {
				let messenger_path = unix.path.as_ref().map_or_else(
					|| path.join("messenger"),
					|messenger_path| {
						if messenger_path.is_absolute() {
							messenger_path.clone()
						} else {
							path.join(messenger_path)
						}
					},
				);
				let messenger = tangram_messenger::unix::Messenger::new(messenger_path)
					.await
					.map_err(|error| tg::error!(!error, "failed to create the messenger"))?;
				Messenger::Unix(messenger)
			},
		};

		// Create the IP pool.
		#[cfg(target_os = "linux")]
		let ip_pool = tangram_sandbox::network::ip::Pool::new(
			config
				.sandbox
				.network
				.ip_ranges
				.iter()
				.map(|range| (range.min.to_bits(), range.max.to_bits())),
		);

		// Create the regions.
		let regions = DashMap::default();

		// Create the object get tasks.
		let object_get_tasks = tangram_futures::task::Map::default();

		// Create the remote clients.
		let remote_clients = DashMap::default();

		// Create the remote list tasks.
		let remote_list_tasks = tangram_futures::task::Map::default();

		// Create the sandbox container root.
		let sandbox_container_root_path = path.join("container/root");
		let sandbox_seatbelt_root_path = path.join("seatbelt/root");
		let tangram_path = tangram_util::env::current_exe()
			.map_err(|error| tg::error!(!error, "failed to get the tangram executable path"))?;
		let sandbox_container_root = sandbox_container_root_path.clone();
		let sandbox_seatbelt_root = sandbox_seatbelt_root_path.clone();
		let sandbox_vm_image = config
			.sandbox
			.isolation
			.vm
			.as_ref()
			.map(|_| path.join("vm/image.squashfs"));
		#[cfg(target_os = "linux")]
		tangram_sandbox::container::root::create(&tangram_sandbox::container::root::Arg {
			path: sandbox_container_root.clone(),
			tangram_path: tangram_path.clone(),
		})?;
		#[cfg(target_os = "macos")]
		tangram_sandbox::seatbelt::root::create(&tangram_sandbox::seatbelt::root::Arg {
			path: sandbox_seatbelt_root.clone(),
			tangram_path: tangram_path.clone(),
		})?;

		// Create the log store.
		let log_store = match &config.logs.store {
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

			config::ObjectStore::Memory(_) => self::object::Store::new_memory(),

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

		// Create the token keys.
		let private_key = match &config.authorization.tokens.private_key {
			Some(config) => {
				let bytes = tokio::fs::read(&config.path).await.map_err(
					|error| tg::error!(!error, path = %config.path.display(), "failed to read the private key"),
				)?;
				Some(tg::token::PrivateKey::new(
					config.name.clone(),
					config.algorithm,
					bytes,
				))
			},
			None => None,
		};
		let mut public_keys = BTreeMap::new();
		for config in &config.authorization.tokens.public_keys {
			let bytes = tokio::fs::read(&config.path).await.map_err(
				|error| tg::error!(!error, path = %config.path.display(), "failed to read the public key"),
			)?;
			let key = tg::token::PublicKey::new(config.name.clone(), config.algorithm, bytes);
			if public_keys.insert(config.name.clone(), key).is_some() {
				return Err(tg::error!(name = %config.name, "duplicate public key"));
			}
		}
		let tokens = Tokens {
			private_key,
			public_keys,
		};

		// Create the server.
		let server = Self(Arc::new(State {
			cache_graph_tasks,
			cache_tasks,
			checkin_tasks,
			config,
			context,
			database,
			diagnostics,
			index,
			index_tasks,
			#[cfg(target_os = "linux")]
			ip_pool,
			library,
			lock,
			log_store,
			messenger,
			object_get_tasks,
			object_store,
			path,
			process_store,
			regions,
			remote_list_tasks,
			remote_clients,
			sandbox_container_root,
			sandbox_permits,
			sandbox_seatbelt_root,
			sandbox_semaphore,
			sandbox_tasks,
			sandbox_vm_image,
			#[cfg(target_os = "linux")]
			sandbox_vm_image_lock: tokio::sync::Mutex::new(false),
			#[cfg(target_os = "linux")]
			sandbox_vm_snapshot_lock: tokio::sync::Mutex::new(()),
			sandboxes,
			tangram_path,
			temps,
			tokens,
			version,
			vfs,
			watches,
		}));

		// Migrate the database if necessary.
		#[cfg(feature = "sqlite")]
		if let Ok(database) = server.database.try_unwrap_sqlite_ref() {
			self::database::sqlite::migrate(database)
				.await
				.map_err(|error| tg::error!(!error, "failed to migrate the database"))?;
		}

		#[cfg(feature = "turso")]
		if let Ok(database) = server.database.try_unwrap_turso_ref() {
			self::database::turso::migrate(database)
				.await
				.map_err(|error| tg::error!(!error, "failed to migrate the database"))?;
		}

		// Migrate the process store if necessary.
		#[cfg(feature = "sqlite")]
		if let Ok(process_store) = server.process_store.try_unwrap_sqlite_ref() {
			self::process::store::sqlite::migrate(process_store)
				.await
				.map_err(|error| tg::error!(!error, "failed to migrate the process store"))?;
		}

		#[cfg(feature = "turso")]
		if let Ok(process_store) = server.process_store.try_unwrap_turso_ref() {
			self::process::store::turso::migrate(process_store)
				.await
				.map_err(|error| tg::error!(!error, "failed to migrate the process store"))?;
		}

		// // Destroy unfinished sandboxes if single process mode is enabled.
		// if server.config().advanced.single_process {
		// 	let result = server.destroy_unfinished_sandboxes().await;
		// 	if let Err(error) = result {
		// 		tracing::error!(error = %error.trace(), "failed to destroy unfinished sandboxes");
		// 	}
		// }

		// Set the remotes if specified in the config.
		if let Some(remotes) = &server.config.remotes {
			let remotes = remotes.clone();
			server
				.database
				.run(|transaction| {
					let remotes = remotes.clone();
					async move { Self::set_config_remotes_with_transaction(transaction, &remotes).await }
						.boxed()
				})
				.await?;
		}

		// Spawn the indexer task.
		let indexer_task = server.config.indexer.clone().map(|config| {
			Task::spawn({
				let server = server.clone();
				|_| async move {
					let result = server.indexer_task(&config).await;
					if let Err(error) = result {
						tracing::error!(error = %error.trace());
					}
				}
			})
		});

		// Spawn the cleaner task.
		let cleaner_task = server.config.cleaner.clone().map(|config| {
			Task::spawn({
				let server = server.clone();
				|_| async move {
					let result = server.cleaner_task(&config).await;
					if let Err(error) = result {
						tracing::error!(error = %error.trace());
					}
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
			Err(error) => {
				return Err(tg::error!(!error, "failed to stat the path"));
			},
		};
		let cache_exists = tokio::fs::try_exists(&cache_path)
			.await
			.map_err(|error| tg::error!(!error, "failed to stat the path"))?;
		if let Some(options) = server.config.vfs {
			if artifacts_exists && !cache_exists {
				tokio::fs::rename(&artifacts_path, &cache_path)
					.await
					.map_err(|error| {
						tg::error!(
							!error,
							"failed to move the artifacts directory to the cache path"
						)
					})?;
			}
			tokio::fs::create_dir_all(&artifacts_path)
				.await
				.map_err(|error| tg::error!(!error, "failed to create the artifacts directory"))?;
			tokio::fs::create_dir_all(&cache_path)
				.await
				.map_err(|error| tg::error!(!error, "failed to create the cache directory"))?;
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
				.inspect_err(|error| {
					tracing::error!(?error, "failed to start the VFS");
				})
				.ok();
			if let Some(vfs) = vfs {
				server.vfs.lock().unwrap().replace(vfs);
			}
		} else {
			if cache_exists {
				tokio::fs::rename(&cache_path, &artifacts_path)
					.await
					.map_err(|error| {
						tg::error!(
							!error,
							"failed to move the artifacts directory to the cache directory"
						)
					})?;
			}
			tokio::fs::create_dir_all(&artifacts_path)
				.await
				.map_err(|error| tg::error!(!error, "failed to create the artifacts directory"))?;
		}

		// Spawn the HTTP task.
		let http_listeners = server
			.config()
			.http
			.as_ref()
			.map_or_else(Vec::new, |config| {
				if config.listeners.is_empty() {
					let path = server.path.join("socket");
					let path = path.to_str().unwrap();
					let url = Uri::builder()
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
		let http_task = if http_listeners.is_empty() {
			None
		} else {
			let http_server = server.clone();
			let mut listeners = Vec::with_capacity(http_listeners.len());
			let mut streams = Vec::new();
			for listener_config in &http_listeners {
				if matches!(listener_config.url.scheme(), Some("http+stdio")) {
					let stream = Self::connect(&listener_config.url).await.map_err(|error| {
						tg::error!(
							!error,
							url = %listener_config.url,
							"failed to connect to the http url"
						)
					})?;
					tracing::info!("serving on {}", listener_config.url);
					streams.push(stream);
				} else {
					let listener = Self::listen(&listener_config.url).await.map_err(|error| {
						tg::error!(
							!error,
							url = %listener_config.url,
							"failed to listen on the http url"
						)
					})?;
					tracing::info!("listening on {}", listener_config.url);
					listeners.push((listener, listener_config.clone(), false));
				}
			}
			Some(Task::spawn(move |stopper| {
				let server = http_server.clone();
				async move {
					let tasks = FuturesUnordered::new();
					for (listener, listener_config, sandbox) in listeners {
						let server = server.clone();
						let stopper = stopper.clone();
						tasks.push(
							async move {
								server
									.serve(listener, listener_config, sandbox, stopper)
									.await;
							}
							.boxed(),
						);
					}
					for stream in streams {
						let server = server.clone();
						let stopper = stopper.clone();
						tasks.push(
							async move {
								server.serve_stream(stream, false, stopper).await;
							}
							.boxed(),
						);
					}
					tasks.collect::<Vec<_>>().await;
				}
			}))
		};

		// Spawn the diagnostics task.
		let diagnostics_task = Some(Task::spawn({
			let server = server.clone();
			|_| async move {
				let result = server.diagnostics_task().await;
				if let Err(error) = result {
					tracing::error!(error = %error.trace());
				}
			}
		}));

		// Spawn the process finalizer task.
		let process_finalizer_task = server.config.process.finalizer.clone().map(|config| {
			Task::spawn({
				let server = server.clone();
				|stopper| async move {
					server
						.finalizer_task(&config, stopper)
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
			Task::spawn({
				let server = server.clone();
				|stopper| async move {
					server
						.sandbox_finalizer_task(&config, stopper)
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
			Task::spawn({
				let server = server.clone();
				let config = config.clone();
				|_| async move {
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
			Some(Task::spawn({
				let server = server.clone();
				|_| async move {
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

				// Stop and await the HTTP task.
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

				// Abort the runner task.
				if let Some(task) = runner_task {
					task.abort();
					let result = task.wait().await;
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
					let result = task.wait().await;
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
					let result = task.wait().await;
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
					let result = task.wait().await;
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "the watchdog task panicked");
					}
					tracing::trace!("watchdog task");
				}

				// // Remove the watches.
				// server.watches.clear();

				// // Abort the checkin tasks.
				// server.checkin_tasks.abort_all();
				// let results = server.checkin_tasks.wait().await;
				// for result in results {
				// 	if let Err(error) = result
				// 		&& !error.is_cancelled()
				// 	{
				// 		tracing::error!(?error, "a checkin task panicked");
				// 	}
				// }
				// tracing::trace!("checkin tasks");

				// // Abort the cache graph tasks.
				// server.cache_graph_tasks.abort_all();
				// let results = server.cache_graph_tasks.wait().await;
				// for result in results {
				// 	if let Err(error) = result
				// 		&& !error.is_cancelled()
				// 	{
				// 		tracing::error!(?error, "a cache graph task failed");
				// 	}
				// }
				// tracing::trace!("cache graph tasks");

				// // Abort the cache tasks.
				// server.cache_tasks.abort_all();
				// let results = server.cache_tasks.wait().await;
				// for result in results {
				// 	if let Err(error) = result
				// 		&& !error.is_cancelled()
				// 	{
				// 		tracing::error!(?error, "a cache task panicked");
				// 	}
				// }
				// tracing::trace!("cache tasks");

				// // Abort the object get tasks.
				// server.object_get_tasks.abort_all();
				// let results = server.object_get_tasks.wait().await;
				// for result in results {
				// 	if let Err(error) = result
				// 		&& !error.is_cancelled()
				// 	{
				// 		tracing::error!(?error, "an object get task panicked");
				// 	}
				// }
				// tracing::trace!("object get tasks");

				// Abort the remote list tasks.
				server.remote_list_tasks.abort_all();
				let results = server.remote_list_tasks.wait().await;
				for result in results {
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "a remote list task panicked");
					}
				}
				tracing::trace!("remote list tasks");

				// Abort the index tasks.
				server.index_tasks.abort_all();
				server.index_tasks.wait().await;

				// Abort the cleaner task.
				if let Some(task) = cleaner_task {
					task.abort();
					let result = task.wait().await;
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
					let result = task.wait().await;
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

		let owned = Owned { server, task };

		Ok(owned)
	}

	// async fn destroy_unfinished_sandboxes(&self) -> tg::Result<()> {
	// 	let session = self.session(&self.context);
	// 	let outputs = session
	// 		.list_sandboxes_local()
	// 		.await
	// 		.map_err(|error| tg::error!(!error, "failed to list sandboxes"))?;
	// 	outputs
	// 		.into_iter()
	// 		.map(|output| {
	// 			let session = session.clone();
	// 			async move {
	// 				let error = tg::error::Data {
	// 					code: Some(tg::error::Code::HeartbeatExpiration),
	// 					message: Some("heartbeat expired".into()),
	// 					..Default::default()
	// 				};
	// 				let error = Some(tg::Either::Left(error));
	// 				if let Err(error) = session
	// 					.try_destroy_sandbox_local(&output.id, error, None)
	// 					.await
	// 				{
	// 					tracing::error!(sandbox = %output.id, error = %error.trace(), "failed to destroy the sandbox");
	// 				}
	// 			}
	// 		})
	// 		.collect::<FuturesUnordered<_>>()
	// 		.collect::<()>()
	// 		.await;
	// 	Ok(())
	// }

	#[must_use]
	pub fn arg(&self) -> tg::Arg {
		let default_url = || {
			let path = self.path.join("socket");
			let path = path.to_str().unwrap();
			Uri::builder()
				.scheme("http+unix")
				.authority(path)
				.path("")
				.build()
				.unwrap()
		};
		let url = match self.config().http.as_ref() {
			Some(http) if http.listeners.is_empty() => Some(default_url()),
			Some(http) => http
				.listeners
				.iter()
				.find(|listener| !matches!(listener.url.scheme(), Some("http+stdio")))
				.map(|listener| listener.url.clone()),
			None => Some(default_url()),
		};
		tg::Arg {
			url,
			version: Some(self.version.clone()),
			..Default::default()
		}
	}

	#[must_use]
	pub(crate) fn session(&self, context: &Context) -> Session {
		Session::new(self.clone(), context.clone())
	}

	async fn set_config_remotes_with_transaction(
		transaction: &database::Transaction<'_>,
		remotes: &std::collections::BTreeMap<String, crate::config::Remote>,
	) -> tg::Result<ControlFlow<(), database::Error>> {
		#[derive(db::row::Deserialize)]
		struct RemoteTokenRow {
			name: String,
			token: Option<String>,
		}
		let statement = indoc!(
			r#"
				select name, token
				from remotes
				where "user" is null;
			"#,
		);
		let result = transaction
			.query_all_into::<RemoteTokenRow>(statement.into(), db::params![])
			.await;
		let tokens = crate::database::retry!(result, "failed to execute the statement")
			.into_iter()
			.map(|row| (row.name, row.token))
			.collect::<std::collections::BTreeMap<_, _>>();
		let statement = indoc!(
			r#"
				delete from remotes
				where "user" is null;
			"#,
		);
		let result = transaction.execute(statement.into(), db::params![]).await;
		crate::database::retry!(result, "failed to delete the remotes");
		for (name, remote) in remotes {
			let p = transaction.p();
			let statement = formatdoc!(
				r#"
					insert into remotes (name, "user", url, token)
					values ({p}1, null, {p}2, {p}3);
				"#,
			);
			let token = remote
				.token
				.clone()
				.or_else(|| tokens.get(name).cloned().flatten());
			let params = db::params![name.clone(), remote.url.to_string(), token];
			let result = transaction.execute(statement.into(), params).await;
			crate::database::retry!(result, "failed to insert the remote");
		}
		Ok(ControlFlow::Break(()))
	}

	#[must_use]
	pub fn config(&self) -> &Config {
		&self.config
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

	#[must_use]
	pub fn vm_snapshot_path(&self) -> PathBuf {
		self.path.join("vm/snapshot")
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
		self.remote_list_tasks.abort_all();
		self.index_tasks.abort_all();
		self.vfs.lock().unwrap().take();
		self.watches.clear();
	}
}
