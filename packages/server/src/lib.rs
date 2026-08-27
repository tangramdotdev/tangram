use {
	self::{
		context::{Context, Origin},
		database::Database,
		index::Index,
		messenger::Messenger,
		session::Session,
		temp::Temp,
		watch::Watch,
	},
	dashmap::{DashMap, DashSet},
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	indoc::{formatdoc, indoc},
	std::{
		collections::BTreeMap,
		ops::{ControlFlow, Deref},
		os::fd::AsRawFd as _,
		path::PathBuf,
		sync::{Arc, Mutex, atomic::AtomicU64},
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::task::Task,
	tangram_index::Index as _,
	tangram_uri::Uri,
	tangram_util::fs::remove,
	tokio::io::AsyncWriteExt as _,
	tracing::Instrument as _,
};

mod authentication;
mod authorization;
mod billing;
mod check;
mod checkin;
mod checkout;
mod checkpoint;
mod checksum;
mod children;
mod clean;
mod cleaner;
mod clock;
mod compiler;
mod context;
mod control;
mod database;
mod diagnostics;
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
mod indexer;
mod list;
mod location;
mod log;
mod match_;
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
mod runner;
mod sandbox;
mod scheduler;
mod session;
mod specifier;
mod store;
mod sync;
mod tag;
mod temp;
mod token;
mod user;
mod vfs;
mod watch;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Shutdown {
	Interrupt,
	Terminate,
}

pub struct State {
	authentication_tokens: Tokens,
	authorization_tokens: Tokens,
	billing: Option<self::billing::Stripe>,
	checkin_tasks: self::checkin::Tasks,
	checkout_graph_tasks: self::checkout::internal::GraphTasks,
	checkout_lock: self::checkout::Lock,
	checkout_tasks: self::checkout::internal::Tasks,
	checkpoints: Option<self::checkpoint::State>,
	clock: self::clock::Clock,
	config: Config,
	context: Context,
	database: Database,
	diagnostics: Mutex<Vec<tg::Diagnostic>>,
	index: Index,
	index_tasks: tangram_futures::task::Set<tg::Result<()>>,
	#[cfg(target_os = "linux")]
	ip_pool: tangram_sandbox::network::ip::Pool,
	library: Mutex<Option<Arc<Temp>>>,
	lock: Mutex<Option<tokio::fs::File>>,
	log_notifications: self::process::log::Notifications,
	messenger: Messenger,
	next_watch_id: AtomicU64,
	object_get_tasks: self::object::get::Tasks,
	path: PathBuf,
	regions: DashMap<String, tg::Client, fnv::FnvBuildHasher>,
	remote_clients: DashMap<Uri, tg::Client, fnv::FnvBuildHasher>,
	remote_list_tasks: self::list::remote::Tasks,
	remote_object_put_tasks: tangram_futures::task::Set<()>,
	remote_process_put_tasks: tangram_futures::task::Set<()>,
	runner: self::runner::Runner,
	sandbox_container_root: PathBuf,
	sandbox_seatbelt_root: PathBuf,
	sandbox_tasks: self::sandbox::Tasks,
	sandbox_vm_image: Option<PathBuf>,
	#[cfg(target_os = "linux")]
	sandbox_vm_image_lock: tokio::sync::Mutex<bool>,
	#[cfg(target_os = "linux")]
	sandbox_vm_snapshot_lock: tokio::sync::Mutex<()>,
	shutdown: tokio::sync::watch::Sender<Option<Shutdown>>,
	store: self::store::Store,
	tangram_path: PathBuf,
	temps: DashSet<PathBuf, fnv::FnvBuildHasher>,
	version: String,
	vfs: Mutex<Option<self::vfs::Server>>,
	watches: DashMap<self::watch::Key, Watch, fnv::FnvBuildHasher>,
}

pub struct Tokens {
	pub private_key: Option<tg::authorization::PrivateKey>,
	pub public_keys: BTreeMap<String, tg::authorization::PublicKey>,
}

impl Owned {
	pub fn shutdown(&self, shutdown: Shutdown) {
		self.server.shutdown.send_replace(Some(shutdown));
		if let Some(task) = self.server.runner.task().lock().unwrap().as_ref() {
			task.stop();
		}
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
		// Validate the configuration.
		config.usage.validate()?;
		let initial_authorization = authorization_search_config(&config.authorization.initial);
		initial_authorization
			.validate()
			.map_err(|error| tg::error!(!error, "invalid initial authorization configuration"))?;
		let final_authorization = authorization_search_config(&config.authorization.final_);
		final_authorization
			.validate()
			.map_err(|error| tg::error!(!error, "invalid final authorization configuration"))?;

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

		// Get the available parallelism.
		let parallelism =
			std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);

		// Remove an existing socket file.
		let socket_path = path.join("socket");
		tokio::fs::remove_file(&socket_path).await.ok();

		// Create the checkout graph tasks.
		let checkout_graph_tasks = tangram_futures::task::Map::default();

		// Create the checkout tasks.
		let checkout_tasks = tangram_futures::task::Map::default();

		// Create the checkin tasks.
		let checkin_tasks = tangram_futures::task::Map::default();

		// Create the checkpoints.
		let checkpoints = config
			.advanced
			.checkpoints
			.then(self::checkpoint::State::new);

		// Create the context.
		let context = Context::root();

		// Validate the indexer configuration.
		if config.roles.contains(&self::config::Role::Indexer) {
			let indexer = &config.indexer;
			if indexer.database_outbox_wakeup_interval.is_zero() {
				return Err(tg::error!(
					"the indexer database outbox wakeup interval must be greater than zero"
				));
			}
			if indexer.log_compaction.enabled {
				if indexer.log_compaction.batch_size == 0 {
					return Err(tg::error!(
						"the indexer log compaction batch size must be greater than zero"
					));
				}
				if indexer.log_compaction.concurrency == 0 {
					return Err(tg::error!(
						"the indexer log compaction concurrency must be greater than zero"
					));
				}
				if indexer.log_compaction.wakeup_interval.is_zero() {
					return Err(tg::error!(
						"the indexer log compaction wakeup interval must be greater than zero"
					));
				}
			}
			for (name, update) in [
				("grant", &indexer.updates.grants),
				("node", &indexer.updates.nodes),
				("storage", &indexer.usage.storage),
			] {
				if update.batch_size == 0 {
					return Err(tg::error!(
						"the indexer {name} update batch size must be greater than zero"
					));
				}
				if update.concurrency == 0 {
					return Err(tg::error!(
						"the indexer {name} update concurrency must be greater than zero"
					));
				}
			}
			if indexer.usage.aggregation.enabled {
				if indexer.usage.aggregation.batch_size == 0 {
					return Err(tg::error!(
						"the indexer usage aggregation batch size must be greater than zero"
					));
				}
				if indexer.usage.aggregation.concurrency == 0 {
					return Err(tg::error!(
						"the indexer usage aggregation concurrency must be greater than zero"
					));
				}
				if indexer.usage.aggregation.poll_interval.is_zero() {
					return Err(tg::error!(
						"the indexer usage aggregation poll interval must be greater than zero"
					));
				}
			}
			if indexer.message_timeout.is_zero() {
				return Err(tg::error!(
					"the indexer message timeout must be greater than zero"
				));
			}
			if indexer.object_outbox_wakeup_interval.is_zero() {
				return Err(tg::error!(
					"the indexer object outbox wakeup interval must be greater than zero"
				));
			}
			if indexer.partition_end <= indexer.partition_start {
				return Err(tg::error!(
					"the indexer partition end must be greater than the partition start"
				));
			}
			if !config.advanced.single_process
				&& indexer.partition_end > config.object.outbox.partition_total
			{
				return Err(tg::error!(
					"the indexer partition range exceeds the object outbox partition total"
				));
			}
			if indexer.poll_interval.is_zero() {
				return Err(tg::error!(
					"the indexer poll interval must be greater than zero"
				));
			}
		}

		// Validate the database outbox configuration.
		let outbox = config.database.outbox();
		if outbox.batch_size == 0 {
			return Err(tg::error!(
				"the database outbox batch size must be greater than zero"
			));
		}

		// Validate the object outbox configuration.
		let outbox = &config.object.outbox;
		if outbox.batch_size == 0 {
			return Err(tg::error!(
				"the object outbox batch size must be greater than zero"
			));
		}
		if outbox.fragment_size == 0 {
			return Err(tg::error!(
				"the object outbox fragment size must be greater than zero"
			));
		}
		if outbox.partition_total == 0 {
			return Err(tg::error!(
				"the object outbox partition total must be greater than zero"
			));
		}

		// Validate the sync configuration.
		if config.sync.get.database.batch_size == 0 {
			return Err(tg::error!(
				"the sync get database batch size must be greater than zero"
			));
		}

		// Validate the process wakeup intervals.
		for (name, interval) in [
			("children", config.process.children_wakeup_interval),
			("status", config.process.status_wakeup_interval),
			("stdio", config.process.stdio_wakeup_interval),
		] {
			if interval.is_zero() {
				return Err(tg::error!(
					"the process {name} wakeup interval must be greater than zero"
				));
			}
		}

		// Validate the sandbox wakeup intervals.
		for (name, interval) in [
			("processes", config.sandbox.processes_wakeup_interval),
			("status", config.sandbox.status_wakeup_interval),
		] {
			if interval.is_zero() {
				return Err(tg::error!(
					"the sandbox {name} wakeup interval must be greater than zero"
				));
			}
		}
		if config
			.sandbox
			.isolation
			.container
			.as_ref()
			.is_some_and(|container| container.max_pids == Some(0))
		{
			return Err(tg::error!(
				"the maximum number of container sandbox pids must be greater than zero"
			));
		}

		// Validate the regions.
		if config.region.as_ref().is_some_and(String::is_empty) {
			return Err(tg::error!("the region must not be empty"));
		}
		if config.primary_region.as_ref().is_some_and(String::is_empty) {
			return Err(tg::error!("the primary region must not be empty"));
		}
		if config
			.regions
			.as_ref()
			.is_some_and(|regions| regions.iter().any(|region| region.name.is_empty()))
		{
			return Err(tg::error!("the region name must not be empty"));
		}
		if config
			.regions
			.as_ref()
			.is_some_and(|regions| !regions.is_empty())
			&& config.primary_region.is_none()
		{
			return Err(tg::error!(
				"the primary region must be configured when regions are configured"
			));
		}
		if let Some(primary_region) = &config.primary_region {
			let primary_region_is_configured = config.region.as_ref() == Some(primary_region)
				|| config.regions.as_ref().is_some_and(|regions| {
					regions.iter().any(|region| &region.name == primary_region)
				});
			if !primary_region_is_configured {
				return Err(tg::error!("the primary region is not configured"));
			}
		}

		if config.roles.contains(&self::config::Role::Scheduler) {
			let scheduler = &config.scheduler;
			if scheduler.create_sandbox_queue_capacity == 0 {
				return Err(tg::error!(
					"the scheduler create sandbox queue capacity must be greater than zero"
				));
			}
			if scheduler.default_cpu == 0 {
				return Err(tg::error!(
					"the default sandbox CPU must be greater than zero"
				));
			}
			if scheduler.default_memory == 0 {
				return Err(tg::error!(
					"the default sandbox memory must be greater than zero"
				));
			}
			if scheduler.max_create_sandbox_requests == 0 {
				return Err(tg::error!(
					"the maximum number of scheduler create sandbox requests must be greater than zero"
				));
			}
			if scheduler.max_create_sandbox_requests_per_runner == 0 {
				return Err(tg::error!(
					"the maximum number of scheduler create sandbox requests per runner must be greater than zero"
				));
			}
		}

		// Create the runner state.
		let capacity = if config.roles.contains(&self::config::Role::Runner) {
			let runner = &config.runner;
			let cpus = runner
				.cpus
				.unwrap_or_else(|| u64::try_from(parallelism).unwrap());
			let default_memory = config.scheduler.default_memory;
			let memory = runner
				.memory
				.unwrap_or_else(|| default_memory.saturating_mul(cpus));
			let capacity = tg::runner::Capacity { cpus, memory };
			if capacity.cpus == 0 {
				return Err(tg::error!(
					"the runner CPU capacity must be greater than zero"
				));
			}
			if capacity.memory == 0 {
				return Err(tg::error!(
					"the runner memory capacity must be greater than zero"
				));
			}
			capacity
		} else {
			tg::runner::Capacity::default()
		};
		let sandbox_pool_size = if config.roles.contains(&self::config::Role::Runner) {
			config.runner.sandbox_pool_size
		} else {
			0
		};
		let runner_config = self::runner::Config {
			capacity,
			sandbox_pool_size,
		};
		let runner = self::runner::Runner::new(runner_config);

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
						read: db::postgres::PoolOptions {
							max: options.read.pool.max.unwrap_or(parallelism),
							min: options.read.pool.min.unwrap_or(0),
							ttl: options.read.pool.ttl,
							url: options.read.url.clone(),
						},
						retry: options.retry.clone().into(),
						write: db::postgres::PoolOptions {
							max: options.write.pool.max.unwrap_or(parallelism),
							min: options.write.pool.min.unwrap_or(0),
							ttl: options.write.pool.ttl,
							url: options.write.url.clone(),
						},
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

		// Create the diagnostics.
		let diagnostics = Mutex::new(Vec::new());

		// Create the clock.
		let clock = self::clock::Clock::new();
		clock.now()?;

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
					let authorize = tangram_index::fdb::AuthorizeConfig {
						concurrency: options.authorize.concurrency,
						process_object_grant: final_authorization,
					};
					let options = tangram_index::fdb::Options {
						authorize,
						cluster: options.cluster.clone(),
						instance: options.instance.clone(),
						max_process_depth: config
							.roles
							.contains(&self::config::Role::Indexer)
							.then(|| u64::try_from(config.indexer.max_process_depth).unwrap()),
						partition_total: options.partition_total,
						read_request_batch_size: options.read_request_batch_size,
						read_transaction_concurrency: options.read_transaction_concurrency,
						usage_partition_total: options.usage_partition_total,
						write_operation_batch_size: options.write_operation_batch_size,
						write_transaction_concurrency: options.write_transaction_concurrency,
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
					let authorize = tangram_index::lmdb::AuthorizeConfig {
						process_object_grant: final_authorization,
					};
					let path = directory.join(&options.path);
					let config = tangram_index::lmdb::Config {
						authorize,
						map_size: options.map_size,
						max_process_depth: config
							.roles
							.contains(&self::config::Role::Indexer)
							.then(|| u64::try_from(config.indexer.max_process_depth).unwrap()),
						path,
						read_request_batch_size: options.read_request_batch_size,
						read_transaction_concurrency: options.read_transaction_concurrency,
						usage_partition_total: options.usage_partition_total,
						write_operation_batch_size: options.write_operation_batch_size,
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
		let instance = config.instance.clone();
		let region = config.region.clone();
		let messenger = match &config.messenger {
			self::config::Messenger::Memory => Messenger::memory(instance, region),
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
					if let (Some(username), Some(password)) = (&nats.username, &nats.password) {
						options = options.user_and_password(username.clone(), password.clone());
					}
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
					Messenger::nats(client, instance, region)
				}
			},
		};
		let log_notifications = self::process::log::Notifications::new(messenger.clone());

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

		// Create the remote object put tasks.
		let remote_object_put_tasks = tangram_futures::task::Set::default();

		// Create the remote process put tasks.
		let remote_process_put_tasks = tangram_futures::task::Set::default();

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
					self::store::Store::new_lmdb(&path, lmdb)
						.map_err(|error| tg::error!(!error, "failed to create the store"))?
				}
			},
			config::Store::Memory(_) => self::store::Store::new_memory(),
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
					self::store::Store::new_scylla(scylla)
						.await
						.map_err(|error| tg::error!(!error, "failed to create the store"))?
				}
			},
		};

		// Create the temp paths.
		let temps = DashSet::default();

		// Create the shutdown channel.
		let (shutdown, _) = tokio::sync::watch::channel(None);

		// Get the version.
		let version = config
			.version
			.clone()
			.unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());

		// Create the vfs.
		let vfs = Mutex::new(None);

		// Create the checkout lock.
		let checkout_lock_path = if config.checkouts && config.vfs.is_some() {
			path.join("checkouts.lock")
		} else {
			path.join("store.lock")
		};
		let checkout_lock =
			self::checkout::Lock::new(&checkout_lock_path, config.advanced.single_process);

		// Create the watches.
		let next_watch_id = AtomicU64::new(0);
		let watches = DashMap::default();

		// Create the token keys.
		let authentication_tokens =
			load_token_keys(Some(&config.authentication.tokens.keys)).await?;
		let authorization_tokens = load_token_keys(config.authorization.tokens.as_ref()).await?;

		// Create the billing provider.
		let billing = config
			.billing
			.as_ref()
			.map(|billing| self::billing::Stripe::new(&billing.stripe));

		// Create the server.
		let server = Self(Arc::new(State {
			authentication_tokens,
			authorization_tokens,
			billing,
			checkin_tasks,
			checkout_graph_tasks,
			checkout_lock,
			checkout_tasks,
			checkpoints,
			clock,
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
			log_notifications,
			messenger,
			next_watch_id,
			object_get_tasks,
			path,
			regions,
			remote_clients,
			remote_list_tasks,
			remote_object_put_tasks,
			remote_process_put_tasks,
			runner,
			sandbox_container_root,
			sandbox_seatbelt_root,
			sandbox_tasks,
			sandbox_vm_image,
			#[cfg(target_os = "linux")]
			sandbox_vm_image_lock: tokio::sync::Mutex::new(false),
			#[cfg(target_os = "linux")]
			sandbox_vm_snapshot_lock: tokio::sync::Mutex::new(()),
			shutdown,
			store,
			tangram_path,
			temps,
			version,
			vfs,
			watches,
		}));

		// Start usage tracking.
		if server.config.usage.enabled {
			server.index.start_usage(server.clock.now()?).await?;
		}

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

		// Destroy unfinished sandboxes if single process mode is enabled.
		if server.config().advanced.single_process {
			let result = server.destroy_unfinished_sandboxes().await;
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), "failed to destroy unfinished sandboxes");
			}
		}

		// Set the remotes if specified in the config.
		if server.is_primary_region()
			&& let Some(remotes) = &server.config.remotes
		{
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

		// Initialize the checkouts directory and start the VFS if enabled.
		if server.checkouts_enabled() {
			let checkout_guard = server.checkout_lock.acquire().await?;
			let store_path = server.store_path();
			let checkout_path = server.path.join("checkouts");
			let vfs_kind = match server.config.vfs.clone().unwrap_or_default().kind {
				config::VfsKind::Auto => {
					if cfg!(target_os = "macos")
						&& std::env::var_os("TANGRAM_MACOS_APP_SOCKET").is_some()
					{
						vfs::Kind::Fskit
					} else if cfg!(target_os = "macos") {
						vfs::Kind::Nfs
					} else if cfg!(target_os = "linux") {
						vfs::Kind::Fuse
					} else {
						unreachable!()
					}
				},
				config::VfsKind::Fskit => vfs::Kind::Fskit,
				config::VfsKind::Fuse => vfs::Kind::Fuse,
				config::VfsKind::Nfs => vfs::Kind::Nfs,
			};
			let store_exists = match tokio::fs::try_exists(&store_path).await {
				Ok(exists) => exists,
				Err(error) if error.raw_os_error() == Some(libc::ENOTCONN) => {
					self::vfs::Server::unmount(vfs_kind, &store_path).await?;
					true
				},
				Err(error) => {
					return Err(tg::error!(!error, "failed to stat the path"));
				},
			};
			let checkout_exists = tokio::fs::try_exists(&checkout_path)
				.await
				.map_err(|error| tg::error!(!error, "failed to stat the path"))?;
			if let Some(options) = server.config.vfs.clone() {
				if store_exists && !checkout_exists {
					tokio::fs::rename(&store_path, &checkout_path)
						.await
						.map_err(|error| {
							tg::error!(
								!error,
								"failed to move the store directory to the checkouts path"
							)
						})?;
				}
				tokio::fs::create_dir_all(&store_path)
					.await
					.map_err(|error| tg::error!(!error, "failed to create the store directory"))?;
				tokio::fs::create_dir_all(&checkout_path)
					.await
					.map_err(|error| {
						tg::error!(!error, "failed to create the checkouts directory")
					})?;
				let vfs = self::vfs::Server::start(
					&server,
					vfs_kind,
					&store_path,
					options,
					Origin::Host,
					Arc::new(std::sync::Mutex::new(Some(tg::Principal::Root))),
					None,
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to start the VFS"))?;
				server.vfs.lock().unwrap().replace(vfs);
			} else {
				if checkout_exists {
					tokio::fs::rename(&checkout_path, &store_path)
						.await
						.map_err(|error| {
							tg::error!(
								!error,
								"failed to move the checkouts directory to the store path"
							)
						})?;
					// Remove named checkout entries before exposing the physical store.
					server
						.remove_all_named_checkout_entries_with_lock(&checkout_guard)
						.await?;
				}
				tokio::fs::create_dir_all(&store_path)
					.await
					.map_err(|error| tg::error!(!error, "failed to create the store directory"))?;
			}
			drop(checkout_guard);
		}

		// Spawn the indexer task.
		let indexer_task = server
			.config
			.roles
			.contains(&self::config::Role::Indexer)
			.then(|| {
				let config = server.config.indexer.clone();
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
		let cleaner_task = server
			.config
			.roles
			.contains(&self::config::Role::Cleaner)
			.then(|| {
				let config = server.config.cleaner.clone();
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

		// Spawn the scheduler task.
		let scheduler_task = server
			.config
			.roles
			.contains(&self::config::Role::Scheduler)
			.then(|| {
				let config = server.config.scheduler.clone();
				Task::spawn({
					let server = server.clone();
					|_| async move {
						server.scheduler_task(&config).await;
					}
				})
			});

		// Spawn the HTTP task.
		let http_listeners = if server.config.roles.contains(&self::config::Role::Http) {
			let config = &server.config.http;
			let mut listeners = if config.listeners.is_empty() {
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
			};
			// Add the socket shared with the macOS extension.
			let group_socket = server
				.config
				.vfs
				.clone()
				.unwrap_or_default()
				.resolved_app_group_socket();
			if let Some(path) = group_socket {
				let url = Uri::builder()
					.scheme("http+unix")
					.authority(path.to_str().unwrap())
					.path("")
					.build()
					.unwrap();
				listeners.push(crate::config::HttpListener { url, tls: None });
			}
			listeners
		} else {
			Vec::new()
		};
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
					listeners.push((listener, listener_config.clone(), Origin::Host));
				}
			}
			Some(Task::spawn(move |stopper| {
				let server = http_server.clone();
				async move {
					let tasks = FuturesUnordered::new();
					for (listener, listener_config, origin) in listeners {
						let server = server.clone();
						let stopper = stopper.clone();
						tasks.push(
							async move {
								server
									.serve(listener, listener_config, origin, stopper)
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
								server.serve_stream(stream, Origin::Host, stopper).await;
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

		// Spawn the runner task.
		if server.config.roles.contains(&self::config::Role::Runner) {
			let task = Task::spawn({
				let server = server.clone();
				let id = server
					.config
					.runner
					.id
					.clone()
					.unwrap_or_else(tg::runner::Id::new);
				let context = Context {
					principal: tg::Principal::Runner(id.clone()),
					..server.context.clone()
				};
				let session = server.session(&context);
				|stopper| async move {
					session.runner_task(id, stopper).boxed().await;
				}
			});
			server.runner.task().lock().unwrap().replace(task);
		}

		let shutdown = {
			let server = server.clone();
			async move {
				tracing::trace!("started");

				// Stop the HTTP and runner tasks.
				if let Some(task) = &http_task {
					task.stop();
				}
				let runner_task = server.runner.task().lock().unwrap().take();
				if let Some(task) = &runner_task {
					task.stop();
				}

				// Await the HTTP task.
				if let Some(task) = http_task {
					let result = task.wait().await;
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "the http task panicked");
					}
					tracing::trace!("http task");
				}

				// Await the runner task.
				if let Some(task) = runner_task {
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

				// Abort the checkout graph tasks.
				server.checkout_graph_tasks.abort_all();
				let results = server.checkout_graph_tasks.wait().await;
				for result in results {
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "a checkout graph task failed");
					}
				}
				tracing::trace!("checkout graph tasks");

				// Abort the checkout tasks.
				server.checkout_tasks.abort_all();
				let results = server.checkout_tasks.wait().await;
				for result in results {
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "a checkout task panicked");
					}
				}
				tracing::trace!("checkout tasks");

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

				// Abort the remote object put tasks.
				server.remote_object_put_tasks.abort_all();
				server.remote_object_put_tasks.wait().await;
				tracing::trace!("remote object put tasks");

				// Abort the remote process put tasks.
				server.remote_process_put_tasks.abort_all();
				server.remote_process_put_tasks.wait().await;
				tracing::trace!("remote process put tasks");

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

				// Abort the scheduler task.
				if let Some(task) = scheduler_task {
					task.abort();
					let result = task.wait().await;
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "the scheduler task panicked");
					}
					tracing::trace!("scheduler task");
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

	async fn destroy_unfinished_sandboxes(&self) -> tg::Result<()> {
		let session = self.session(&self.context);
		let outputs = session
			.list_sandboxes_local(None, None)
			.await
			.map_err(|error| tg::error!(!error, "failed to list sandboxes"))?;
		outputs
			.into_iter()
			.map(|output| async move {
				let result = self
					.destroy_expired_runner_sandbox(&output.id)
					.boxed()
					.await;
				if let Err(error) = result {
					tracing::error!(sandbox = %output.id, error = %error.trace(), "failed to destroy the sandbox");
				}
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<()>()
			.await;
		Ok(())
	}

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
		let http = &self.config().http;
		let url = if http.listeners.is_empty() {
			Some(default_url())
		} else {
			http.listeners
				.iter()
				.find(|listener| !matches!(listener.url.scheme(), Some("http+stdio")))
				.map(|listener| listener.url.clone())
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
			r"
				select name, token
				from remotes
				where principal is null;
			",
		);
		let result = transaction
			.query_all_into::<RemoteTokenRow>(statement.into(), db::params![])
			.await;
		let tokens = crate::database::retry!(result, "failed to execute the statement")
			.into_iter()
			.map(|row| (row.name, row.token))
			.collect::<std::collections::BTreeMap<_, _>>();
		let statement = indoc!(
			r"
				delete from remotes
				where principal is null;
			",
		);
		let result = transaction.execute(statement.into(), db::params![]).await;
		crate::database::retry!(result, "failed to delete the remotes");
		for (name, remote) in remotes {
			let p = transaction.p();
			let statement = formatdoc!(
				r"
					insert into remotes (name, principal, token, trusted, url)
					values ({p}1, null, {p}2, {p}3, {p}4);
				",
			);
			let token = remote
				.token
				.clone()
				.or_else(|| tokens.get(name).cloned().flatten());
			let params = db::params![name.clone(), token, remote.trusted, remote.url.to_string()];
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
	fn checkouts_enabled(&self) -> bool {
		self.config.checkouts
	}

	#[must_use]
	fn named_checkout_maintenance_enabled(&self) -> bool {
		self.checkouts_enabled() && self.vfs.lock().unwrap().is_none()
	}

	#[must_use]
	fn store_path(&self) -> PathBuf {
		self.path.join("store")
	}

	#[must_use]
	fn checkout_path(&self) -> PathBuf {
		if self.vfs.lock().unwrap().is_some() {
			self.path.join("checkouts")
		} else {
			self.store_path()
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
		self.checkout_graph_tasks.abort_all();
		self.checkout_tasks.abort_all();
		self.library.lock().unwrap().take();
		self.sandbox_tasks.abort_all();
		self.object_get_tasks.abort_all();
		self.remote_object_put_tasks.abort_all();
		self.remote_process_put_tasks.abort_all();
		self.remote_list_tasks.abort_all();
		self.index_tasks.abort_all();
		self.vfs.lock().unwrap().take();
		self.watches.clear();
	}
}

fn authorization_search_config(
	config: &self::config::AuthorizationSearches,
) -> tangram_index::authorize::Config {
	let ancestor = tangram_index::authorize::SearchConfig {
		max_depth: config.ancestor.max_depth,
		max_edges: config.ancestor.max_edges,
		max_nodes: config.ancestor.max_nodes,
		page_size: config.ancestor.page_size,
	};
	let descendant = tangram_index::authorize::SearchConfig {
		max_depth: config.descendant.max_depth,
		max_edges: config.descendant.max_edges,
		max_nodes: config.descendant.max_nodes,
		page_size: config.descendant.page_size,
	};
	let subtree = tangram_index::authorize::SubtreeConfig {
		max_depth: config.subtree.max_depth,
		max_objects: config.subtree.max_objects,
		max_processes: config.subtree.max_processes,
	};
	tangram_index::authorize::Config {
		ancestor,
		descendant,
		subtree,
	}
}

async fn load_token_keys(config: Option<&config::TokenKeys>) -> tg::Result<Tokens> {
	let private_key = match config.and_then(|config| config.private_key.as_ref()) {
		Some(config) => {
			let bytes = match &config.path {
				Some(path) => match config.algorithm {
					tg::authorization::Algorithm::Ed25519 => tokio::fs::read(path).await.map_err(
						|error| tg::error!(!error, path = %path.display(), "failed to read the private key"),
					)?,
				},
				None => match config.algorithm {
					tg::authorization::Algorithm::Ed25519 => {
						tg::authorization::PrivateKey::generate(
							config.name.clone(),
							config.algorithm,
						)?
						.bytes
					},
				},
			};
			Some(tg::authorization::PrivateKey::new(
				config.name.clone(),
				config.algorithm,
				bytes,
			))
		},
		None => None,
	};
	let mut public_keys = BTreeMap::new();
	if let Some(config) = config {
		for config in &config.public_keys {
			let bytes = match &config.path {
				Some(path) => match config.algorithm {
					tg::authorization::Algorithm::Ed25519 => tokio::fs::read(path).await.map_err(
						|error| tg::error!(!error, path = %path.display(), "failed to read the public key"),
					)?,
				},
				None => match config.algorithm {
					tg::authorization::Algorithm::Ed25519 => {
						let matching_private_key = private_key.as_ref().filter(|private_key| {
							private_key.name == config.name
								&& private_key.algorithm == config.algorithm
						});
						let key = if let Some(private_key) = matching_private_key {
							tg::authorization::PublicKey::from_private_key(private_key)?
						} else {
							let private_key = tg::authorization::PrivateKey::generate(
								config.name.clone(),
								config.algorithm,
							)?;
							tg::authorization::PublicKey::from_private_key(&private_key)?
						};
						key.bytes
					},
				},
			};
			let key =
				tg::authorization::PublicKey::new(config.name.clone(), config.algorithm, bytes);
			if public_keys.insert(config.name.clone(), key).is_some() {
				return Err(tg::error!(name = %config.name, "duplicate public key"));
			}
		}
	}
	let tokens = Tokens {
		private_key,
		public_keys,
	};

	Ok(tokens)
}
