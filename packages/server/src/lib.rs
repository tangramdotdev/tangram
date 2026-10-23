use {
	self::{
		archive::Archive,
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
		path::{Path, PathBuf},
		sync::{Arc, Mutex, atomic::AtomicU64},
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::task::Task,
	tangram_uri::Uri,
	tangram_util::fs::remove,
	tokio::io::AsyncWriteExt as _,
	tracing::Instrument as _,
};

mod archive;
mod authentication;
mod authorization;
mod billing;
mod cache;
mod capture;
mod check;
mod checkin;
mod checkout;
mod checkpoint;
mod checksum;
mod children;
mod clean;
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
mod graph;
mod group;
mod health;
mod http;
mod index;
mod indexer;
mod instance;
mod list;
mod location;
mod log;
mod match_;
mod messenger;
mod migrations;
mod module;
mod node;
mod notification;
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
mod sync;
mod tag;
mod temp;
mod token;
mod usage;
mod user;
mod verify;
mod vfs;
mod watch;
mod write;

pub use self::config::Config;

pub mod config;
pub mod cursor;
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
	archive: Option<Archive>,

	archive_tasks: tangram_futures::task::Set<tg::Result<()>>,

	authentication_cache: Arc<self::authentication::cache::Cache>,

	authentication_tokens: Tokens,

	authorization_tokens: Tokens,

	billing: Option<self::billing::Billing>,

	cache: self::cache::Cache,

	checkin_tasks: self::checkin::Tasks,

	checkout_graph_tasks: self::checkout::internal::GraphTasks,

	checkout_lock: self::checkout::Lock,

	checkout_tasks: self::checkout::internal::Tasks,

	checkout_xattr_value_size: Mutex<Option<usize>>,

	checkpoints: Option<Arc<self::checkpoint::State>>,

	clock: self::clock::Clock,

	config: Config,

	context: Context,

	database: Database,

	diagnostics: Mutex<Vec<tg::Diagnostic>>,

	index: Index,

	index_changed: tokio::sync::Notify,

	index_tasks: tangram_futures::task::Set<tg::Result<()>>,

	index_wait_sender: self::index::WaitSender,

	indexers: self::indexer::Cache,

	#[cfg(target_os = "linux")]
	ip_pool: tangram_sandbox::network::ip::Pool,

	library: Mutex<Option<Arc<Temp>>>,

	lock: Mutex<Option<tokio::fs::File>>,

	messenger: Messenger,

	next_watch_id: AtomicU64,

	notifications: self::notification::Notifications,

	object_get_tasks: self::object::get::Tasks,

	path: PathBuf,

	process_connect_clients:
		tangram_messenger::service::Clients<self::process::connect::service::Protocol, Messenger>,

	process_connect_service_tasks: tangram_futures::task::Set<tg::Result<()>>,

	process_connect_services: DashMap<
		tg::process::Nonce,
		tokio::sync::mpsc::UnboundedSender<self::process::connect::service::LocalRequest>,
		fnv::FnvBuildHasher,
	>,

	process_connect_services_changed: tokio::sync::Notify,

	process_control_clients:
		tangram_messenger::service::Clients<self::process::control::service::Protocol, Messenger>,

	process_control_service_tasks: tangram_futures::task::Set<tg::Result<()>>,

	process_control_services:
		DashMap<tg::process::Id, self::process::control::service::Local, fnv::FnvBuildHasher>,

	process_control_services_changed: tokio::sync::Notify,

	regions: DashMap<String, tg::Client, fnv::FnvBuildHasher>,

	remote_clients: DashMap<Uri, tg::Client, fnv::FnvBuildHasher>,

	remote_list_tasks: self::list::remote::Tasks,

	remote_object_put_tasks: tangram_futures::task::Set<()>,

	remote_process_put_tasks: tangram_futures::task::Set<()>,

	runner: self::runner::Runner,

	sandbox_assets: self::runner::assets::Assets,

	sandbox_control_clients:
		tangram_messenger::service::Clients<self::sandbox::control::service::Protocol, Messenger>,

	sandbox_control_service_tasks: tangram_futures::task::Set<tg::Result<()>>,

	sandbox_control_services:
		DashMap<tg::sandbox::Id, self::sandbox::control::service::Local, fnv::FnvBuildHasher>,

	sandbox_control_services_changed: tokio::sync::Notify,

	sandbox_tasks: self::sandbox::Tasks,

	shutdown: tokio::sync::watch::Sender<Option<Shutdown>>,

	sync_clients: tangram_messenger::service::Clients<self::sync::service::Protocol, Messenger>,

	sync_index_tasks: tangram_futures::task::Set<tg::Result<()>>,

	sync_service_tasks: tangram_futures::task::Set<tg::Result<()>>,

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
		config
			.http
			.http2
			.validate()
			.map_err(|source| tg::error!(!source, "invalid HTTP/2 configuration"))?;
		config
			.sync
			.service
			.validate()
			.map_err(|source| tg::error!(!source, "invalid sync service configuration"))?;
		if config.authentication.cache.capacity != 0 && config.authentication.cache.ttl.is_zero() {
			return Err(tg::error!(
				"the authentication cache ttl must be greater than zero"
			));
		}
		config
			.process
			.stdio_relay
			.validate()
			.map_err(|source| tg::error!(!source, "invalid stdio relay configuration"))?;
		if (config.sync.permission_time_to_live / 2).is_zero() {
			return Err(tg::error!(
				"the sync permission time to live must be greater than zero"
			));
		}
		if config.scheduler.message_ack_timeout.is_zero() {
			return Err(tg::error!(
				"the scheduler message ack timeout must be greater than zero"
			));
		}
		config
			.process
			.control
			.validate()
			.map_err(|source| tg::error!(!source, "invalid process control configuration"))?;
		let connect = &config.process.connect;
		if connect.max_operations == 0 || connect.max_pending_messages == 0 {
			return Err(tg::error!(
				"the process connect max operations and max pending messages must be greater than zero"
			));
		}
		let log_batch = &config.process.stdio_log_batch;
		if log_batch.channel_capacity == 0 || log_batch.max_chunks == 0 || log_batch.max_size == 0 {
			return Err(tg::error!(
				"the process stdio log batch channel capacity, max chunks, and max size must be greater than zero"
			));
		}
		if config.process.stdio_open_timeout.is_zero() {
			return Err(tg::error!(
				"the process stdio open timeout must be greater than zero"
			));
		}
		if config.runner.stdio_buffer_chunks == 0 || config.runner.stdio_buffer_size == 0 {
			return Err(tg::error!(
				"the runner stdio buffer chunks and size must be greater than zero"
			));
		}
		if config.sync.max_frame_size == 0 {
			return Err(tg::error!(
				"the sync max frame size must be greater than zero"
			));
		}
		for (name, duration) in [
			(
				"advanced version check interval",
				config.advanced.version_check_interval,
			),
			("control request timeout", config.control.request_timeout),
			("HTTP request timeout", config.http.request_timeout),
			(
				"indexer billing cleanup poll interval",
				config.indexer.billing_cleanup.poll_interval,
			),
			(
				"indexer updates poll interval",
				config.indexer.updates.poll_interval,
			),
			("progress interval", config.progress.interval),
			(
				"runner progress log interval",
				config.runner.progress_log_interval,
			),
			(
				"runner progress update interval",
				config.runner.progress_update_interval,
			),
			(
				"sync metadata poll interval",
				config.sync.metadata_poll_interval,
			),
			("sync progress interval", config.sync.progress_interval),
			(
				"verification poll interval",
				config.verification.poll_interval,
			),
			(
				"verification request timeout",
				config.verification.request_timeout,
			),
		] {
			if duration.is_zero() {
				return Err(tg::error!("the {name} must be greater than zero"));
			}
		}
		if config.http.max_arg_length == 0 {
			return Err(tg::error!(
				"the HTTP max arg length must be greater than zero"
			));
		}
		if let self::config::Messenger::Memory(memory) = &config.messenger
			&& memory.subscription_capacity == 0
		{
			return Err(tg::error!(
				"the memory messenger subscription capacity must be greater than zero"
			));
		}
		if let Some(billing) = &config.billing
			&& billing.stripe.webhook_tolerance.is_zero()
		{
			return Err(tg::error!(
				"the Stripe webhook tolerance must be greater than zero"
			));
		}

		// Validate the channel capacities, batch sizes, concurrencies, and page sizes.
		let vfs = config.vfs.clone().unwrap_or_default();
		let watch = config.watch.clone().unwrap_or_default();
		let sizes = [
			(
				"checkin solve prefetch concurrency",
				config.checkin.solve.prefetch_concurrency,
			),
			(
				"indexer request channel capacity",
				config.indexer.request.channel_capacity,
			),
			(
				"process connect high priority channel capacity",
				config.process.connect.high_priority_channel_capacity,
			),
			(
				"process connect low priority channel capacity",
				config.process.connect.low_priority_channel_capacity,
			),
			(
				"process connect spawn channel capacity",
				config.process.connect.spawn_channel_capacity,
			),
			(
				"process control channel capacity",
				config.process.control_channel_capacity,
			),
			(
				"process local control channel capacity",
				config.process.local_control_channel_capacity,
			),
			(
				"process spawn authorization batch size",
				config.process.spawn.authorization_batch_size,
			),
			(
				"process sync channel capacity",
				config.process.sync_channel_capacity,
			),
			(
				"progress channel capacity",
				config.progress.channel_capacity,
			),
			("read index batch size", config.read.index_batch_size),
			(
				"runner control channel capacity",
				config.runner.control_channel_capacity,
			),
			(
				"runner log channel capacity",
				config.runner.log_channel_capacity,
			),
			(
				"runner process control channel capacity",
				config.runner.process_control_channel_capacity,
			),
			(
				"runner progress channel capacity",
				config.runner.progress_channel_capacity,
			),
			(
				"runner sandbox channel capacity",
				config.runner.sandbox_channel_capacity,
			),
			(
				"sandbox control channel capacity",
				config.sandbox.control_channel_capacity,
			),
			("sync channel capacity", config.sync.channel_capacity),
			(
				"sync get channel capacity",
				config.sync.get.channel_capacity,
			),
			(
				"sync get checkout batch size",
				config.sync.get.checkout.batch_size,
			),
			(
				"sync get checkout channel capacity",
				config.sync.get.checkout.channel_capacity,
			),
			(
				"sync get checkout max read handles",
				config.sync.get.checkout.max_read_handles,
			),
			(
				"sync get checkout max write handles",
				config.sync.get.checkout.max_write_handles,
			),
			(
				"sync get queue checkout batch size",
				config.sync.get.queue.checkout_batch_size,
			),
			(
				"sync message channel capacity",
				config.sync.message_channel_capacity,
			),
			(
				"sync push channel capacity",
				config.sync.push_channel_capacity,
			),
			(
				"sync put channel capacity",
				config.sync.put.channel_capacity,
			),
			(
				"vfs directory snapshot cache capacity",
				vfs.directory_snapshot_cache_capacity,
			),
			(
				"vfs directory snapshot read entry limit",
				vfs.directory_snapshot_read_entry_limit,
			),
			("watch channel capacity", watch.channel_capacity),
		];
		for (name, size) in sizes {
			if size == 0 {
				return Err(tg::error!("the {name} must be greater than zero"));
			}
		}
		let sizes = [
			(
				"object queue recovery batch size",
				config.object.queue_recovery_batch_size,
			),
			(
				"pagination database page length",
				config.pagination.database_page_length,
			),
			(
				"process default children page size",
				config.process.default_children_page_size,
			),
			(
				"process default log read size",
				config.process.default_log_read_size,
			),
			("read default size", config.read.default_size),
			(
				"sandbox default processes page size",
				config.sandbox.default_processes_page_size,
			),
		];
		for (name, size) in sizes {
			if size == 0 {
				return Err(tg::error!("the {name} must be greater than zero"));
			}
		}

		// Validate the sandbox and VFS configuration.
		config
			.sandbox
			.http2
			.validate()
			.map_err(|source| tg::error!(!source, "invalid sandbox HTTP/2 configuration"))?;
		let sandbox = &config.sandbox;
		let sizes = [
			("sandbox build attempts", sandbox.build_attempts),
			(
				"vfs fuse io_uring completion batch size",
				vfs.fuse.io_uring_completion_batch_size,
			),
			(
				"vfs fuse io_uring max retired slots per worker",
				vfs.fuse.io_uring_max_retired_slots_per_worker,
			),
			(
				"vfs fuse io_uring max slots per queue",
				vfs.fuse.io_uring_max_slots_per_queue,
			),
			(
				"vfs fuse io_uring max worker count",
				vfs.fuse.io_uring_max_worker_count,
			),
			(
				"vfs fuse io_uring payload memory budget",
				vfs.fuse.io_uring_payload_memory_budget,
			),
			("vfs fuse max write", vfs.fuse.max_write),
			(
				"vfs fuse read write async concurrency",
				vfs.fuse.read_write_async_concurrency,
			),
			(
				"vfs fuse read write async queue depth",
				vfs.fuse.read_write_async_queue_depth,
			),
			(
				"vfs fuse read write max reader count",
				vfs.fuse.read_write_max_reader_count,
			),
			(
				"vfs fuse sqpoll entries",
				usize::try_from(vfs.fuse.sqpoll_entries).unwrap_or(usize::MAX),
			),
			("vfs nfs max array length", vfs.nfs.max_array_length),
			(
				"vfs nfs max compound operations",
				vfs.nfs.max_compound_operations,
			),
			("vfs nfs max connections", vfs.nfs.max_connections),
			(
				"vfs nfs max global in flight requests",
				vfs.nfs.max_global_in_flight_requests,
			),
			(
				"vfs nfs max in flight requests per connection",
				vfs.nfs.max_in_flight_requests_per_connection,
			),
			("vfs nfs max name length", vfs.nfs.max_name_length),
			(
				"vfs nfs max pending responses",
				vfs.nfs.max_pending_responses,
			),
			(
				"vfs nfs max read size",
				usize::try_from(vfs.nfs.max_read_size).unwrap_or(usize::MAX),
			),
			("vfs nfs max record size", vfs.nfs.max_record_size),
			("vfs nfs readdir concurrency", vfs.nfs.readdir_concurrency),
		];
		for (name, size) in sizes {
			if size == 0 {
				return Err(tg::error!("the {name} must be greater than zero"));
			}
		}
		if vfs.nfs.max_record_size > 0x7fff_ffff {
			return Err(tg::error!(
				"the vfs nfs max record size must not exceed 2147483647 bytes"
			));
		}
		let mut durations = vec![
			(
				"sandbox cgroup cleanup interval",
				sandbox.cgroup_cleanup_interval,
			),
			(
				"sandbox cgroup cleanup timeout",
				sandbox.cgroup_cleanup_timeout,
			),
			("sandbox connect timeout", sandbox.connect_timeout),
			(
				"sandbox network startup timeout",
				sandbox.network.startup_timeout,
			),
			("sandbox request timeout", sandbox.request_timeout),
			("sandbox shutdown timeout", sandbox.shutdown_timeout),
			(
				"vfs fuse io_uring startup timeout",
				vfs.fuse.io_uring_startup_timeout,
			),
			("vfs fuse mount ready timeout", vfs.fuse.mount_ready_timeout),
			(
				"vfs nfs lease reaper interval",
				vfs.nfs.lease_reaper_interval,
			),
			("vfs nfs lease time", vfs.nfs.lease_time),
		];
		if let self::config::RunnerBackend::CloudHypervisor(vm) = &config.runner.backend {
			durations.extend([
				(
					"sandbox vm network interface timeout",
					vm.network_interface_timeout,
				),
				("sandbox vm ready timeout", vm.ready_timeout),
				(
					"sandbox vm serial connect interval",
					vm.serial_connect_interval,
				),
				(
					"sandbox vm serial connect timeout",
					vm.serial_connect_timeout,
				),
			]);
		}
		for (name, duration) in durations {
			if duration.is_zero() {
				return Err(tg::error!("the {name} must be greater than zero"));
			}
		}
		let pagination = &config.pagination;
		if pagination.default_limit == 0 || pagination.default_limit > pagination.max_limit {
			return Err(tg::error!(
				"the pagination default limit must be between 1 and the pagination max limit"
			));
		}
		if config.runner.log_buffer_size < tg::process::stdio::MAX_CHUNK_SIZE {
			return Err(tg::error!(
				"the runner log buffer size must be at least the maximum stdio chunk size"
			));
		}
		config.usage.validate()?;
		verification_search_config(&config.verification.permissions.initial)
			.validate()
			.map_err(|error| tg::error!(!error, "invalid initial verification configuration"))?;
		verification_search_config(&config.verification.permissions.final_)
			.validate()
			.map_err(|error| tg::error!(!error, "invalid final verification configuration"))?;

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

		// Ensure the temp directory exists.
		let temp_path = path.join("tmp");
		tokio::fs::create_dir_all(&temp_path)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the temp directory"))?;

		#[cfg(target_os = "linux")]
		if config.roles.contains(&self::config::Role::Runner) {
			let container = config.runner.backend.resources(&config.runner.resources);
			let options = tangram_sandbox::host::Options {
				harden: config.runner.backend.harden(),
				id_mapped_mounts: container.uid_map.is_some() || container.gid_map.is_some(),
			};
			tangram_sandbox::host::validate(&temp_path, &options).map_err(|error| {
				tg::error!(
					!error,
					"the host does not satisfy the container prerequisites"
				)
			})?;
			if options.harden {
				tracing::info!(
					cpu = ?config.scheduler.default_cpu,
					filesystem_inodes = ?container.max_filesystem_inodes,
					filesystem_size = ?container.max_filesystem_size,
					gid_map = ?container.gid_map.as_ref().map(|map| (map.host, map.count)),
					harden = true,
					max_duration = ?container.max_duration,
					max_open_files = ?container.max_open_files,
					max_pids = ?container.max_pids,
					memory = ?config.scheduler.default_memory,
					seccomp = ?container.seccomp,
					uid_map = ?container.uid_map.as_ref().map(|map| (map.host, map.count)),
					"validated the hardened container isolation profile"
				);
			}
		}

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
			.then(|| Arc::new(self::checkpoint::State::new()));

		// Create the context.
		let context = Context::root();

		// Validate the archive configuration.
		if config.archive.is_some() && !matches!(&config.cache, self::config::Cache::Scylla(_)) {
			return Err(tg::error!("an archive requires the scylla cache"));
		}

		// Create the archive.
		let archive = config
			.archive
			.as_ref()
			.map(|config| match config {
				self::config::Archive::S3(config) => Archive::new_s3(config),
			})
			.transpose()?;

		// Create the archive tasks.
		let archive_tasks = tangram_futures::task::Set::default();

		if config.process.log_cache_partition_total == 0 {
			return Err(tg::error!(
				"the log cache partition total must be greater than zero"
			));
		}

		// Validate the indexer configuration.
		if config.roles.contains(&self::config::Role::Indexer) {
			let indexer = &config.indexer;
			if !config.advanced.single_process && indexer.id.is_none() {
				return Err(tg::error!(
					"the indexer ID is required in multi-process mode"
				));
			}
			if indexer.cleaning.enabled {
				if indexer.cleaning.batch_size == 0 {
					return Err(tg::error!(
						"the indexer cleaning batch size must be greater than zero"
					));
				}
				if indexer.cleaning.concurrency == 0 {
					return Err(tg::error!(
						"the indexer cleaning concurrency must be greater than zero"
					));
				}
				if indexer.cleaning.poll_interval.is_zero() {
					return Err(tg::error!(
						"the indexer cleaning poll interval must be greater than zero"
					));
				}
				if let Some(capacity) = &indexer.cleaning.capacity {
					validate_capacity_threshold(capacity, "indexer cleaning")?;
				}
			}
			let log_cache = &indexer.log_cache;
			if log_cache.enabled
				&& (log_cache.batch_size == 0
					|| log_cache.concurrency == 0
					|| log_cache.poll_interval.is_zero())
			{
				return Err(tg::error!(
					"the log cache batch size, concurrency, and poll interval must be greater than zero"
				));
			}
			if log_cache.partitions.start > log_cache.partitions.end
				|| log_cache.partitions.end > config.process.log_cache_partition_total
			{
				return Err(tg::error!("the log cache partition range is invalid"));
			}

			for (name, update) in [
				("permission", &indexer.updates.permissions),
				(
					"storage and metadata",
					&indexer.updates.storage_and_metadata,
				),
				("usage", &indexer.updates.usage),
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
			if indexer.usage.expiration.enabled {
				if indexer.usage.expiration.batch_size == 0 {
					return Err(tg::error!(
						"the indexer usage expiration batch size must be greater than zero"
					));
				}
				if indexer.usage.expiration.poll_interval.is_zero() {
					return Err(tg::error!(
						"the indexer usage expiration poll interval must be greater than zero"
					));
				}
			}
			let partitions = match &config.index {
				self::config::Index::Fdb(index) => [
					(
						"cleaning",
						&indexer.cleaning.partitions,
						index.cleaning_partition_total,
					),
					(
						"permission update",
						&indexer.updates.permissions.partitions,
						index.permission_update_partition_total,
					),
					(
						"storage and metadata update",
						&indexer.updates.storage_and_metadata.partitions,
						index.storage_and_metadata_update_partition_total,
					),
					(
						"usage update",
						&indexer.updates.usage.partitions,
						index.usage_update_partition_total,
					),
					(
						"usage",
						&indexer.usage_partitions,
						index.usage_partition_total,
					),
				],
				self::config::Index::Lmdb(index) => [
					("cleaning", &indexer.cleaning.partitions, 1),
					(
						"permission update",
						&indexer.updates.permissions.partitions,
						1,
					),
					(
						"storage and metadata update",
						&indexer.updates.storage_and_metadata.partitions,
						1,
					),
					("usage update", &indexer.updates.usage.partitions, 1),
					(
						"usage",
						&indexer.usage_partitions,
						index.usage_partition_total,
					),
				],
			};
			for (name, partitions, total) in partitions {
				if partitions.end < partitions.start {
					return Err(tg::error!(
						"the indexer {name} partition end must not be less than the partition start"
					));
				}
				if partitions.end > total {
					return Err(tg::error!(
						"the indexer {name} partition range exceeds the partition total"
					));
				}
			}
			let partitions = &indexer.object_cache_partitions;
			if partitions.end < partitions.start {
				return Err(tg::error!(
					"the indexer object cache partition end must not be less than the partition start"
				));
			}
			if let Some(cache) = &config.object.cache
				&& partitions.end > cache.partition_total
			{
				return Err(tg::error!(
					"the indexer object cache partition range exceeds the partition total"
				));
			}

			if indexer.request.archive_concurrency == 0
				|| indexer.request.concurrency == 0
				|| indexer.request.enqueue_batch_capacity == 0
				|| indexer.request.enqueue_batch_size == 0
				|| indexer.request.index_concurrency == 0
				|| indexer.request.wait_concurrency == 0
			{
				return Err(tg::error!(
					"the indexer request concurrency must be greater than zero"
				));
			}
		}

		if config.scheduler.request_concurrency == 0 {
			return Err(tg::error!(
				"the scheduler request concurrency must be greater than zero"
			));
		}

		// Validate the database index queue configuration.
		let queue = config.database.index_queue();
		if queue.batch_size == 0 {
			return Err(tg::error!(
				"the database index queue batch size must be greater than zero"
			));
		}
		if queue.wakeup_interval.is_zero() {
			return Err(tg::error!(
				"the database index queue wakeup interval must be greater than zero"
			));
		}

		// Validate the queue configuration.
		if config.object.queue_checkpoint_interval.is_zero() {
			return Err(tg::error!(
				"the queue checkpoint interval must be greater than zero"
			));
		}

		// Validate the archive queue configuration.
		let queue = &config.object.archive_queue;
		if queue.concurrency == 0 {
			return Err(tg::error!(
				"the archive queue concurrency must be greater than zero"
			));
		}
		if queue.sequence_reservation_size == 0 {
			return Err(tg::error!(
				"the archive queue sequence reservation size must be greater than zero"
			));
		}

		// Validate the object cache configuration.
		if let Some(cache) = &config.object.cache {
			if cache.batch_size == 0 {
				return Err(tg::error!(
					"the object cache batch size must be greater than zero"
				));
			}
			validate_capacity_threshold(&cache.capacity, "object cache")?;
			if cache.partition_total == 0 {
				return Err(tg::error!(
					"the object cache partition total must be greater than zero"
				));
			}
			if cache.poll_interval.is_zero() {
				return Err(tg::error!(
					"the object cache poll interval must be greater than zero"
				));
			}
			match &config.cache {
				self::config::Cache::Fjall(_)
				| self::config::Cache::Lmdb(_)
				| self::config::Cache::Rocksdb(_) => {},
				self::config::Cache::Memory(_) => {
					return Err(tg::error!("the memory cache does not report capacity"));
				},
				self::config::Cache::Scylla(cache) => {
					let Some(capacity) = &cache.capacity else {
						return Err(tg::error!(
							"the Scylla cache capacity configuration is required for the object cache"
						));
					};
					match capacity {
						self::config::ScyllaCacheCapacity::Prometheus(capacity) => {
							if capacity.available_query.trim().is_empty() {
								return Err(tg::error!(
									"the Scylla cache capacity available query must not be empty"
								));
							}
							if capacity.total_query.trim().is_empty() {
								return Err(tg::error!(
									"the Scylla cache capacity total query must not be empty"
								));
							}
							if capacity.ttl.is_zero() {
								return Err(tg::error!(
									"the Scylla cache capacity TTL must be greater than zero"
								));
							}
						},
					}
				},
			}
		}

		// Validate the index queue configuration.
		let queue = &config.object.index_queue;
		if queue.concurrency == 0 {
			return Err(tg::error!(
				"the index queue concurrency must be greater than zero"
			));
		}
		if queue.batch_timeout.is_zero() {
			return Err(tg::error!(
				"the index queue batch timeout must be greater than zero"
			));
		}
		if queue.fragment_size == 0 {
			return Err(tg::error!(
				"the index queue fragment size must be greater than zero"
			));
		}
		if queue.sequence_reservation_size == 0 {
			return Err(tg::error!(
				"the index queue sequence reservation size must be greater than zero"
			));
		}

		// Validate the indexer configuration.
		if config.indexer.request.wait_concurrency == 0 {
			return Err(tg::error!(
				"the index wait concurrency must be greater than zero"
			));
		}
		if config.indexer.request.poll_interval.is_zero() {
			return Err(tg::error!(
				"the indexer request poll interval must be greater than zero"
			));
		}
		if config.indexer.batch.timeout.is_zero() {
			return Err(tg::error!(
				"the indexer batch timeout must be greater than zero"
			));
		}
		if config.indexer.cache.poll_interval.is_zero() {
			return Err(tg::error!(
				"the indexer cache poll interval must be greater than zero"
			));
		}
		if config.indexer.request.response_ttl.is_zero() {
			return Err(tg::error!(
				"the indexer response TTL must be greater than zero"
			));
		}
		if config.indexer.request.timeout.is_zero() {
			return Err(tg::error!(
				"the indexer request timeout must be greater than zero"
			));
		}

		// Validate the object put configuration.
		if config.object.put_timeout.is_zero() {
			return Err(tg::error!(
				"the object put timeout must be greater than zero"
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
		let container = config.runner.backend.resources(&config.runner.resources);
		config.runner.resources.filesystem_project_ids.validate()?;
		if container.max_open_files == Some(0) {
			return Err(tg::error!(
				"the maximum number of container sandbox open files must be greater than zero"
			));
		}
		if container.max_duration == Some(std::time::Duration::ZERO) {
			return Err(tg::error!(
				"the maximum container sandbox duration must be greater than zero"
			));
		}
		if container.max_pids == Some(0) {
			return Err(tg::error!(
				"the maximum number of container sandbox pids must be greater than zero"
			));
		}
		if container.max_filesystem_inodes == Some(0) {
			return Err(tg::error!(
				"the maximum number of container sandbox filesystem inodes must be greater than zero"
			));
		}
		if container.max_filesystem_size == Some(0) {
			return Err(tg::error!(
				"the maximum container sandbox filesystem size must be greater than zero"
			));
		}
		if config.advanced.preserve_temp_directories
			&& (container.max_filesystem_inodes.is_some()
				|| container.max_filesystem_size.is_some())
		{
			return Err(tg::error!(
				"container filesystem limits are incompatible with preserving temp directories"
			));
		}
		if let config::RunnerBackend::Builtin(backend) = &config.runner.backend {
			if backend.harden && (backend.uid_map.is_none() || backend.gid_map.is_none()) {
				return Err(tg::error!(
					"hardened builtin isolation requires subordinate identity maps"
				));
			}
			if backend.uid_map.is_some() != backend.gid_map.is_some() {
				return Err(tg::error!(
					"builtin uid and gid maps must be configured together"
				));
			}
			if let Some(map) = &backend.uid_map {
				// SAFETY: This function has no preconditions.
				map.validate(unsafe { libc::getuid() }, "uid")?;
			}
			if let Some(map) = &backend.gid_map {
				// SAFETY: This function has no preconditions.
				map.validate(unsafe { libc::getgid() }, "gid")?;
			}
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
			if scheduler
				.default_cpu
				.max
				.is_some_and(|max| max.millis() == 0)
			{
				return Err(tg::error!(
					"the default sandbox CPU must be greater than zero"
				));
			}
			if scheduler.default_memory.max == Some(0) {
				return Err(tg::error!(
					"the default sandbox memory must be greater than zero"
				));
			}
			scheduler
				.default_cpu
				.threads()
				.map_err(|error| tg::error!(!error, "invalid default sandbox CPU bounds"))?;
			for resource in [scheduler.default_memory, scheduler.default_storage] {
				resource.validate().map_err(|error| {
					tg::error!(!error, "invalid default sandbox resource bounds")
				})?;
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

		// Validate the sampling interval and overcommit factors.
		if config.runner.memory_sampling_interval.is_zero() {
			return Err(tg::error!(
				"the memory sampling interval must be greater than zero"
			));
		}
		if [
			config.runner.overcommit.cpu,
			config.runner.overcommit.memory,
			config.runner.overcommit.storage,
		]
		.contains(&0)
		{
			return Err(tg::error!(
				"the resource overcommit factors must be greater than zero"
			));
		}

		// Create the runner state.
		let capacity = if config.roles.contains(&self::config::Role::Runner) {
			let runner = &config.runner;
			let cpus = runner
				.cpus
				.unwrap_or_else(|| u64::try_from(parallelism).unwrap());
			#[cfg(target_os = "linux")]
			let (physical_memory, physical_storage) = tangram_sandbox::capacity::detect(&path)?;
			#[cfg(not(target_os = "linux"))]
			let (physical_memory, physical_storage) = (1_073_741_824_u64.saturating_mul(cpus), 0);
			let memory = runner.memory.unwrap_or(physical_memory * 3 / 4);
			let storage = runner.storage.unwrap_or(physical_storage * 3 / 4);
			if memory > physical_memory || storage > physical_storage {
				return Err(tg::error!(
					"the runner resource capacity exceeds its available backing"
				));
			}
			let memory = tg::sandbox::Resource::new(
				memory,
				memory
					.checked_mul(runner.overcommit.memory)
					.ok_or_else(|| tg::error!("the memory overcommit capacity overflowed"))?,
			);
			let storage = tg::sandbox::Resource::new(
				storage,
				storage
					.checked_mul(runner.overcommit.storage)
					.ok_or_else(|| tg::error!("the storage overcommit capacity overflowed"))?,
			);

			let max = cpus
				.checked_mul(runner.overcommit.cpu)
				.and_then(|value| value.checked_mul(1000))
				.ok_or_else(|| tg::error!("the CPU capacity is too large"))?;
			let max = tg::sandbox::Amount::from_millis(max);
			if cpus > u64::try_from(parallelism).unwrap() {
				return Err(tg::error!(
					"the runner CPU capacity exceeds its available hardware threads"
				));
			}
			let min = cpus
				.checked_mul(1000)
				.ok_or_else(|| tg::error!("the CPU capacity is too large"))?;
			let min = tg::sandbox::Amount::from_millis(min);
			let sandboxes = runner.sandboxes.unwrap_or(cpus);
			if sandboxes == 0 {
				return Err(tg::error!(
					"the runner sandbox capacity must be greater than zero"
				));
			}
			let capacity = tg::runner::Capacity {
				cpu: tg::sandbox::Cpu::new(min, max),
				memory,
				sandboxes,
				storage,
			};
			if capacity.cpu.max.is_none_or(|max| max.millis() == 0) {
				return Err(tg::error!(
					"the runner CPU capacity must be greater than zero"
				));
			}

			if capacity.memory.max.is_none_or(|max| max == 0) {
				return Err(tg::error!(
					"the runner memory capacity must be greater than zero"
				));
			}

			capacity
		} else {
			tg::runner::Capacity::default()
		};

		// Warm sandboxes only on runners.
		let sandbox_pool_size = if config.roles.contains(&self::config::Role::Runner) {
			config.runner.sandbox_pool_size
		} else {
			0
		};
		let runner_config = self::runner::Config {
			capacity,

			#[cfg(target_os = "linux")]
			filesystem_project_ids: config.runner.resources.filesystem_project_ids,

			overcommit: config.runner.overcommit,

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
					let options = tangram_database_postgres::DatabaseOptions {
						read: tangram_database_postgres::PoolOptions {
							max: options.read.pool.max.unwrap_or(parallelism),
							min: options.read.pool.min.unwrap_or(0),
							ttl: options.read.pool.ttl,
							url: options.read.url.clone(),
						},
						retry: options.retry.clone().into(),
						write: tangram_database_postgres::PoolOptions {
							max: options.write.pool.max.unwrap_or(parallelism),
							min: options.write.pool.min.unwrap_or(0),
							ttl: options.write.pool.ttl,
							url: options.write.url.clone(),
						},
					};
					let database = tangram_database_postgres::Database::new(options)
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
					let options = tangram_database_sqlite::DatabaseOptions {
						initialize,
						max: config.pool.max.unwrap_or(parallelism),
						min: config.pool.min.unwrap_or(0),
						path: path.join(&config.path),
						retry: config.retry.clone().into(),
						ttl: config.pool.ttl,
					};
					let database = tangram_database_sqlite::Database::new(options)
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
					let busy_timeout = config.busy_timeout;
					let initialize: tangram_database_turso::Initialize =
						Arc::new(move |connection| {
							Box::pin(self::database::turso::initialize(connection, busy_timeout))
						});
					let options = tangram_database_turso::DatabaseOptions {
						initialize,
						max: config.pool.max.unwrap_or(parallelism),
						min: config.pool.min.unwrap_or(0),
						path: path.join(&config.path),
						retry: config.retry.clone().into(),
						ttl: config.pool.ttl,
					};
					let database = tangram_database_turso::Database::new(options)
						.await
						.map_err(|error| tg::error!(!error, "failed to create the database"))?;
					Database::Turso(database)
				}
			},
		};

		// Create the diagnostics.
		let diagnostics = Mutex::new(Vec::new());

		// Create the clock.
		let clock = self::clock::Clock::new(config.advanced.clock.as_ref(), checkpoints.clone());

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
					let verification = tangram_index_fdb::VerificationConfig {
						concurrency: options.verification.concurrency,
					};
					let options = tangram_index_fdb::Options {
						cleaning_partition_total: options.cleaning_partition_total,
						cluster: options.cluster.clone(),
						instance: options.instance.clone(),
						max_process_depth: config
							.roles
							.contains(&self::config::Role::Indexer)
							.then(|| {
								u64::try_from(config.indexer.updates.max_process_depth).unwrap()
							}),
						max_write_operation_batch_size: options.max_write_operation_batch_size,
						object_permission_time_to_touch: config.object.permission_time_to_touch,
						permission_update_partition_total: options
							.permission_update_partition_total,
						process_data_fragment_size: options.process_data_fragment_size,
						process_permission_time_to_touch: config.process.permission_time_to_touch,
						read_channel_capacity: options.read_channel_capacity,
						read_request_batch_size: options.read_request_batch_size,
						read_task_concurrency: options.read_task_concurrency,
						read_task_count: options.read_task_count,
						storage_and_metadata_update_partition_total: options
							.storage_and_metadata_update_partition_total,
						storage_relation_batch_size: options.storage_relation_batch_size,
						usage_partition_total: options.usage_partition_total,
						usage_update_partition_total: options.usage_update_partition_total,
						verification,
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
					let path = directory.join(&options.path);
					let config = tangram_index_lmdb::Config {
						map_size: options.map_size,
						max_process_depth: config
							.roles
							.contains(&self::config::Role::Indexer)
							.then(|| {
								u64::try_from(config.indexer.updates.max_process_depth).unwrap()
							}),
						max_readers: options.max_readers,
						object_permission_time_to_touch: config.object.permission_time_to_touch,
						path,
						posix_sem_prefix: options.posix_sem_prefix.clone(),
						process_data_fragment_size: options.process_data_fragment_size,
						process_permission_time_to_touch: config.process.permission_time_to_touch,
						read_channel_capacity: options.read_channel_capacity,
						read_request_batch_size: options.read_request_batch_size,
						read_transaction_concurrency: options.read_transaction_concurrency,
						usage_partition_total: options.usage_partition_total,
						write_channel_capacity: options.write_channel_capacity,
						write_operation_batch_size: options.write_operation_batch_size,
					};
					Index::new_lmdb(&config)
						.map_err(|error| tg::error!(!error, "failed to create the index"))?
				}
			},
		};

		// Create the index tasks.
		let index_tasks = tangram_futures::task::Set::default();

		// Create the index change notification.
		let index_changed = tokio::sync::Notify::new();

		// Create the index wait channel.
		let (index_wait_sender, index_wait_receiver) =
			tokio::sync::mpsc::channel(config.indexer.request.wait_concurrency);

		// Create the library.
		let library = Mutex::new(None);

		// Create the authentication cache.
		let authentication_cache = Arc::new(self::authentication::cache::Cache::new(
			&config.authentication.cache,
		));
		let (authentication_cache_events, authentication_cache_events_receiver) =
			tokio::sync::broadcast::channel(16);

		// Create the messenger.
		let instance = config.instance.clone();
		let region = config.region.clone();
		let messenger = match &config.messenger {
			self::config::Messenger::Memory(memory) => {
				let config = tangram_messenger_memory::Config {
					subscription_capacity: memory.subscription_capacity,
				};
				Messenger::memory(&config, instance, region)
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
					let cache = authentication_cache.clone();
					let checkpoints = checkpoints.clone();
					let events = authentication_cache_events.clone();
					let mut options =
						async_nats::ConnectOptions::new().event_callback(move |event| {
							let cache = cache.clone();
							let checkpoints = checkpoints.clone();
							let events = events.clone();
							async move {
								// Name the event, logging errors.
								let event = match event {
									async_nats::Event::ClientError(error) => {
										tracing::error!(%error, "the messenger client failed; disabling authentication caching");
										"client_error"
									},
									async_nats::Event::Closed => "closed",
									async_nats::Event::Connected => "connected",
									async_nats::Event::Disconnected => "disconnected",
									async_nats::Event::Draining => "draining",
									async_nats::Event::LameDuckMode => return,
									async_nats::Event::ServerError(error) => {
										tracing::error!(%error, "the messenger server failed; disabling authentication caching");
										"server_error"
									},
									async_nats::Event::SlowConsumer(_) => "slow_consumer",
								};

								// Mark the authentication cache not ready and notify its subscriber.
								cache.set_ready(false);
								let _ = events.send(());

								// Hit the checkpoint for the authentication cache becoming not ready.
								if let Some(checkpoints) = &checkpoints {
									let mut params = tg::checkpoint::Params::new();
									params.insert("event".to_owned(), event.into());
									checkpoints
										.hit("authentication.cache.unready", params)
										.await;
								}
							}
						});
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
		let notifications = self::notification::Notifications::new(messenger.clone(), &config);

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

		// Prepare the runner's reusable sandbox assets.
		let tangram_path = tangram_util::env::current_exe()
			.map_err(|error| tg::error!(!error, "failed to get the tangram executable path"))?;
		let sandbox_assets = self::runner::assets::Assets::new(&path, &tangram_path, &config)?;

		// Create the cache.
		let cache = match &config.cache {
			config::Cache::Fjall(fjall) => {
				#[cfg(not(feature = "fjall"))]
				{
					let _ = fjall;
					return Err(tg::error!(
						"this version of tangram was not compiled with fjall support"
					));
				}
				#[cfg(feature = "fjall")]
				{
					self::cache::Cache::new_fjall(&path, fjall)
						.map_err(|error| tg::error!(!error, "failed to create the cache"))?
				}
			},
			config::Cache::Lmdb(lmdb) => {
				#[cfg(not(feature = "lmdb"))]
				{
					let _ = lmdb;
					return Err(tg::error!(
						"this version of tangram was not compiled with lmdb support"
					));
				}
				#[cfg(feature = "lmdb")]
				{
					self::cache::Cache::new_lmdb(&path, lmdb)
						.map_err(|error| tg::error!(!error, "failed to create the cache"))?
				}
			},
			config::Cache::Memory(_) => self::cache::Cache::new_memory(),
			config::Cache::Rocksdb(rocksdb) => {
				#[cfg(not(feature = "rocksdb"))]
				{
					let _ = rocksdb;
					return Err(tg::error!(
						"this version of tangram was not compiled with rocksdb support"
					));
				}
				#[cfg(feature = "rocksdb")]
				{
					self::cache::Cache::new_rocksdb(&path, rocksdb)
						.map_err(|error| tg::error!(!error, "failed to create the cache"))?
				}
			},
			config::Cache::Scylla(scylla) => {
				#[cfg(not(feature = "scylla"))]
				{
					let _ = scylla;
					return Err(tg::error!(
						"this version of tangram was not compiled with scylla support"
					));
				}
				#[cfg(feature = "scylla")]
				{
					self::cache::Cache::new_scylla(scylla)
						.await
						.map_err(|error| tg::error!(!error, "failed to create the cache"))?
				}
			},
		};

		// Create the temp paths.
		let temps = DashSet::default();

		// Create the indexer cache.
		let indexers = self::indexer::Cache::default();

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

		// Load or create the token keys.
		let authentication_tokens = load_token_keys(
			Some(&config.authentication.tokens.keys),
			&path.join("authentication.key"),
		)
		.await?;
		let authorization_tokens = load_token_keys(
			config.verification.tokens.as_ref(),
			&path.join("authorization.key"),
		)
		.await?;

		// Create the billing provider.
		let billing = config.billing.as_ref().map(self::billing::Billing::new);

		// Create the sync clients, which observe their events as checkpoints.
		let observer = Arc::new(self::sync::service::Observer::new(checkpoints.clone()));
		let sync_clients = tangram_messenger::service::Clients::new(
			messenger.clone(),
			"syncs.service",
			config.sync.service,
			observer,
		)
		.map_err(|source| tg::error!(!source, "failed to create the sync clients"))?;

		// Create the process connect clients, which observe their events as checkpoints.
		let observer = Arc::new(self::process::connect::service::Observer::new(
			checkpoints.clone(),
		));
		let process_connect_clients = tangram_messenger::service::Clients::new(
			messenger.clone(),
			"processes.service",
			config.process.connect.service,
			observer,
		)
		.map_err(|source| tg::error!(!source, "failed to create the process connect clients"))?;

		// Create the process control clients.
		let process_control_clients = tangram_messenger::service::Clients::new(
			messenger.clone(),
			"processes.control.service",
			config.process.control_service,
			Arc::new(self::process::control::service::Observer),
		)
		.map_err(|source| tg::error!(!source, "failed to create the process control clients"))?;

		// Create the sandbox control clients.
		let sandbox_control_clients = tangram_messenger::service::Clients::new(
			messenger.clone(),
			"sandboxes.control.service",
			config.sandbox.control_service,
			Arc::new(self::sandbox::control::service::Observer),
		)
		.map_err(|source| tg::error!(!source, "failed to create the sandbox control clients"))?;

		// Create the server.
		let server = Self(Arc::new(State {
			archive,

			archive_tasks,

			authentication_cache,

			authentication_tokens,

			authorization_tokens,

			billing,

			cache,

			checkin_tasks,

			checkout_graph_tasks,

			checkout_lock,

			checkout_tasks,

			checkout_xattr_value_size: Mutex::new(None),

			checkpoints,

			clock,

			config,

			context,

			database,

			diagnostics,

			index,

			index_changed,

			index_tasks,

			index_wait_sender,

			indexers,

			#[cfg(target_os = "linux")]
			ip_pool,

			library,

			lock,

			messenger,

			next_watch_id,

			notifications,

			object_get_tasks,

			path,

			process_connect_clients,

			process_connect_service_tasks: tangram_futures::task::Set::default(),

			process_connect_services: DashMap::default(),

			process_connect_services_changed: tokio::sync::Notify::new(),

			process_control_clients,

			process_control_service_tasks: tangram_futures::task::Set::default(),

			process_control_services: DashMap::default(),

			process_control_services_changed: tokio::sync::Notify::new(),

			regions,

			remote_clients,

			remote_list_tasks,

			remote_object_put_tasks,

			remote_process_put_tasks,

			runner,

			sandbox_assets,

			sandbox_control_clients,

			sandbox_control_service_tasks: tangram_futures::task::Set::default(),

			sandbox_control_services: DashMap::default(),

			sandbox_control_services_changed: tokio::sync::Notify::new(),

			sandbox_tasks,

			shutdown,

			sync_clients,

			sync_index_tasks: tangram_futures::task::Set::default(),

			sync_service_tasks: tangram_futures::task::Set::default(),

			tangram_path,

			temps,

			version,

			vfs,

			watches,
		}));

		// Migrate the directory before using its databases.
		server.migrate().await?;

		// Start usage tracking.
		if server.config.usage.enabled {
			server.index.start_usage(server.clock.now()).await?;
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
					Some(tg::Principal::Root),
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

		// Spawn the shared index wait task.
		let index_wait_task = Task::spawn({
			let server = server.clone();
			move |stopper| async move {
				server.index_wait_task(index_wait_receiver, stopper).await;
			}
		});

		// Spawn the indexer cache task.
		let indexer_cache_task = Task::spawn({
			let poll_interval = server.config.indexer.cache.poll_interval;
			let server = server.clone();
			move |stopper| async move {
				server.indexer_cache_task(poll_interval, stopper).await;
			}
		});

		// Spawn the indexer task.
		let indexer_task = server
			.config
			.roles
			.contains(&self::config::Role::Indexer)
			.then(|| {
				let config = server.config.indexer.clone();
				Task::spawn({
					let server = server.clone();
					|stopper| async move {
						let result = server.indexer_task(&config, stopper).await;
						if let Err(error) = result {
							tracing::error!(error = %error.trace(), "the indexer failed; exiting for crash recovery");
							std::process::exit(1);
						}
					}
				})
			});

		// Spawn the scheduler task.
		let (scheduler_started_sender, scheduler_started) = tokio::sync::oneshot::channel();
		let scheduler_task = server
			.config
			.roles
			.contains(&self::config::Role::Scheduler)
			.then(|| {
				let config = server.config.scheduler.clone();
				Task::spawn({
					let server = server.clone();
					|_| async move {
						server
							.scheduler_task(&config, scheduler_started_sender)
							.await;
					}
				})
			});

		// Subscribe to authentication invalidations before accepting requests.
		let (authentication_cache_started_sender, authentication_cache_started) =
			tokio::sync::oneshot::channel();
		let authentication_cache_task = Task::spawn({
			let server = server.clone();
			|_| async move {
				let _events = authentication_cache_events;
				server
					.authentication_cache_task(
						authentication_cache_events_receiver,
						authentication_cache_started_sender,
					)
					.await;
			}
		});
		let _ = authentication_cache_started.await;

		// Spawn the HTTP task.
		let http_listeners = if server.config.roles.contains(&self::config::Role::Api) {
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
				let scheduler_started = scheduler_task.is_some().then_some(scheduler_started);
				|stopper| async move {
					// Wait for this server's scheduler to subscribe, so that the runner's first scheduler message is not lost, unless the server stops first.
					if let Some(scheduler_started) = scheduler_started {
						tokio::select! {
							_ = scheduler_started => {},
							() = stopper.wait() => return,
						}
					}
					session.runner_task(id, stopper).boxed().await;
				}
			});
			server.runner.task().lock().unwrap().replace(task);
		}

		let shutdown = {
			let server = server.clone();

			async move {
				tracing::trace!("started");

				// Stop the HTTP and runner tasks while the indexer continues accepting their writes.
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

				// Stop the authentication cache subscriber.
				authentication_cache_task.abort();
				server.authentication_cache.set_ready(false);

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

				// Enqueue interrupted transfers before stopping their control tasks.
				server.sync_index_tasks.wait().await;

				// Stop the sync clients and drain the sync service tasks after the transfers have stopped.
				server.sync_clients.stop();
				server.sync_service_tasks.stop_all();
				server.sync_service_tasks.wait().await;

				// Stop the process connect clients and drain the process connect service tasks.
				server.process_connect_clients.stop();
				server.process_connect_service_tasks.stop_all();
				server.process_connect_service_tasks.wait().await;

				// Stop the process control clients and drain the process control service tasks.
				server.process_control_clients.stop();
				server.process_control_service_tasks.stop_all();
				server.process_control_service_tasks.wait().await;

				// Stop the sandbox control clients and drain the sandbox control service tasks.
				server.sandbox_control_clients.stop();
				server.sandbox_control_service_tasks.stop_all();
				server.sandbox_control_service_tasks.wait().await;

				// Finish the archive tasks after their producers have stopped.
				server.archive_tasks.wait().await;

				// Finish the index tasks after their producers have stopped.
				server.index_tasks.wait().await;

				// Stop the index wait task after its callers have stopped.
				index_wait_task.stop();
				let result = index_wait_task.wait().await;
				if let Err(error) = result
					&& !error.is_cancelled()
				{
					tracing::error!(?error, "the index wait task panicked");
				}

				// Stop the indexer task.
				if let Some(task) = indexer_task {
					task.stop();
					let result = task.wait().await;
					if let Err(error) = result
						&& !error.is_cancelled()
					{
						tracing::error!(?error, "the index task panicked");
					}
					tracing::trace!("indexer task");
				}

				// Stop the indexer cache task.
				indexer_cache_task.stop();
				let result = indexer_cache_task.wait().await;
				if let Err(error) = result
					&& !error.is_cancelled()
				{
					tracing::error!(?error, "the indexer cache task panicked");
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
					.destroy_runner_sandbox(&output.id, "server restarted")
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
			http: tg::Http {
				http2: self.config().http.http2,
				..Default::default()
			},
			stream_retry: Self::client_stream_retry(),
			sync: tg::sync::Config {
				max_frame_size: self.config().sync.max_frame_size,
			},
			url,
			version: Some(self.version.clone()),
			..Default::default()
		}
	}

	/// Get the stream retry of the clients that a server creates, which reconnect for as long as the server runs.
	#[must_use]
	pub(crate) fn client_stream_retry() -> tangram_futures::retry::Options {
		tangram_futures::retry::Options {
			max_retries: u64::MAX,
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
		self.sync_service_tasks.abort_all();
		self.process_connect_service_tasks.abort_all();
		self.process_control_service_tasks.abort_all();
		self.sandbox_control_service_tasks.abort_all();
		self.sync_index_tasks.abort_all();
		self.archive_tasks.abort_all();
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

fn verification_search_config(
	config: &self::config::PermissionSearches,
) -> tangram_index::verify::Config {
	let ancestor = tangram_index::verify::SearchConfig {
		max_depth: config.ancestor.max_depth,
		max_edges: config.ancestor.max_edges,
		max_nodes: config.ancestor.max_nodes,
		page_size: config.ancestor.page_size,
	};
	let descendant = tangram_index::verify::SearchConfig {
		max_depth: config.descendant.max_depth,
		max_edges: config.descendant.max_edges,
		max_nodes: config.descendant.max_nodes,
		page_size: config.descendant.page_size,
	};
	let subtree = tangram_index::verify::SubtreeConfig {
		max_depth: config.subtree.max_depth,
		max_objects: config.subtree.max_objects,
		max_processes: config.subtree.max_processes,
	};
	tangram_index::verify::Config {
		permissions: tangram_index::verify::PermissionsConfig {
			ancestor,
			descendant,
			subtree,
		},
	}
}

async fn load_token_keys(config: Option<&config::TokenKeys>, path: &Path) -> tg::Result<Tokens> {
	let private_key = match config.and_then(|config| config.private_key.as_ref()) {
		Some(config) => {
			let bytes = match &config.path {
				Some(path) => match config.algorithm {
					tg::authorization::Algorithm::Ed25519 => tokio::fs::read(path).await.map_err(
						|error| tg::error!(!error, path = %path.display(), "failed to read the private key"),
					)?,
				},
				None => load_or_create_token_private_key(config, path).await?,
			};
			let key =
				tg::authorization::PrivateKey::new(config.name.clone(), config.algorithm, bytes);
			tg::authorization::PublicKey::from_private_key(&key)?;

			Some(key)
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

async fn load_or_create_token_private_key(
	config: &config::TokenPrivateKey,
	path: &Path,
) -> tg::Result<Vec<u8>> {
	// Reuse the private key for the lifetime of the server directory.
	match tokio::fs::read(path).await {
		Ok(bytes) => return Ok(bytes),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
		Err(error) => {
			return Err(
				tg::error!(!error, path = %path.display(), "failed to read the private key"),
			);
		},
	}

	// Publish a complete key while the server holds the directory lock.
	let key = tg::authorization::PrivateKey::generate(config.name.clone(), config.algorithm)?;
	let directory = path.parent().unwrap();
	let temp_path = directory.join("tmp").join(uuid::Uuid::now_v7().to_string());
	let mut file = tokio::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.mode(0o600)
		.open(&temp_path)
		.await
		.map_err(|error| tg::error!(!error, "failed to create the temporary private key file"))?;
	scopeguard::defer! {
		std::fs::remove_file(&temp_path).ok();
	}
	file.write_all(&key.bytes)
		.await
		.map_err(|error| tg::error!(!error, "failed to write the private key"))?;
	file.sync_all()
		.await
		.map_err(|error| tg::error!(!error, "failed to sync the private key"))?;
	tokio::fs::rename(&temp_path, path).await.map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to persist the private key"),
	)?;
	tokio::fs::File::open(directory)
		.await
		.map_err(|error| tg::error!(!error, "failed to open the private key directory"))?
		.sync_all()
		.await
		.map_err(|error| tg::error!(!error, "failed to sync the private key directory"))?;

	Ok(key.bytes)
}

fn validate_capacity_threshold(
	config: &self::config::CapacityThreshold,
	name: &str,
) -> tg::Result<()> {
	match config {
		self::config::CapacityThreshold::Bytes(config) => {
			validate_capacity_threshold_direction(config, name)?;
		},
		self::config::CapacityThreshold::Ratio(config) => {
			let values = match config {
				self::config::CapacityThresholdDirection::Above(config) => {
					[config.start_above, config.stop_at]
				},
				self::config::CapacityThresholdDirection::Below(config) => {
					[config.start_below, config.stop_at]
				},
			};
			if values
				.into_iter()
				.any(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
			{
				return Err(tg::error!(
					"the {name} capacity ratios must be finite and between zero and one"
				));
			}
			validate_capacity_threshold_direction(config, name)?;
		},
	}

	Ok(())
}

fn validate_capacity_threshold_direction<T>(
	config: &self::config::CapacityThresholdDirection<T>,
	name: &str,
) -> tg::Result<()>
where
	T: PartialOrd,
{
	match config {
		self::config::CapacityThresholdDirection::Above(config)
			if config.stop_at >= config.start_above =>
		{
			return Err(tg::error!(
				"the {name} capacity stop threshold must be less than the start-above threshold"
			));
		},
		self::config::CapacityThresholdDirection::Below(config)
			if config.stop_at <= config.start_below =>
		{
			return Err(tg::error!(
				"the {name} capacity stop threshold must be greater than the start-below threshold"
			));
		},
		self::config::CapacityThresholdDirection::Above(_)
		| self::config::CapacityThresholdDirection::Below(_) => {},
	}

	Ok(())
}
