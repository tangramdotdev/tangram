use {
	crate::{
		Components, Config, Owned, Server, State,
		context::{Context, Origin},
		database::Database,
		index::Index,
		load_token_keys,
		messenger::Messenger,
	},
	dashmap::{DashMap, DashSet},
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	std::{
		os::fd::AsRawFd as _,
		path::{Path, PathBuf},
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_uri::Uri,
	tangram_util::fs::remove,
	tokio::io::AsyncWriteExt as _,
	tracing::Instrument as _,
};

struct Directory {
	lock: Mutex<Option<tokio::fs::File>>,
	original_path: PathBuf,
	path: PathBuf,
}

struct Tasks {
	diagnostics: Task<()>,
	http: Option<Task<()>>,
}

impl Server {
	pub async fn start(
		config: Config,
		components: impl FnOnce(&crate::component::Arg<'_>) -> tg::Result<Components> + Send,
	) -> tg::Result<Owned> {
		// Validate the configuration.
		config.validate()?;

		// Prepare the directory.
		let directory = Self::start_directory(&config).await?;

		// Create the server.
		let state = Self::start_state(config, directory, components)
			.boxed()
			.await?;
		let server = Self(Arc::new(state));

		// Initialize the server.
		server.start_initialize().await?;

		// Spawn the tasks.
		let tasks = server.start_tasks().await?;

		// Spawn the shutdown task.
		let task = server.spawn_shutdown_task(tasks);

		let owned = Owned { server, task };

		Ok(owned)
	}

	async fn start_directory(config: &Config) -> tg::Result<Directory> {
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
		if config.roles.contains(&crate::config::Role::Runner) {
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

		// Remove an existing socket file.
		let socket_path = path.join("socket");
		tokio::fs::remove_file(&socket_path).await.ok();

		let directory = Directory {
			lock,
			original_path: directory,
			path,
		};

		Ok(directory)
	}

	async fn start_state(
		config: Config,
		directory: Directory,
		components: impl FnOnce(&crate::component::Arg<'_>) -> tg::Result<Components> + Send,
	) -> tg::Result<State> {
		let Directory {
			lock,
			original_path,
			path,
		} = directory;

		// Get the available parallelism.
		let parallelism =
			std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);

		// Create the checkpoints.
		let checkpoints = config
			.advanced
			.checkpoints
			.then(|| Arc::new(crate::checkpoint::State::new()));

		// Create the context.
		let context = Context::root();

		// Create the archive.
		let archive = config
			.archive
			.as_ref()
			.map(|config| match config {
				crate::config::Archive::S3(config) => {
					tangram_server_archive::Archive::new_s3(config)
				},
			})
			.transpose()?;

		// Create the database.
		let database = Self::create_database(&config, &path, parallelism).await?;

		// Create the diagnostics.
		let diagnostics = Mutex::new(Vec::new());

		// Create the clock.
		let clock = crate::clock::Clock::new(config.advanced.clock.as_ref(), checkpoints.clone());

		// Create the index.
		let index = Self::create_index(&config, &original_path)?;

		// Create the library.
		let library = Mutex::new(None);

		// Create the messenger.
		let messenger = Self::create_messenger(&config).await?;
		let notifications = crate::notification::Notifications::new(messenger.clone());

		// Create the regions.
		let regions = DashMap::default();

		// Get the tangram executable path.
		let tangram_path = tangram_util::env::current_exe()
			.map_err(|error| tg::error!(!error, "failed to get the tangram executable path"))?;

		// Create the cache.
		let cache = Self::create_cache(&config, &path).await?;

		// Create the temp paths.
		let temps = DashSet::default();

		// Create the shutdown channel.
		let (shutdown, _) = tokio::sync::watch::channel(None);

		// Get the version.
		let version = config
			.version
			.clone()
			.unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());

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

		// Create the components.
		let arg = crate::component::Arg {
			checkpoints: checkpoints.as_ref(),
			config: &config,
			messenger: &messenger,
			path: &path,
			tangram_path: &tangram_path,
		};
		let components = components(&arg)
			.map_err(|error| tg::error!(!error, "failed to create the components"))?;

		// Create the state.
		let state = State {
			archive,
			authentication_tokens,
			authorization_tokens,
			cache,
			checkpoints,
			clock,
			components,
			config,
			context,
			database,
			diagnostics,
			index,
			library,
			lock,
			messenger,
			notifications,
			path,
			regions,
			shutdown,
			tangram_path,
			temps,
			version,
		};

		Ok(state)
	}

	async fn create_database(
		config: &Config,
		path: &Path,
		parallelism: usize,
	) -> tg::Result<Database> {
		let database = match &config.database {
			crate::config::Database::Postgres(options) => {
				#[cfg(not(feature = "postgres"))]
				{
					let _ = (options, parallelism);
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
			crate::config::Database::Sqlite(config) => {
				#[cfg(not(feature = "sqlite"))]
				{
					let _ = (config, path, parallelism);
					return Err(tg::error!(
						"this version of tangram was not compiled with sqlite support"
					));
				}
				#[cfg(feature = "sqlite")]
				{
					let initialize = Arc::new(crate::database::sqlite::initialize);
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
			crate::config::Database::Turso(config) => {
				#[cfg(not(feature = "turso"))]
				{
					let _ = (config, path, parallelism);
					return Err(tg::error!(
						"this version of tangram was not compiled with turso support"
					));
				}
				#[cfg(feature = "turso")]
				{
					let busy_timeout = config.busy_timeout;
					let initialize: tangram_database_turso::Initialize =
						Arc::new(move |connection| {
							Box::pin(crate::database::turso::initialize(connection, busy_timeout))
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

		Ok(database)
	}

	fn create_index(config: &Config, directory: &Path) -> tg::Result<Index> {
		let index = match &config.index {
			crate::config::Index::Fdb(options) => {
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
							.contains(&crate::config::Role::Indexer)
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
			crate::config::Index::Lmdb(options) => {
				#[cfg(not(feature = "lmdb"))]
				{
					let _ = (options, directory);
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
							.contains(&crate::config::Role::Indexer)
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

		Ok(index)
	}

	async fn create_messenger(config: &Config) -> tg::Result<Messenger> {
		let instance = config.instance.clone();
		let region = config.region.clone();
		let messenger = match &config.messenger {
			crate::config::Messenger::Memory(memory) => {
				let config = tangram_messenger_memory::Config {
					subscription_capacity: memory.subscription_capacity,
				};
				Messenger::memory(&config, instance, region)
			},
			crate::config::Messenger::Nats(nats) => {
				#[cfg(not(feature = "nats"))]
				{
					let _ = nats;
					return Err(tg::error!(
						"this version of tangram was not compiled with nats support"
					));
				}
				#[cfg(feature = "nats")]
				{
					let listeners = crate::messenger::Listeners::default();
					let mut options = async_nats::ConnectOptions::new().event_callback({
						let listeners = listeners.clone();
						move |event| {
							let listeners = listeners.clone();
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

								// Notify the listeners.
								listeners.notify(event).await;
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
					Messenger::nats(client, listeners, instance, region)
				}
			},
		};

		Ok(messenger)
	}

	async fn create_cache(config: &Config, path: &Path) -> tg::Result<crate::cache::Cache> {
		let cache = match &config.cache {
			crate::config::Cache::Fjall(fjall) => {
				#[cfg(not(feature = "fjall"))]
				{
					let _ = (fjall, path);
					return Err(tg::error!(
						"this version of tangram was not compiled with fjall support"
					));
				}
				#[cfg(feature = "fjall")]
				{
					crate::cache::Cache::new_fjall(path, fjall)
						.map_err(|error| tg::error!(!error, "failed to create the cache"))?
				}
			},
			crate::config::Cache::Lmdb(lmdb) => {
				#[cfg(not(feature = "lmdb"))]
				{
					let _ = (lmdb, path);
					return Err(tg::error!(
						"this version of tangram was not compiled with lmdb support"
					));
				}
				#[cfg(feature = "lmdb")]
				{
					crate::cache::Cache::new_lmdb(path, lmdb)
						.map_err(|error| tg::error!(!error, "failed to create the cache"))?
				}
			},
			crate::config::Cache::Memory(_) => crate::cache::Cache::new_memory(),
			crate::config::Cache::Rocksdb(rocksdb) => {
				#[cfg(not(feature = "rocksdb"))]
				{
					let _ = (rocksdb, path);
					return Err(tg::error!(
						"this version of tangram was not compiled with rocksdb support"
					));
				}
				#[cfg(feature = "rocksdb")]
				{
					crate::cache::Cache::new_rocksdb(path, rocksdb)
						.map_err(|error| tg::error!(!error, "failed to create the cache"))?
				}
			},
			crate::config::Cache::Scylla(scylla) => {
				#[cfg(not(feature = "scylla"))]
				{
					let _ = scylla;
					return Err(tg::error!(
						"this version of tangram was not compiled with scylla support"
					));
				}
				#[cfg(feature = "scylla")]
				{
					crate::cache::Cache::new_scylla(scylla)
						.await
						.map_err(|error| tg::error!(!error, "failed to create the cache"))?
				}
			},
		};

		Ok(cache)
	}

	async fn start_initialize(&self) -> tg::Result<()> {
		// Migrate the directory before using its databases.
		self.migrate().await?;

		// Start the usage component.
		self.components
			.usage
			.start(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the usage component"))?;

		// Start the sandbox component.
		self.components
			.sandbox
			.start(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the sandbox component"))?;

		// Start the remote component.
		self.components
			.remote
			.start(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the remote component"))?;

		// Start the VFS component.
		self.components
			.vfs
			.start(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the VFS"))?;

		// Start the checkout component.
		self.components
			.checkout
			.start(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the checkout component"))?;

		Ok(())
	}

	async fn start_tasks(&self) -> tg::Result<Tasks> {
		// Start the indexer component.
		self.components
			.indexer
			.start(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the indexer"))?;

		// Start the scheduler component.
		self.components
			.scheduler
			.start(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the scheduler"))?;

		// Start the authentication component.
		self.components
			.authentication
			.start(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the authentication component"))?;

		// Spawn the HTTP task.
		let http = self.spawn_http_task().await?;

		// Spawn the diagnostics task.
		let diagnostics = self.spawn_diagnostics_task();

		// Start the runner component.
		self.components
			.runner
			.start(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the runner"))?;

		let tasks = Tasks { diagnostics, http };

		Ok(tasks)
	}

	async fn spawn_http_task(&self) -> tg::Result<Option<Task<()>>> {
		// Get the listeners.
		let http_listeners = if self.config.roles.contains(&crate::config::Role::Api) {
			let config = &self.config.http;
			let mut listeners = if config.listeners.is_empty() {
				let path = self.path.join("socket");
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
			let group_socket = self
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
		// Spawn the task.
		let http_task = if http_listeners.is_empty() {
			None
		} else {
			let http_server = self.clone();
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

		Ok(http_task)
	}

	fn spawn_diagnostics_task(&self) -> Task<()> {
		let server = self.clone();
		Task::spawn(|_| async move {
			let result = server.diagnostics_task().await;
			if let Err(error) = result {
				tracing::error!(error = %error.trace());
			}
		})
	}

	fn spawn_shutdown_task(&self, tasks: Tasks) -> tangram_futures::task::Shared<()> {
		let server = self.clone();
		tangram_futures::task::Shared::spawn(|stop| async move {
			stop.wait().await;
			server
				.shutdown_task(tasks)
				.instrument(tracing::debug_span!("shutdown"))
				.await;
		})
	}

	async fn shutdown_task(&self, tasks: Tasks) {
		let Tasks {
			diagnostics: diagnostics_task,
			http: http_task,
		} = tasks;

		tracing::trace!("started");

		// Stop the HTTP and runner tasks while the indexer continues accepting their writes.
		if let Some(task) = &http_task {
			task.stop();
		}
		self.components.runner.stop_runner_task();

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

		// Stop the runner component.
		self.components.runner.stop(self).await;

		// Stop the authentication component.
		self.components.authentication.stop(self).await;

		// Stop the VFS component.
		self.components.vfs.stop(self).await;

		// Abort the diagnostics task.
		diagnostics_task.abort();

		// Stop the watch component.
		self.components.watch.stop(self).await;

		// Stop the checkin component.
		self.components.checkin.stop(self).await;

		// Stop the checkout component.
		self.components.checkout.stop(self).await;

		// Stop the object component.
		self.components.object.stop(self).await;

		// Stop the list component.
		self.components.list.stop(self).await;

		// Stop the sync component.
		self.components.sync.stop(self).await;

		// Stop the process component.
		self.components.process.stop(self).await;

		// Stop the sandbox component.
		self.components.sandbox.stop(self).await;

		// Finish the archive tasks after their producers have stopped.
		self.components.object.wait_archive_tasks().await;

		// Finish the index tasks after their producers have stopped.
		self.components.indexer.wait_index_tasks().await;

		// Stop the indexer component.
		self.components.indexer.stop(self).await;

		// Stop the scheduler component.
		self.components.scheduler.stop(self).await;

		// Remove the temp paths.
		self.temps
			.iter()
			.map(|entry| remove(entry.key().clone()).map(|_| ()))
			.collect::<FuturesUnordered<_>>()
			.collect::<()>()
			.await;
		tracing::trace!("temps");

		// Unlock.
		let lock = self.lock.lock().unwrap().take();
		if let Some(lock) = lock {
			lock.set_len(0).await.ok();
			tracing::trace!("released lock file");
		}

		tracing::trace!("finished");
	}
}
