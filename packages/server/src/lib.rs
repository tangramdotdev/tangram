use self::{database::Database, messenger::Messenger, runtime::Runtime};
use async_nats as nats;
use dashmap::DashMap;
use futures::{future, Future, FutureExt as _, Stream};
use http_body_util::BodyExt as _;
use hyper_util::rt::{TokioExecutor, TokioIo};
use itertools::Itertools as _;
use rusqlite as sqlite;
use std::{
	collections::HashMap,
	convert::Infallible,
	os::fd::AsRawFd as _,
	path::{Path, PathBuf},
	pin::pin,
	sync::{Arc, Mutex, RwLock},
};
use tangram_client as tg;
use tangram_database as db;
use tangram_either::Either;
use tangram_futures::task::{Stop, Task, TaskMap};
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};
use tokio::{
	io::{AsyncBufRead, AsyncRead, AsyncWrite, AsyncWriteExt as _},
	net::{TcpListener, UnixListener},
};
use url::Url;

mod artifact;
mod blob;
mod build;
mod clean;
mod compiler;
mod database;
mod health;
mod lockfile;
mod messenger;
mod module;
mod object;
mod package;
mod progress;
mod reference;
mod remote;
mod runtime;
mod tag;
mod target;
mod temp;
mod user;
mod util;
mod vfs;

pub use self::config::Config;

pub mod config;

/// A server.
#[derive(Clone)]
pub struct Server(Arc<Inner>);

pub struct Inner {
	blob_store_task_map: BlobStoreTaskMap,
	build_permits: BuildPermits,
	build_semaphore: Arc<tokio::sync::Semaphore>,
	builds: BuildTaskMap,
	checkout_task_map: CheckoutTaskMap,
	config: Config,
	database: Database,
	file_descriptor_semaphore: tokio::sync::Semaphore,
	local_pool_handle: tokio_util::task::LocalPoolHandle,
	lock_file: Mutex<Option<tokio::fs::File>>,
	messenger: Messenger,
	path: PathBuf,
	remotes: DashMap<String, tg::Client>,
	runtimes: RwLock<HashMap<String, Runtime>>,
	task: Mutex<Option<Task<()>>>,
	vfs: Mutex<Option<self::vfs::Server>>,
}

type BlobStoreTaskMap = TaskMap<tg::blob::Id, tg::Result<bool>, fnv::FnvBuildHasher>;

type BuildPermits =
	DashMap<tg::build::Id, Arc<tokio::sync::Mutex<Option<BuildPermit>>>, fnv::FnvBuildHasher>;

struct BuildPermit(
	#[allow(dead_code)]
	Either<tokio::sync::OwnedSemaphorePermit, tokio::sync::OwnedMutexGuard<Option<Self>>>,
);

type BuildTaskMap = TaskMap<tg::build::Id, (), fnv::FnvBuildHasher>;

type CheckoutTaskMap =
	TaskMap<tg::artifact::Id, tg::Result<tg::artifact::checkout::Output>, fnv::FnvBuildHasher>;

impl Server {
	pub async fn start(options: Config) -> tg::Result<Server> {
		// Ensure the path exists.
		let path = options.path.clone();
		tokio::fs::create_dir_all(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;

		// Lock the lock file.
		let lock_path = path.join("lock");
		let mut lock_file = tokio::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(true)
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
			.write_all(pid.to_string().as_bytes())
			.await
			.map_err(|source| tg::error!(!source, "failed to write the pid to the lock file"))?;
		let lock_file = Mutex::new(Some(lock_file));

		// Ensure the blobs directory exists.
		let blobs_path = path.join("blobs");
		tokio::fs::create_dir_all(&blobs_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the blobs directory"))?;

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

		// Remove an existing socket file.
		let socket_path = path.join("socket");
		tokio::fs::remove_file(&socket_path).await.ok();

		// Create the blob store task map.
		let blob_store_task_map = TaskMap::default();

		// Create the build permits.
		let build_permits = DashMap::default();

		// Create the build semaphore.
		let permits = options
			.build
			.as_ref()
			.map(|build| build.concurrency)
			.unwrap_or_default();
		let build_semaphore = Arc::new(tokio::sync::Semaphore::new(permits));

		// Create the build tasks.
		let builds = TaskMap::default();

		// Create the checkout tasks.
		let checkouts = TaskMap::default();

		// Create the database.
		let database = match &options.database {
			self::config::Database::Sqlite(options) => {
				let initialize = Arc::new(|connection: &sqlite::Connection| {
					connection.pragma_update(None, "auto_vaccum", "incremental")?;
					connection.pragma_update(None, "busy_timeout", "5000")?;
					connection.pragma_update(None, "cache_size", "-20000")?;
					connection.pragma_update(None, "foreign_keys", "on")?;
					connection.pragma_update(None, "journal_mode", "wal")?;
					connection.pragma_update(None, "mmap_size", "2147483648")?;
					connection.pragma_update(None, "synchronous", "normal")?;
					connection.pragma_update(None, "temp_store", "memory")?;
					Ok(())
				});
				let options = db::sqlite::DatabaseOptions {
					connections: options.connections,
					initialize,
					path: path.join("database"),
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

		// Create the file system semaphore.
		let file_descriptor_semaphore =
			tokio::sync::Semaphore::new(options.advanced.file_descriptor_semaphore_size);

		// Create the local pool handle.
		let local_pool_handle = tokio_util::task::LocalPoolHandle::new(
			std::thread::available_parallelism().unwrap().get(),
		);

		// Create the messenger.
		let messenger = match &options.messenger {
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

		// Get the remotes.
		let remotes = options
			.remotes
			.iter()
			.map(|(name, remote)| (name.clone(), remote.client.clone()))
			.collect();

		// Create the runtimes.
		let runtimes = RwLock::new(HashMap::default());

		// Create the task.
		let task = Mutex::new(None);

		// Create the vfs.
		let vfs = Mutex::new(None);

		// Create the server.
		let server = Self(Arc::new(Inner {
			blob_store_task_map,
			build_permits,
			build_semaphore,
			builds,
			checkout_task_map: checkouts,
			database,
			file_descriptor_semaphore,
			local_pool_handle,
			lock_file,
			messenger,
			config: options,
			path,
			remotes,
			runtimes,
			task,
			vfs,
		}));

		// Migrate the database.
		self::database::migrate(&server.database)
			.await
			.map_err(|source| tg::error!(!source, "failed to migrate the database"))?;

		// Spawn tasks to connect the remotes.
		for remote in server.remotes.iter().map(|entry| entry.value().clone()) {
			tokio::spawn(async move { remote.connect().await.ok() });
		}

		// Start the VFS if enabled.
		let artifacts_path = server.path.join("artifacts");
		let cache_path = server.path.join("cache");
		let artifacts_exists = tokio::fs::try_exists(&artifacts_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to stat the path"))?;
		let checkouts_exists = tokio::fs::try_exists(&cache_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to stat the path"))?;
		if let Some(options) = server.config.vfs {
			if artifacts_exists && !checkouts_exists {
				tokio::fs::rename(&artifacts_path, &cache_path)
					.await
					.map_err(|source| {
						tg::error!(
							!source,
							"failed to move the artifacts directory to the checkouts path"
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
				.map_err(|source| {
					tg::error!(!source, "failed to create the checkouts directory")
				})?;
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
			if checkouts_exists {
				tokio::fs::rename(&cache_path, &artifacts_path)
					.await
					.map_err(|source| {
						tg::error!(
							!source,
							"failed to move the artifacts directory to the checkouts directory"
						)
					})?;
			}
			tokio::fs::create_dir_all(&artifacts_path)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to create the artifacts directory")
				})?;
		}

		// Add the runtimes.
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
			let runtime = self::runtime::linux::Runtime::new(&server).await?;
			let runtime = self::runtime::Runtime::Linux(runtime);
			server.runtimes.write().unwrap().insert(triple, runtime);
		}

		// Spawn the build heartbeat monitor task.
		let build_heartbeat_monitor_task =
			server
				.config
				.build_heartbeat_monitor
				.as_ref()
				.map(|options| {
					tokio::spawn({
						let server = server.clone();
						let options = options.clone();
						async move {
							server.build_heartbeat_monitor_task(&options).await;
						}
					})
				});

		// Spawn the build indexer task.
		let build_indexer_task = if server.config.build_indexer.is_some() {
			Some(tokio::spawn({
				let server = server.clone();
				async move {
					server
						.build_indexer_task()
						.await
						.inspect_err(|error| {
							tracing::error!(?error);
						})
						.ok();
				}
			}))
		} else {
			None
		};

		// Spawn the object indexer task.
		let object_indexer_task = if server.config.object_indexer.is_some() {
			Some(tokio::spawn({
				let server = server.clone();
				async move {
					server
						.object_indexer_task()
						.await
						.inspect_err(|error| {
							tracing::error!(?error);
						})
						.ok();
				}
			}))
		} else {
			None
		};

		// Spawn the build spawn task.
		let build_spawn_task = if server.config.build.is_some() {
			Some(tokio::spawn({
				let server = server.clone();
				async move {
					server.build_spawn_task().await;
				}
			}))
		} else {
			None
		};

		// Listen.
		let listener = Self::listen(&server.config.url).await?;
		tracing::trace!("listening on {}", server.config.url);

		// Spawn the HTTP task.
		let http_task = Some(Task::spawn(|stop| {
			let server = server.clone();
			async move {
				Self::serve(server.clone(), listener, stop).await;
			}
		}));

		let shutdown = {
			let server = server.clone();
			async move {
				// Stop the HTTP task.
				if let Some(task) = http_task {
					task.stop();
					task.wait().await.unwrap();
				}

				// Abort the build spawn task.
				if let Some(task) = build_spawn_task {
					task.abort();
					task.await.ok();
				}

				// Abort the object index task.
				if let Some(task) = object_indexer_task {
					task.abort();
					task.await.ok();
				}

				// Abort the build index task.
				if let Some(task) = build_indexer_task {
					task.abort();
					task.await.ok();
				}

				// Abort the build heartbeat monitor task.
				if let Some(task) = build_heartbeat_monitor_task {
					task.abort();
					task.await.ok();
				}

				// Abort the build tasks.
				server.builds.abort_all();
				server.builds.wait().await;

				// Remove the runtimes.
				server.runtimes.write().unwrap().clear();

				// Abort the checkout tasks.
				server.checkout_task_map.abort_all();
				server.checkout_task_map.wait().await;

				// Stop the VFS.
				let vfs = server.vfs.lock().unwrap().take();
				if let Some(vfs) = vfs {
					vfs.stop();
					vfs.wait().await;
				}

				// Release the lock file.
				let lock_file = server.lock_file.lock().unwrap().take();
				if let Some(lock_file) = lock_file {
					lock_file.set_len(0).await.ok();
				}
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
	pub fn artifacts_path(&self) -> PathBuf {
		self.path.join("artifacts")
	}

	#[must_use]
	pub fn blobs_path(&self) -> PathBuf {
		self.path.join("blobs")
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
				let listener = UnixListener::bind(path)
					.map_err(|source| tg::error!(!source, "failed to bind"))?;
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
			let service = hyper::service::service_fn({
				let handle = handle.clone();
				let stop = stop.clone();
				move |mut request| {
					let handle = handle.clone();
					let stop = stop.clone();
					async move {
						request.extensions_mut().insert(stop);
						let response = Self::handle_request(&handle, request).await;
						Ok::<_, Infallible>(response)
					}
				}
			});

			// Spawn a task to serve the connection.
			task_tracker.spawn({
				let stop = stop.clone();
				async move {
					let builder =
						hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
					let connection = builder.serve_connection_with_upgrades(stream, service);
					let result = match future::select(pin!(connection), pin!(stop.wait())).await {
						future::Either::Left((result, _)) => result,
						future::Either::Right(((), mut connection)) => {
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

	async fn handle_request<H>(
		handle: &H,
		mut request: http::Request<Incoming>,
	) -> http::Response<Outgoing>
	where
		H: tg::Handle,
	{
		let id = tg::Id::new_uuidv7(tg::id::Kind::Request);
		request.extensions_mut().insert(id.clone());

		tracing::trace!(?id, method = ?request.method(), path = ?request.uri().path(), "received request");

		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect_vec();
		let response = match (method, path_components.as_slice()) {
			// Artifacts.
			(http::Method::POST, ["artifacts", "checkin"]) => {
				Self::handle_check_in_artifact_request(handle, request).boxed()
			},
			(http::Method::POST, ["artifacts", artifact, "checkout"]) => {
				Self::handle_check_out_artifact_request(handle, request, artifact).boxed()
			},

			// Blobs.
			(http::Method::POST, ["blobs"]) => {
				Self::handle_create_blob_request(handle, request).boxed()
			},
			(http::Method::GET, ["blobs", blob, "read"]) => {
				Self::handle_read_blob_request(handle, request, blob).boxed()
			},

			// Builds.
			(http::Method::GET, ["builds", build]) => {
				Self::handle_get_build_request(handle, request, build).boxed()
			},
			(http::Method::PUT, ["builds", build]) => {
				Self::handle_put_build_request(handle, request, build).boxed()
			},
			(http::Method::POST, ["builds", build, "push"]) => {
				Self::handle_push_build_request(handle, request, build).boxed()
			},
			(http::Method::POST, ["builds", build, "pull"]) => {
				Self::handle_pull_build_request(handle, request, build).boxed()
			},
			(http::Method::POST, ["builds", "dequeue"]) => {
				Self::handle_dequeue_build_request(handle, request).boxed()
			},
			(http::Method::POST, ["builds", build, "start"]) => {
				Self::handle_start_build_request(handle, request, build).boxed()
			},
			(http::Method::GET, ["builds", build, "status"]) => {
				Self::handle_get_build_status_request(handle, request, build).boxed()
			},
			(http::Method::GET, ["builds", build, "children"]) => {
				Self::handle_get_build_children_request(handle, request, build).boxed()
			},
			(http::Method::GET, ["builds", build, "log"]) => {
				Self::handle_get_build_log_request(handle, request, build).boxed()
			},
			(http::Method::POST, ["builds", build, "log"]) => {
				Self::handle_add_build_log_request(handle, request, build).boxed()
			},
			(http::Method::GET, ["builds", build, "outcome"]) => {
				Self::handle_get_build_outcome_request(handle, request, build).boxed()
			},
			(http::Method::POST, ["builds", build, "finish"]) => {
				Self::handle_finish_build_request(handle, request, build).boxed()
			},
			(http::Method::POST, ["builds", build, "touch"]) => {
				Self::handle_touch_build_request(handle, request, build).boxed()
			},
			(http::Method::POST, ["builds", build, "heartbeat"]) => {
				Self::handle_heartbeat_build_request(handle, request, build).boxed()
			},

			// Compiler.
			(http::Method::POST, ["lsp"]) => Self::handle_lsp_request(handle, request).boxed(),

			// Objects.
			(http::Method::HEAD, ["objects", object]) => {
				Self::handle_head_object_request(handle, request, object).boxed()
			},
			(http::Method::GET, ["objects", object]) => {
				Self::handle_get_object_request(handle, request, object).boxed()
			},
			(http::Method::PUT, ["objects", object]) => {
				Self::handle_put_object_request(handle, request, object).boxed()
			},
			(http::Method::POST, ["objects", object, "push"]) => {
				Self::handle_push_object_request(handle, request, object).boxed()
			},
			(http::Method::POST, ["objects", object, "pull"]) => {
				Self::handle_pull_object_request(handle, request, object).boxed()
			},

			// Packages.
			(http::Method::POST, ["packages", "check"]) => {
				Self::handle_check_package_request(handle, request).boxed()
			},
			(http::Method::POST, ["packages", "document"]) => {
				Self::handle_document_package_request(handle, request).boxed()
			},
			(http::Method::POST, ["packages", "format"]) => {
				Self::handle_format_package_request(handle, request).boxed()
			},

			// References.
			(http::Method::GET, ["references", path @ ..]) => {
				Self::handle_get_reference_request(handle, request, path).boxed()
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

			// Runtimes.
			(http::Method::GET, ["runtimes", "js", "doc"]) => {
				Self::handle_get_js_runtime_doc_request(handle, request).boxed()
			},

			// Server.
			(http::Method::POST, ["clean"]) => {
				Self::handle_server_clean_request(handle, request).boxed()
			},
			(http::Method::GET, ["health"]) => {
				Self::handle_server_health_request(handle, request).boxed()
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

			// Targets.
			(http::Method::POST, ["targets", target, "build"]) => {
				Self::handle_build_target_request(handle, request, target).boxed()
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
			http::Response::builder()
				.status(http::StatusCode::INTERNAL_SERVER_ERROR)
				.json(error)
				.unwrap()
		});

		// Add the request ID to the response.
		let key = http::HeaderName::from_static("x-tangram-request-id");
		let value = http::HeaderValue::from_str(&id.to_string()).unwrap();
		response.headers_mut().insert(key, value);

		// Add tracing for response body errors.
		let response = response.map(|body| {
			Outgoing::body(body.map_err(|error| {
				tracing::error!(?error, "response body error");
				error
			}))
		});

		tracing::trace!(?id, status = ?response.status(), "sending response");

		response
	}
}

impl tg::Handle for Server {
	fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkin::Output>>>
				+ Send
				+ 'static,
		>,
	> {
		self.check_in_artifact(arg)
	}

	fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkout::Output>>>
				+ Send
				+ 'static,
		>,
	> {
		self.check_out_artifact(id, arg)
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

	fn try_get_build(
		&self,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<Option<tg::build::get::Output>>> {
		self.try_get_build(id)
	}

	fn put_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::put::Arg,
	) -> impl Future<Output = tg::Result<tg::build::put::Output>> {
		self.put_build(id, arg)
	}

	fn push_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::push::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		self.push_build(id, arg)
	}

	fn pull_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::pull::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		self.pull_build(id, arg)
	}

	fn try_dequeue_build(
		&self,
		arg: tg::build::dequeue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::build::dequeue::Output>>> {
		self.try_dequeue_build(arg)
	}

	fn try_start_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::start::Arg,
	) -> impl Future<Output = tg::Result<bool>> {
		self.try_start_build(id, arg)
	}

	fn try_get_build_status_stream(
		&self,
		id: &tg::build::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::status::Event>> + Send + 'static>,
		>,
	> {
		self.try_get_build_status_stream(id)
	}

	fn try_get_build_children_stream(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::build::children::get::Event>> + Send + 'static,
			>,
		>,
	> {
		self.try_get_build_children_stream(id, arg)
	}

	fn try_get_build_log_stream(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::log::get::Event>> + Send + 'static>,
		>,
	> {
		self.try_get_build_log_stream(id, arg)
	}

	fn add_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::post::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.add_build_log(id, arg)
	}

	fn try_get_build_outcome_future(
		&self,
		id: &tg::build::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send + 'static>,
		>,
	> {
		self.try_get_build_outcome_future(id)
	}

	fn finish_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::finish::Arg,
	) -> impl Future<Output = tg::Result<bool>> {
		self.finish_build(id, arg)
	}

	fn touch_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.touch_build(id, arg)
	}

	fn heartbeat_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::build::heartbeat::Output>> {
		self.heartbeat_build(id, arg)
	}

	fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> impl Future<Output = tg::Result<()>> {
		self.lsp(input, output)
	}

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
	) -> impl Future<Output = tg::Result<tg::object::put::Output>> {
		self.put_object(id, arg)
	}

	fn push_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::push::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		self.push_object(id, arg)
	}

	fn pull_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::pull::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		self.pull_object(id, arg)
	}

	fn check_package(
		&self,
		arg: tg::package::check::Arg,
	) -> impl Future<Output = tg::Result<tg::package::check::Output>> {
		self.check_package(arg)
	}

	fn document_package(
		&self,
		arg: tg::package::document::Arg,
	) -> impl Future<Output = tg::Result<serde_json::Value>> {
		self.document_package(arg)
	}

	fn format_package(
		&self,
		arg: tg::package::format::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.format_package(arg)
	}

	fn try_get_reference(
		&self,
		reference: &tg::Reference,
	) -> impl Future<Output = tg::Result<Option<tg::Referent<Either<tg::build::Id, tg::object::Id>>>>>
	{
		self.try_get_reference(reference)
	}

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
		self.remove_remote(name)
	}

	fn get_js_runtime_doc(&self) -> impl Future<Output = tg::Result<serde_json::Value>> {
		self.get_js_runtime_doc()
	}

	fn health(&self) -> impl Future<Output = tg::Result<tg::Health>> {
		self.health()
	}

	fn clean(&self) -> impl Future<Output = tg::Result<()>> {
		self.clean()
	}

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

	fn try_build_target(
		&self,
		id: &tg::target::Id,
		arg: tg::target::build::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::target::build::Output>>> {
		self.try_build_target(id, arg)
	}

	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::user::User>>> {
		self.get_user(token)
	}
}

impl std::ops::Deref for Server {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[cfg(test)]
#[ctor::ctor]
fn ctor() {
	test_init();
}

#[cfg(test)]
fn test_init() {
	// Set file descriptor limit.
	let limit = 65536;
	let rlimit = libc::rlimit {
		rlim_cur: limit,
		rlim_max: limit,
	};
	let ret = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rlimit) };
	assert!(ret == 0, "failed to set the file descriptor limit");

	// Initialize v8.
	v8::icu::set_common_data_73(deno_core_icudata::ICU_DATA).unwrap();
	let platform = v8::new_default_platform(0, true);
	v8::V8::initialize_platform(platform.make_shared());
	v8::V8::initialize();
}
