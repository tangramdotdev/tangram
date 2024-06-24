use self::{database::Database, messenger::Messenger, runtime::Runtime, util::fs::remove};
use async_nats as nats;
use dashmap::DashMap;
use futures::{future, Future, FutureExt as _, Stream};
use http_body_util::BodyExt as _;
use hyper_util::rt::{TokioExecutor, TokioIo};
use itertools::Itertools as _;
use std::{
	collections::HashMap,
	convert::Infallible,
	os::fd::AsRawFd,
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
mod messenger;
mod migrations;
mod object;
mod package;
mod reference;
mod remote;
mod runtime;
mod server;
mod tag;
mod target;
mod tmp;
mod user;
mod util;
mod vfs;

pub use self::options::Options;

pub mod options;

/// A server.
#[derive(Clone)]
pub struct Server(Arc<Inner>);

pub struct Inner {
	artifact_store_task_map: ArtifactStoreTaskMap,
	blob_store_task_map: BlobStoreTaskMap,
	build_permits: BuildPermits,
	build_semaphore: Arc<tokio::sync::Semaphore>,
	builds: BuildTaskMap,
	checkout_task_map: CheckoutTaskMap,
	database: Database,
	file_descriptor_semaphore: tokio::sync::Semaphore,
	local_pool_handle: tokio_util::task::LocalPoolHandle,
	lock_file: Mutex<Option<tokio::fs::File>>,
	messenger: Messenger,
	options: Options,
	path: PathBuf,
	remotes: DashMap<String, tg::Client>,
	runtimes: RwLock<HashMap<String, Runtime>>,
	task: Mutex<Option<Task<tg::Result<()>>>>,
	vfs: Mutex<Option<self::vfs::Server>>,
}

type ArtifactStoreTaskMap = TaskMap<tg::artifact::Id, tg::Result<bool>, fnv::FnvBuildHasher>;

type BlobStoreTaskMap = TaskMap<tg::blob::Id, tg::Result<bool>, fnv::FnvBuildHasher>;

type BuildPermits =
	DashMap<tg::build::Id, Arc<tokio::sync::Mutex<Option<BuildPermit>>>, fnv::FnvBuildHasher>;

struct BuildPermit(
	#[allow(dead_code)]
	Either<tokio::sync::OwnedSemaphorePermit, tokio::sync::OwnedMutexGuard<Option<Self>>>,
);

type BuildTaskMap = TaskMap<tg::build::Id, (), fnv::FnvBuildHasher>;

type CheckoutTaskMap =
	TaskMap<tg::artifact::Id, tg::Result<std::path::PathBuf>, fnv::FnvBuildHasher>;

impl Server {
	pub async fn start(options: Options) -> tg::Result<Server> {
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

		// Migrate the directory.
		Self::migrate(&path).await?;

		// Ensure the blobs directory exists.
		let blobs_path = path.join("blobs");
		tokio::fs::create_dir_all(&blobs_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the blobs directory"))?;

		// Ensure the checkouts directory exists.
		let checkouts_path = path.join("checkouts");
		tokio::fs::create_dir_all(&checkouts_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the checkouts directory"))?;

		// Ensure the logs directory exists.
		let logs_path = path.join("logs");
		tokio::fs::create_dir_all(&logs_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the logs directory"))?;

		// Ensure the tmp directory exists.
		let tmp_path = path.join("tmp");
		tokio::fs::create_dir_all(&tmp_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the tmp directory"))?;

		// Remove an existing socket file.
		let socket_path = path.join("socket");
		tokio::fs::remove_file(&socket_path).await.ok();

		// Create the artifact store task map.
		let artifact_store_task_map = TaskMap::default();

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
			self::options::Database::Sqlite(options) => {
				let options = db::sqlite::Options {
					path: path.join("database"),
					connections: options.connections,
				};
				let database = db::sqlite::Database::new(options)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the database"))?;
				Either::Left(database)
			},
			self::options::Database::Postgres(options) => {
				let options = db::postgres::Options {
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
			self::options::Messenger::Memory => {
				Messenger::Left(tangram_messenger::memory::Messenger::new())
			},
			self::options::Messenger::Nats(nats) => {
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
			artifact_store_task_map,
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
			options,
			path,
			remotes,
			runtimes,
			task,
			vfs,
		}));

		// Start the task.
		let task = Task::spawn(|stop| {
			let server = server.clone();
			async move { server.task(stop).await }
		});
		server.task.lock().unwrap().replace(task);

		Ok(server)
	}

	pub fn stop(&self) {
		self.task.lock().unwrap().as_ref().unwrap().stop();
	}

	pub async fn wait(&self) -> tg::Result<()> {
		let task = self.task.lock().unwrap().clone().unwrap();
		task.wait()
			.await
			.map_err(|source| tg::error!(!source, "the task failed"))?
	}

	pub async fn task(&self, stop: Stop) -> tg::Result<()> {
		// Spawn tasks to connect the remotes.
		for remote in self.remotes.iter().map(|entry| entry.value().clone()) {
			tokio::spawn(async move { remote.connect().await.ok() });
		}

		// Start the VFS if necessary.
		if let Some(options) = self.options.vfs {
			// If the VFS is enabled, then start the VFS server.
			let kind = if cfg!(target_os = "macos") {
				vfs::Kind::Nfs
			} else if cfg!(target_os = "linux") {
				vfs::Kind::Fuse
			} else {
				unreachable!()
			};
			let artifacts_path = self.artifacts_path();
			let vfs = self::vfs::Server::start(self, kind, &artifacts_path, options)
				.await
				.inspect_err(|source| {
					tracing::error!(%source, "failed to start the VFS");
				})
				.ok();
			if let Some(vfs) = vfs {
				self.vfs.lock().unwrap().replace(vfs);
			}
		}

		// If there is no VFS, then create a symlink from the artifacts directory to the checkouts directory.
		if self.vfs.lock().unwrap().is_none() {
			let artifacts_path = self.artifacts_path();
			remove(&artifacts_path).await.ok();
			tokio::fs::symlink("checkouts", artifacts_path)
				.await
				.map_err(|source|tg::error!(!source, "failed to create a symlink from the artifacts directory to the checkouts directory"))?;
		}

		// Add the runtimes.
		{
			let triple = "builtin".to_owned();
			let runtime = self::runtime::builtin::Runtime::new(self);
			let runtime = self::runtime::Runtime::Builtin(runtime);
			self.runtimes.write().unwrap().insert(triple, runtime);
		}
		{
			let triple = "js".to_owned();
			let runtime = self::runtime::js::Runtime::new(self);
			let runtime = self::runtime::Runtime::Js(runtime);
			self.runtimes.write().unwrap().insert(triple, runtime);
		}
		#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
		{
			let triple = "aarch64-darwin".to_owned();
			let runtime = self::runtime::darwin::Runtime::new(self);
			let runtime = self::runtime::Runtime::Darwin(runtime);
			self.runtimes.write().unwrap().insert(triple, runtime);
		}
		#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
		{
			let triple = "aarch64-linux".to_owned();
			let runtime = self::runtime::linux::Runtime::new(self).await?;
			let runtime = self::runtime::Runtime::Linux(runtime);
			self.runtimes.write().unwrap().insert(triple, runtime);
		}
		#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
		{
			let triple = "x86_64-darwin".to_owned();
			let runtime = self::runtime::darwin::Runtime::new(self);
			let runtime = self::runtime::Runtime::Darwin(runtime);
			self.runtimes.write().unwrap().insert(triple, runtime);
		}
		#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
		{
			let triple = "x86_64-linux".to_owned();
			let runtime = self::runtime::linux::Runtime::new(self).await?;
			let runtime = self::runtime::Runtime::Linux(runtime);
			self.runtimes.write().unwrap().insert(triple, runtime);
		}

		// Start the build heartbeat monitor task.
		let build_heartbeat_monitor_task =
			self.options
				.build_heartbeat_monitor
				.as_ref()
				.map(|options| {
					tokio::spawn({
						let server = self.clone();
						let options = options.clone();
						async move { server.build_heartbeat_monitor_task(&options).await }
					})
				});

		// Start the build indexer task.
		let build_indexer_task = if self.options.build_indexer.is_some() {
			Some(tokio::spawn({
				let server = self.clone();
				async move { server.build_indexer_task().await }
			}))
		} else {
			None
		};

		// Start the object indexer task.
		let object_indexer_task = if self.options.object_indexer.is_some() {
			Some(tokio::spawn({
				let server = self.clone();
				async move { server.object_indexer_task().await }
			}))
		} else {
			None
		};

		// Start the build spawn task.
		let build_spawn_task = if self.options.build.is_some() {
			Some(tokio::spawn({
				let server = self.clone();
				async move { server.build_spawn_task().await }
			}))
		} else {
			None
		};

		// Serve.
		Self::serve(self.clone(), self.options.url.clone(), stop.clone()).await?;

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
		self.builds.abort_all();
		self.builds.wait().await;

		// Remove the runtimes.
		self.runtimes.write().unwrap().clear();

		// Abort the checkouts.
		self.checkout_task_map.abort_all();
		self.checkout_task_map.wait().await;

		// Stop the VFS.
		let vfs = self.vfs.lock().unwrap().take();
		if let Some(vfs) = vfs {
			vfs.stop();
			vfs.wait().await?;
		}

		// Release the lock file.
		let lock_file = self.lock_file.lock().unwrap().take();
		if let Some(lock_file) = lock_file {
			lock_file.set_len(0).await.ok();
		}

		Ok(())
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
	pub fn checkouts_path(&self) -> PathBuf {
		self.path.join("checkouts")
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
	pub fn tmp_path(&self) -> PathBuf {
		self.path.join("tmp")
	}
}

impl Server {
	async fn serve<H>(handle: H, url: Url, stop: Stop) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		// Create the task tracker.
		let task_tracker = tokio_util::task::TaskTracker::new();

		// Create the listener.
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

		tracing::trace!("serving on {url}");

		loop {
			// Accept a new connection.
			let accept = async {
				let stream = match &listener {
					tokio_util::either::Either::Left(listener) => tokio_util::either::Either::Left(
						listener
							.accept()
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to accept a new connection")
							})?
							.0,
					),
					tokio_util::either::Either::Right(listener) => {
						tokio_util::either::Either::Right(
							listener
								.accept()
								.await
								.map_err(|source| {
									tg::error!(!source, "failed to accept a new connection")
								})?
								.0,
						)
					},
				};
				Ok::<_, tg::Error>(TokioIo::new(stream))
			};
			let stream = match future::select(pin!(accept), pin!(stop.stopped())).await {
				future::Either::Left((result, _)) => result?,
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
					let result = match future::select(pin!(connection), pin!(stop.stopped())).await
					{
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

		Ok(())
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
			(http::Method::POST, ["builds", build, "children"]) => {
				Self::handle_add_build_child_request(handle, request, build).boxed()
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
			impl Stream<Item = tg::Result<tg::Progress<tg::artifact::Id>>> + Send + 'static,
		>,
	> {
		self.check_in_artifact(arg)
	}

	fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::Progress<PathBuf>>> + Send + 'static>,
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
		Output = tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static>,
	> {
		self.push_build(id, arg)
	}

	fn pull_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::pull::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static>,
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
	) -> impl Future<Output = tg::Result<Option<bool>>> {
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

	fn add_build_child(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::post::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.add_build_child(id, arg)
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
	) -> impl Future<Output = tg::Result<Option<bool>>> {
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
		Output = tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static>,
	> {
		self.push_object(id, arg)
	}

	fn pull_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::pull::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static>,
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
	) -> impl Future<Output = tg::Result<Option<tg::reference::get::Output>>> {
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

	fn health(&self) -> impl Future<Output = tg::Result<tg::server::Health>> {
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
