use self::{
	database::{Database, Transaction},
	messenger::Messenger,
	runtime::Runtime,
	util::fs::remove,
};
use async_nats as nats;
use bytes::Bytes;
use dashmap::DashMap;
use either::Either;
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
	sync::Arc,
};
use tangram_client as tg;
use tangram_database as db;
use tangram_futures::task::{Stop, Task, TaskMap};
use tangram_http::{Incoming, Outgoing};
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
mod remote;
mod root;
mod runtime;
mod server;
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
	build_permits: BuildPermits,
	build_semaphore: Arc<tokio::sync::Semaphore>,
	builds: BuildTaskMap,
	checkouts: CheckoutTaskMap,
	database: Database,
	file_descriptor_semaphore: tokio::sync::Semaphore,
	object_index_queue: ObjectIndexQueue,
	local_pool_handle: tokio_util::task::LocalPoolHandle,
	lock_file: std::sync::Mutex<Option<tokio::fs::File>>,
	messenger: Messenger,
	options: Options,
	path: PathBuf,
	remotes: DashMap<String, tg::Client>,
	runtimes: std::sync::RwLock<HashMap<String, Runtime>>,
	task: std::sync::Mutex<Option<Task<tg::Result<()>>>>,
	vfs: std::sync::Mutex<Option<self::vfs::Server>>,
}

type BuildPermits =
	DashMap<tg::build::Id, Arc<tokio::sync::Mutex<Option<BuildPermit>>>, fnv::FnvBuildHasher>;

struct BuildPermit(
	#[allow(dead_code)]
	Either<tokio::sync::OwnedSemaphorePermit, tokio::sync::OwnedMutexGuard<Option<Self>>>,
);

type BuildTaskMap = TaskMap<tg::build::Id, (), fnv::FnvBuildHasher>;

type CheckoutTaskMap =
	TaskMap<tg::artifact::Id, tg::Result<tg::artifact::checkout::Output>, fnv::FnvBuildHasher>;

struct ObjectIndexQueue {
	sender: async_channel::Sender<tg::object::Id>,
	receiver: async_channel::Receiver<tg::object::Id>,
}

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
		let lock_file = std::sync::Mutex::new(Some(lock_file));

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

		// Create the object index queue.
		let (sender, receiver) = async_channel::unbounded();
		let object_index_queue = ObjectIndexQueue { sender, receiver };

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
			.map(|remote| (remote.name.clone(), remote.client.clone()))
			.collect();

		// Create the runtimes.
		let runtimes = std::sync::RwLock::new(HashMap::default());

		// Create the task.
		let task = std::sync::Mutex::new(None);

		// Create the vfs.
		let vfs = std::sync::Mutex::new(None);

		// Create the server.
		let server = Self(Arc::new(Inner {
			build_permits,
			build_semaphore,
			builds,
			checkouts,
			database,
			file_descriptor_semaphore,
			object_index_queue,
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
		// Start the VFS if necessary.
		if self.options.vfs {
			// If the VFS is enabled, then start the VFS server.
			let kind = if cfg!(target_os = "macos") {
				vfs::Kind::Nfs
			} else if cfg!(target_os = "linux") {
				vfs::Kind::Fuse
			} else {
				unreachable!()
			};
			let artifacts_path = self.artifacts_path();
			let vfs = self::vfs::Server::start(self, kind, &artifacts_path)
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
		let triple = "js".to_owned();
		let runtime = self::runtime::js::Runtime::new(self);
		let runtime = self::runtime::Runtime::Js(runtime);
		self.runtimes.write().unwrap().insert(triple, runtime);
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

		// Start the build spawn task.
		let build_queue_task = if self.options.build.is_some() {
			Some(tokio::spawn({
				let server = self.clone();
				async move { server.build_spawn_task().await }
			}))
		} else {
			None
		};

		// Start the build monitor task.
		let build_monitor_task = self.options.build_monitor.as_ref().map(|options| {
			tokio::spawn({
				let server = self.clone();
				let options = options.clone();
				async move { server.build_monitor_task(&options).await }
			})
		});

		// Start the index task.
		let index_task = if true {
			Some(tokio::spawn({
				let server = self.clone();
				async move { server.index_task().await }
			}))
		} else {
			None
		};

		// Serve.
		Self::serve(self.clone(), self.options.url.clone(), stop.clone()).await?;

		// Abort the index task.
		if let Some(task) = index_task {
			task.abort();
			task.await.ok();
		}

		// Abort the build queue task.
		if let Some(task) = build_queue_task {
			task.abort();
			task.await.ok();
		}

		// Abort the build monitor task.
		if let Some(task) = build_monitor_task {
			task.abort();
			task.await.ok();
		}

		// Abort the build tasks.
		self.builds.abort_all();
		self.builds.wait().await;

		// Remove the runtimes.
		self.runtimes.write().unwrap().clear();

		// Abort the checkouts.
		self.checkouts.abort_all();
		self.checkouts.wait().await;

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

			// Builds.
			(http::Method::GET, ["builds"]) => {
				Self::handle_list_builds_request(handle, request).boxed()
			},
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
			(http::Method::POST, ["format"]) => {
				Self::handle_format_request(handle, request).boxed()
			},
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
			(http::Method::GET, ["packages"]) => {
				Self::handle_list_packages_request(handle, request).boxed()
			},
			(http::Method::GET, ["packages", dependency]) => {
				Self::handle_get_package_request(handle, request, dependency).boxed()
			},
			(http::Method::GET, ["packages", dependency, "check"]) => {
				Self::handle_check_package_request(handle, request, dependency).boxed()
			},
			(http::Method::GET, ["packages", dependency, "doc"]) => {
				Self::handle_get_package_doc_request(handle, request, dependency).boxed()
			},
			(http::Method::GET, ["packages", dependency, "versions"]) => {
				Self::handle_get_package_versions_request(handle, request, dependency).boxed()
			},
			(http::Method::POST, ["packages", dependency, "format"]) => {
				Self::handle_format_package_request(handle, request, dependency).boxed()
			},
			(http::Method::GET, ["packages", dependency, "outdated"]) => {
				Self::handle_outdated_package_request(handle, request, dependency).boxed()
			},
			(http::Method::POST, ["packages", artifact, "publish"]) => {
				Self::handle_publish_package_request(handle, request, artifact).boxed()
			},
			(http::Method::POST, ["packages", dependency, "yank"]) => {
				Self::handle_yank_package_request(handle, request, dependency).boxed()
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

			// Roots.
			(http::Method::GET, ["roots"]) => {
				Self::handle_list_roots_request(handle, request).boxed()
			},
			(http::Method::GET, ["roots", name]) => {
				Self::handle_get_root_request(handle, request, name).boxed()
			},
			(http::Method::PUT, ["roots", name]) => {
				Self::handle_put_root_request(handle, request, name).boxed()
			},
			(http::Method::DELETE, ["roots", name]) => {
				Self::handle_delete_root_request(handle, request, name).boxed()
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

			// Targets.
			(http::Method::POST, ["targets", target, "build"]) => {
				Self::handle_build_target_request(handle, request, target).boxed()
			},

			// Users.
			(http::Method::GET, ["user"]) => Self::handle_get_user_request(handle, request).boxed(),

			(_, _) => future::ok(
				http::Response::builder()
					.status(http::StatusCode::NOT_FOUND)
					.body(Outgoing::bytes("not found"))
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
				.body(Outgoing::json(error))
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
	type Transaction<'a> = Transaction<'a>;

	fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::checkin::Output>> {
		self.check_in_artifact(arg)
	}

	fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::checkout::Output>> {
		self.check_out_artifact(id, arg)
	}

	fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<tg::blob::Id>> {
		self.create_blob(reader)
	}

	fn list_builds(
		&self,
		args: tg::build::list::Arg,
	) -> impl Future<Output = tg::Result<tg::build::list::Output>> {
		self.list_builds(args)
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
	) -> impl Future<Output = tg::Result<()>> {
		self.put_build(id, arg)
	}

	fn push_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::push::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::build::Progress>> + Send + 'static>,
	> {
		self.push_build(id, arg)
	}

	fn pull_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::pull::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::build::Progress>> + Send + 'static>,
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
	) -> impl Future<Output = tg::Result<Option<bool>>> {
		self.try_start_build(id)
	}

	fn try_get_build_status_stream(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>,
		>,
	> {
		self.try_get_build_status_stream(id, arg)
	}

	fn try_get_build_children_stream(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::children::Chunk>> + Send + 'static>,
		>,
	> {
		self.try_get_build_children_stream(id, arg)
	}

	fn add_build_child(
		&self,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<()>> {
		self.add_build_child(build_id, child_id)
	}

	fn try_get_build_log_stream(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::log::Chunk>> + Send + 'static>,
		>,
	> {
		self.try_get_build_log_stream(id, arg)
	}

	fn add_build_log(
		&self,
		build_id: &tg::build::Id,
		bytes: Bytes,
	) -> impl Future<Output = tg::Result<()>> {
		self.add_build_log(build_id, bytes)
	}

	fn try_get_build_outcome_future(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send + 'static>,
		>,
	> {
		self.try_get_build_outcome_future(id, arg)
	}

	fn finish_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.finish_build(id, arg)
	}

	fn touch_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> {
		self.touch_build(id)
	}

	fn heartbeat_build(
		&self,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<tg::build::heartbeat::Output>> + Send {
		self.heartbeat_build(id)
	}

	fn format(&self, text: String) -> impl Future<Output = tg::Result<String>> {
		self.format(text)
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
		transaction: Option<&Self::Transaction<'_>>,
	) -> impl Future<Output = tg::Result<tg::object::put::Output>> {
		self.put_object(id, arg, transaction)
	}

	fn push_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::push::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::object::Progress>> + Send + 'static>,
	> + Send {
		self.push_object(id, arg)
	}

	fn pull_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::pull::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::object::Progress>> + Send + 'static>,
	> + Send {
		self.pull_object(id, arg)
	}

	fn list_packages(
		&self,
		arg: tg::package::list::Arg,
	) -> impl Future<Output = tg::Result<tg::package::list::Output>> {
		self.list_packages(arg)
	}

	fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::package::get::Output>>> {
		self.try_get_package(dependency, arg)
	}

	fn check_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::check::Arg,
	) -> impl Future<Output = tg::Result<Vec<tg::Diagnostic>>> {
		self.check_package(dependency, arg)
	}

	fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::doc::Arg,
	) -> impl Future<Output = tg::Result<Option<serde_json::Value>>> {
		self.try_get_package_doc(dependency, arg)
	}

	fn format_package(&self, dependency: &tg::Dependency) -> impl Future<Output = tg::Result<()>> {
		self.format_package(dependency)
	}

	fn get_package_outdated(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::outdated::Arg,
	) -> impl Future<Output = tg::Result<tg::package::outdated::Output>> {
		self.get_package_outdated(dependency, arg)
	}

	fn publish_package(
		&self,
		id: &tg::artifact::Id,
		arg: tg::package::publish::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.publish_package(id, arg)
	}

	fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Option<Vec<String>>>> {
		self.try_get_package_versions(dependency)
	}

	fn yank_package(
		&self,
		id: &tg::artifact::Id,
		arg: tg::package::yank::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.yank_package(id, arg)
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

	fn list_roots(
		&self,
		arg: tg::root::list::Arg,
	) -> impl Future<Output = tg::Result<tg::root::list::Output>> {
		self.list_roots(arg)
	}

	fn try_get_root(
		&self,
		name: &str,
	) -> impl Future<Output = tg::Result<Option<tg::root::get::Output>>> {
		self.try_get_root(name)
	}

	fn put_root(
		&self,
		name: &str,
		arg: tg::root::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_root(name, arg)
	}

	fn delete_root(&self, name: &str) -> impl Future<Output = tg::Result<()>> {
		self.remove_root(name)
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

	fn build_target(
		&self,
		id: &tg::target::Id,
		arg: tg::target::build::Arg,
	) -> impl Future<Output = tg::Result<tg::target::build::Output>> {
		self.build_target(id, arg)
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
