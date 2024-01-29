use self::database::Database;
pub use self::options::Options;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{
	future,
	stream::{BoxStream, FuturesUnordered},
	FutureExt, TryStreamExt,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use itertools::Itertools;
use std::{
	collections::HashMap,
	convert::Infallible,
	os::fd::AsRawFd,
	path::{Path, PathBuf},
	sync::Arc,
};
use tangram_client as tg;
use tangram_error::{Error, Result, Wrap, WrapErr};
use tangram_util::{
	fs::rmrf,
	http::{full, get_token, Incoming, Outgoing},
};
use tokio::net::{TcpListener, UnixListener};
use tokio_util::either::Either;

mod artifact;
mod build;
mod clean;
mod database;
mod migrations;
mod object;
pub mod options;
mod package;
mod server;
mod user;

/// A server.
#[derive(Clone)]
pub struct Server {
	inner: Arc<Inner>,
}

struct Inner {
	build_context:
		std::sync::RwLock<HashMap<tg::build::Id, Arc<BuildContext>, fnv::FnvBuildHasher>>,
	build_semaphore: Arc<tokio::sync::Semaphore>,
	database: Database,
	file_descriptor_semaphore: tokio::sync::Semaphore,
	http_task: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
	http_task_stop_sender: tokio::sync::watch::Sender<bool>,
	local_queue_task: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
	local_queue_task_stop_sender: tokio::sync::watch::Sender<bool>,
	local_queue_task_wake_sender: tokio::sync::watch::Sender<()>,
	lockfile: std::sync::Mutex<Option<tokio::fs::File>>,
	path: PathBuf,
	remote: Option<Box<dyn tg::Handle>>,
	remote_queue_task: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
	remote_queue_task_stop_sender: tokio::sync::watch::Sender<bool>,
	shutdown: tokio::sync::watch::Sender<bool>,
	shutdown_task: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
	version: String,
	vfs: std::sync::Mutex<Option<tangram_vfs::Server>>,
}

struct BuildContext {
	build: tg::build::Id,
	children: Option<tokio::sync::watch::Sender<()>>,
	depth: u64,
	log: Option<tokio::sync::watch::Sender<()>>,
	status: Option<tokio::sync::watch::Sender<()>>,
	stop: tokio::sync::watch::Sender<bool>,
	task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct Tmp {
	path: PathBuf,
}

impl Server {
	#[allow(clippy::too_many_lines)]
	pub async fn start(options: Options) -> Result<Server> {
		// Get the address.
		let address = options.address;

		// Get the path.
		let path = options.path;

		// Ensure the path exists.
		tokio::fs::create_dir_all(&path)
			.await
			.wrap_err("Failed to create the directory.")?;

		// Acquire the lockfile.
		let lockfile = tokio::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(path.join("lock"))
			.await
			.wrap_err("Failed to open the lockfile.")?;
		let ret = unsafe { libc::flock(lockfile.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
		if ret != 0 {
			return Err(std::io::Error::last_os_error().wrap("Failed to acquire the lockfile."));
		}
		let lockfile = std::sync::Mutex::new(Some(lockfile));

		// Migrate the path.
		Self::migrate(&path).await?;

		// Write the PID file.
		tokio::fs::write(&path.join("pid"), std::process::id().to_string())
			.await
			.wrap_err("Failed to write the PID file.")?;

		// Remove an existing socket file.
		rmrf(&path.join("socket"))
			.await
			.wrap_err("Failed to remove an existing socket file.")?;

		// Create the build context.
		let build_context = std::sync::RwLock::new(HashMap::default());

		// Create the build semaphore.
		let permits = options
			.build
			.permits
			.unwrap_or_else(|| std::thread::available_parallelism().unwrap().get());
		let build_semaphore = Arc::new(tokio::sync::Semaphore::new(permits));

		// Open the database.
		let database_path = path.join("database");
		let database = Database::new(&database_path).await?;

		// Create the file system semaphore.
		let file_descriptor_semaphore = tokio::sync::Semaphore::new(16);

		// Create the http task.
		let http_task = std::sync::Mutex::new(None);

		// Create the http task stop channel.
		let (http_task_stop_sender, http_task_stop_receiver) = tokio::sync::watch::channel(false);

		// Create the local queue task.
		let local_queue_task = std::sync::Mutex::new(None);

		// Create the local queue task wake channel.
		let (local_queue_task_wake_sender, local_queue_task_wake_receiver) =
			tokio::sync::watch::channel(());

		// Create the local queue task stop channel.
		let (local_queue_task_stop_sender, local_queue_task_stop_receiver) =
			tokio::sync::watch::channel(false);

		// Get the remote.
		let remote = if let Some(remote) = options.remote {
			Some(remote.tg)
		} else {
			None
		};

		// Create the remote queue task.
		let remote_queue_task = std::sync::Mutex::new(None);

		// Create the remote queue task stop channel.
		let (remote_queue_task_stop_sender, remote_queue_task_stop_receiver) =
			tokio::sync::watch::channel(false);

		// Create the shutdown channel.
		let (shutdown, _) = tokio::sync::watch::channel(false);

		// Create the shutdown task.
		let shutdown_task = std::sync::Mutex::new(None);

		// Get the version.
		let version = options.version;

		// Create the vfs.
		let vfs = std::sync::Mutex::new(None);

		// Create the server.
		let inner = Arc::new(Inner {
			build_context,
			build_semaphore,
			database,
			file_descriptor_semaphore,
			http_task,
			http_task_stop_sender,
			local_queue_task,
			local_queue_task_stop_sender,
			local_queue_task_wake_sender,
			lockfile,
			path,
			remote,
			remote_queue_task,
			remote_queue_task_stop_sender,
			shutdown,
			shutdown_task,
			version,
			vfs,
		});
		let server = Server { inner };

		// Empty the queue.
		{
			let db = server.inner.database.get().await?;
			let statement = "
				delete from build_queue;
			";
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute([])
				.wrap_err("Failed to execute the query.")?;
		}

		// Terminate unfinished builds.
		{
			let db = server.inner.database.get().await?;
			let statement = r#"
				update builds
				set state = json_set(
					state,
					'$.status', 'finished',
					'$.outcome', json('{"kind":"terminated"}')
				)
				where state->>'status' != 'finished';
			"#;
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute([])
				.wrap_err("Failed to execute the query.")?;
		}

		// Start the VFS if necessary and set up the checkouts directory.
		let artifacts_path = server.artifacts_path();
		rmrf(&artifacts_path)
			.await
			.wrap_err("Failed to remove the artifacts directory.")?;
		if options.vfs.enable {
			// Create the artifacts directory.
			tokio::fs::create_dir_all(&artifacts_path)
				.await
				.wrap_err("Failed to create the artifacts directory.")?;

			// Start the VFS server.
			let vfs = tangram_vfs::Server::start(&server, &artifacts_path)
				.await
				.wrap_err("Failed to start the VFS.")?;

			server.inner.vfs.lock().unwrap().replace(vfs);
		} else {
			// Create a symlink from the artifacts directory to the checkouts directory.
			tokio::fs::symlink("checkouts", artifacts_path)
				.await
				.wrap_err("Failed to create a symlink from the artifacts directory to the checkouts directory.")?;
		}

		// Start the build queue task.
		let task = tokio::spawn({
			let server = server.clone();
			async move {
				let result = server
					.local_queue_task(
						local_queue_task_wake_receiver,
						local_queue_task_stop_receiver,
					)
					.await;
				match result {
					Ok(()) => Ok(()),
					Err(error) => {
						tracing::error!(?error);
						Err(error)
					},
				}
			}
		});
		server.inner.local_queue_task.lock().unwrap().replace(task);

		// Start the build queue remote task.
		let task = tokio::spawn({
			let server = server.clone();
			async move {
				let result = server
					.remote_queue_task(remote_queue_task_stop_receiver)
					.await;
				match result {
					Ok(()) => Ok(()),
					Err(error) => {
						tracing::error!(?error);
						Err(error)
					},
				}
			}
		});
		server.inner.remote_queue_task.lock().unwrap().replace(task);

		// Start the http task.
		let task = tokio::spawn({
			let server = server.clone();
			async move {
				match server.serve(address, http_task_stop_receiver).await {
					Ok(()) => Ok(()),
					Err(error) => {
						tracing::error!(?error);
						Err(error)
					},
				}
			}
		});
		server.inner.http_task.lock().unwrap().replace(task);

		Ok(server)
	}

	pub fn stop(&self) {
		let server = self.clone();
		let task = tokio::spawn(async move {
			// Stop the http task.
			server.inner.http_task_stop_sender.send_replace(true);

			// Stop the local queue task.
			server.inner.local_queue_task_stop_sender.send_replace(true);

			// Stop the remote queue task.
			server
				.inner
				.remote_queue_task_stop_sender
				.send_replace(true);

			// Join the http task.
			let task = server.inner.http_task.lock().unwrap().take();
			if let Some(task) = task {
				match task.await {
					Ok(result) => Ok(result),
					Err(error) if error.is_cancelled() => Ok(Ok(())),
					Err(error) => Err(error),
				}
				.unwrap()?;
			}

			// Join the local queue task.
			let local_queue_task = server
				.inner
				.local_queue_task
				.lock()
				.unwrap()
				.take()
				.unwrap();
			local_queue_task.await.unwrap()?;

			// Join the remote queue task.
			let remote_queue_task = server
				.inner
				.remote_queue_task
				.lock()
				.unwrap()
				.take()
				.unwrap();
			remote_queue_task.await.unwrap()?;

			// Stop running builds.
			for context in server.inner.build_context.read().unwrap().values() {
				context.stop.send_replace(true);
			}

			// Join running builds.
			let tasks = server
				.inner
				.build_context
				.read()
				.unwrap()
				.values()
				.cloned()
				.filter_map(|context| context.task.lock().unwrap().take())
				.collect_vec();
			tasks
				.into_iter()
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await
				.unwrap();

			// Remove running builds.
			server.inner.build_context.write().unwrap().clear();

			// Join the vfs.
			let vfs = server.inner.vfs.lock().unwrap().clone();
			if let Some(vfs) = vfs {
				vfs.stop();
				vfs.join().await?;
			}

			// Release the lockfile.
			server.inner.lockfile.lock().unwrap().take();

			Ok(())
		});

		self.inner.shutdown_task.lock().unwrap().replace(task);
		self.inner.shutdown.send_replace(true);
	}

	pub async fn join(&self) -> Result<()> {
		self.inner
			.shutdown
			.subscribe()
			.wait_for(|shutdown| *shutdown)
			.await
			.unwrap();
		let task = self.inner.shutdown_task.lock().unwrap().take();
		if let Some(task) = task {
			task.await.unwrap()?;
		}
		Ok(())
	}

	#[allow(clippy::unused_async)]
	async fn health(&self) -> Result<tg::server::Health> {
		Ok(tg::server::Health {
			version: self.inner.version.clone(),
		})
	}

	#[must_use]
	pub fn path(&self) -> &Path {
		&self.inner.path
	}

	#[must_use]
	pub fn artifacts_path(&self) -> PathBuf {
		self.path().join("artifacts")
	}

	#[must_use]
	pub fn checkouts_path(&self) -> PathBuf {
		self.path().join("checkouts")
	}

	#[must_use]
	pub fn database_path(&self) -> PathBuf {
		self.path().join("database")
	}

	#[must_use]
	pub fn tmp_path(&self) -> PathBuf {
		self.path().join("tmp")
	}

	fn create_tmp(&self) -> Tmp {
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		let id = uuid::Uuid::now_v7();
		let id = ENCODING.encode(&id.into_bytes());
		let path = self.tmp_path().join(id);
		Tmp { path }
	}
}

impl Server {
	pub async fn serve(
		self,
		address: tg::Address,
		mut stop: tokio::sync::watch::Receiver<bool>,
	) -> Result<()> {
		// Create the tasks.
		let mut tasks = tokio::task::JoinSet::new();

		// Create the listener.
		let listener = match &address {
			tg::Address::Unix(path) => Either::Left(
				UnixListener::bind(path).wrap_err("Failed to create the UNIX listener.")?,
			),
			tg::Address::Inet(inet) => Either::Right(
				TcpListener::bind(inet.to_string())
					.await
					.wrap_err("Failed to create the TCP listener.")?,
			),
		};

		tracing::info!("🚀 Serving on {address:?}.");

		loop {
			// Accept a new connection.
			let accept = async {
				let stream = match &listener {
					Either::Left(listener) => Either::Left(
						listener
							.accept()
							.await
							.wrap_err("Failed to accept a new connection.")?
							.0,
					),
					Either::Right(listener) => Either::Right(
						listener
							.accept()
							.await
							.wrap_err("Failed to accept a new connection.")?
							.0,
					),
				};
				Ok::<_, Error>(TokioIo::new(stream))
			};
			let stream = tokio::select! {
				stream = accept => stream?,
				() = stop.wait_for(|stop| *stop).map(|_| ()) => {
					break
				},
			};

			// Create the service.
			let service = hyper::service::service_fn({
				let server = self.clone();
				let stop = stop.clone();
				move |mut request| {
					let server = server.clone();
					let stop = stop.clone();
					async move {
						request.extensions_mut().insert(stop);
						let response = server.handle_request(request).await;
						Ok::<_, Infallible>(response)
					}
				}
			});

			// Spawn a task to serve the connection.
			tasks.spawn({
				let mut stop = stop.clone();
				async move {
					let builder =
						hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
					let connection = builder.serve_connection_with_upgrades(stream, service);
					tokio::pin!(connection);
					let result = tokio::select! {
						result = connection.as_mut() => {
							Some(result)
						},
						() = stop.wait_for(|stop| *stop).map(|_| ()) => {
							connection.as_mut().graceful_shutdown();
							None
						}
					};
					let result = match result {
						Some(result) => result,
						None => connection.await,
					};
					if let Err(error) = result {
						tracing::error!(?error, "Failed to serve the connection.");
					}
				}
			});
		}

		// Join all tasks.
		while let Some(result) = tasks.join_next().await {
			result.unwrap();
		}

		Ok(())
	}

	async fn try_get_user_from_request(
		&self,
		request: &http::Request<Incoming>,
	) -> Result<Option<tg::user::User>> {
		// Get the token.
		let Some(token) = get_token(request, None) else {
			return Ok(None);
		};

		// Get the user.
		let user = self.get_user_for_token(&token).await?;

		Ok(user)
	}

	#[allow(clippy::too_many_lines)]
	async fn handle_request(
		&self,
		mut request: http::Request<Incoming>,
	) -> http::Response<Outgoing> {
		let id = tg::Id::new_uuidv7(tg::id::Kind::Request);
		request.extensions_mut().insert(id.clone());

		tracing::info!(?id, method = ?request.method(), path = ?request.uri().path(), "Received request.");

		let method = request.method().clone();
		let path_components = request.uri().path().split('/').skip(1).collect_vec();
		let response = match (method, path_components.as_slice()) {
			// Artifacts
			(http::Method::POST, ["artifacts", "checkin"]) => self
				.handle_check_in_artifact_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["artifacts", "checkout"]) => self
				.handle_check_out_artifact_request(request)
				.map(Some)
				.boxed(),

			// Builds
			(http::Method::GET, ["builds"]) => {
				self.handle_list_builds_request(request).map(Some).boxed()
			},
			(http::Method::HEAD, ["builds", _]) => self
				.handle_get_build_exists_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["builds", _]) => {
				self.handle_get_build_request(request).map(Some).boxed()
			},
			(http::Method::PUT, ["builds", _]) => {
				self.handle_put_build_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["builds"]) => self
				.handle_get_or_create_build_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["builds", "dequeue"]) => {
				self.handle_dequeue_build_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["builds", _, "status"]) => self
				.handle_get_build_status_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["builds", _, "status"]) => self
				.handle_set_build_status_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["builds", _, "children"]) => self
				.handle_get_build_children_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["builds", _, "children"]) => self
				.handle_add_build_child_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["builds", _, "log"]) => {
				self.handle_get_build_log_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["builds", _, "log"]) => {
				self.handle_add_build_log_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["builds", _, "outcome"]) => self
				.handle_get_build_outcome_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["builds", _, "outcome"]) => self
				.handle_set_build_outcome_request(request)
				.map(Some)
				.boxed(),

			// Objects
			(http::Method::HEAD, ["objects", _]) => self
				.handle_get_object_exists_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["objects", _]) => {
				self.handle_get_object_request(request).map(Some).boxed()
			},
			(http::Method::PUT, ["objects", _]) => {
				self.handle_put_object_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["objects", _, "push"]) => {
				self.handle_push_object_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["objects", _, "pull"]) => {
				self.handle_pull_object_request(request).map(Some).boxed()
			},

			// Packages
			(http::Method::GET, ["packages", "search"]) => self
				.handle_search_packages_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["packages", _]) => {
				self.handle_get_package_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["packages", _, "versions"]) => self
				.handle_get_package_versions_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["packages", _, "metadata"]) => self
				.handle_get_package_metadata_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["packages", _, "dependencies"]) => self
				.handle_get_package_dependencies_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["packages"]) => self
				.handle_publish_package_request(request)
				.map(Some)
				.boxed(),

			// Server
			(http::Method::GET, ["health"]) => {
				self.handle_health_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["clean"]) => self.handle_clean_request(request).map(Some).boxed(),
			(http::Method::POST, ["stop"]) => self.handle_stop_request(request).map(Some).boxed(),

			// Users
			(http::Method::POST, ["logins"]) => {
				self.handle_create_login_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["logins", _]) => {
				self.handle_get_login_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["user"]) => self
				.handle_get_user_for_token_request(request)
				.map(Some)
				.boxed(),

			(_, _) => future::ready(None).boxed(),
		}
		.await;

		let mut response = match response {
			None => http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
				.body(full("not found"))
				.unwrap(),
			Some(Err(error)) => {
				let body = serde_json::to_string(&error)
					.unwrap_or_else(|_| "internal server error".to_owned());
				http::Response::builder()
					.status(http::StatusCode::INTERNAL_SERVER_ERROR)
					.body(full(body))
					.unwrap()
			},
			Some(Ok(response)) => response,
		};

		let key = http::HeaderName::from_static("x-tangram-request-id");
		let value = http::HeaderValue::from_str(&id.to_string()).unwrap();
		response.headers_mut().insert(key, value);

		tracing::info!(?id, status = ?response.status(), "Sending response.");

		response
	}
}

#[async_trait]
impl tg::Handle for Server {
	fn clone_box(&self) -> Box<dyn tg::Handle> {
		Box::new(self.clone())
	}

	fn file_descriptor_semaphore(&self) -> &tokio::sync::Semaphore {
		&self.inner.file_descriptor_semaphore
	}

	async fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> Result<tg::artifact::CheckInOutput> {
		self.check_in_artifact(arg).await
	}

	async fn check_out_artifact(&self, arg: tg::artifact::CheckOutArg) -> Result<()> {
		self.check_out_artifact(arg).await
	}

	async fn try_list_builds(&self, args: tg::build::ListArg) -> Result<tg::build::ListOutput> {
		self.try_list_builds(args).await
	}

	async fn get_build_exists(&self, id: &tg::build::Id) -> Result<bool> {
		self.get_build_exists(id).await
	}

	async fn try_get_build(&self, id: &tg::build::Id) -> Result<Option<tg::build::GetOutput>> {
		self.try_get_build(id).await
	}

	async fn try_put_build(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		state: &tg::build::State,
	) -> Result<tg::build::PutOutput> {
		self.try_put_build(user, id, state).await
	}

	async fn push_build(&self, user: Option<&tg::User>, id: &tg::build::Id) -> Result<()> {
		self.push_build(user, id).await
	}

	async fn pull_build(&self, id: &tg::build::Id) -> Result<()> {
		self.pull_build(id).await
	}

	async fn get_or_create_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::GetOrCreateArg,
	) -> Result<tg::build::GetOrCreateOutput> {
		self.get_or_create_build(user, arg).await
	}

	async fn try_dequeue_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::queue::DequeueArg,
	) -> Result<Option<tg::build::queue::DequeueOutput>> {
		self.try_dequeue_build(user, arg).await
	}

	async fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Status>>>> {
		self.try_get_build_status(id, arg, None).await
	}

	async fn set_build_status(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> Result<()> {
		self.set_build_status(user, id, status).await
	}

	async fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::GetArg,
	) -> Result<Option<BoxStream<'static, Result<tg::build::children::Chunk>>>> {
		self.try_get_build_children(id, arg, None).await
	}

	async fn add_build_child(
		&self,
		user: Option<&tg::User>,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> Result<()> {
		self.add_build_child(user, build_id, child_id).await
	}

	async fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
	) -> Result<Option<BoxStream<'static, Result<tg::build::log::Chunk>>>> {
		self.try_get_build_log(id, arg, None).await
	}

	async fn add_build_log(
		&self,
		user: Option<&tg::User>,
		build_id: &tg::build::Id,
		bytes: Bytes,
	) -> Result<()> {
		self.add_build_log(user, build_id, bytes).await
	}

	async fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
	) -> Result<Option<Option<tg::build::Outcome>>> {
		self.try_get_build_outcome(id, arg, None).await
	}

	async fn set_build_outcome(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<()> {
		self.set_build_outcome(user, id, outcome).await
	}

	async fn get_object_exists(&self, id: &tg::object::Id) -> Result<bool> {
		self.get_object_exists(id).await
	}

	async fn try_get_object(&self, id: &tg::object::Id) -> Result<Option<tg::object::GetOutput>> {
		self.try_get_object(id).await
	}

	async fn try_put_object(
		&self,
		id: &tg::object::Id,
		bytes: &Bytes,
	) -> Result<tg::object::PutOutput> {
		self.try_put_object(id, bytes).await
	}

	async fn push_object(&self, id: &tg::object::Id) -> Result<()> {
		self.push_object(id).await
	}

	async fn pull_object(&self, id: &tg::object::Id) -> Result<()> {
		self.pull_object(id).await
	}

	async fn search_packages(
		&self,
		arg: tg::package::SearchArg,
	) -> Result<tg::package::SearchOutput> {
		self.search_packages(arg).await
	}

	async fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> Result<Option<tg::package::GetOutput>> {
		self.try_get_package(dependency, arg).await
	}

	async fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<Vec<String>>> {
		self.try_get_package_versions(dependency).await
	}

	async fn publish_package(&self, user: Option<&tg::User>, id: &tg::directory::Id) -> Result<()> {
		self.publish_package(user, id).await
	}

	async fn health(&self) -> Result<tg::server::Health> {
		self.health().await
	}

	async fn clean(&self) -> Result<()> {
		self.clean().await
	}

	async fn stop(&self) -> Result<()> {
		self.stop();
		Ok(())
	}

	async fn create_login(&self) -> Result<tg::user::Login> {
		self.create_login().await
	}

	async fn get_login(&self, id: &tg::Id) -> Result<Option<tg::user::Login>> {
		self.get_login(id).await
	}

	async fn get_user_for_token(&self, token: &str) -> Result<Option<tg::user::User>> {
		self.get_user_for_token(token).await
	}
}

impl AsRef<Path> for Tmp {
	fn as_ref(&self) -> &Path {
		&self.path
	}
}

impl Drop for Tmp {
	fn drop(&mut self) {
		tokio::spawn(rmrf(self.path.clone()));
	}
}
