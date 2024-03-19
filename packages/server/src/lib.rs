pub use self::options::Options;
use self::{database::Database, messenger::Messenger};
use async_nats as nats;
use async_trait::async_trait;
use bytes::Bytes;
use either::Either;
use futures::{
	future,
	stream::{BoxStream, FuturesUnordered},
	FutureExt, TryStreamExt,
};
use http_body_util::BodyExt;
use hyper_util::rt::{TokioExecutor, TokioIo};
use itertools::Itertools;
use std::{
	collections::HashMap,
	convert::Infallible,
	os::fd::AsRawFd,
	path::{Path, PathBuf},
	pin::pin,
	sync::Arc,
};

use tangram_client as tg;
use tangram_error::{error, Error, Result};
use tangram_util::{
	fs::rmrf,
	http::{full, get_token, Incoming, Outgoing},
};
use tokio::{
	io::{AsyncRead, AsyncWrite},
	net::{TcpListener, UnixListener},
};
use url::Url;

mod artifact;
mod blob;
mod build;
mod clean;
mod database;
mod language;
mod messenger;
mod migrations;
mod object;
pub mod options;
mod package;
mod runtime;
mod server;
mod user;
mod vfs;

/// A server.
#[derive(Clone)]
pub struct Server {
	inner: Arc<Inner>,
}

struct Inner {
	build_queue_task: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
	build_queue_task_stop: tokio::sync::watch::Sender<bool>,
	build_semaphore: Arc<tokio::sync::Semaphore>,
	build_state: std::sync::RwLock<HashMap<tg::build::Id, Arc<BuildState>, fnv::FnvBuildHasher>>,
	database: Database,
	file_descriptor_semaphore: tokio::sync::Semaphore,
	http: std::sync::Mutex<Option<Http>>,
	local_pool_handle: tokio_util::task::LocalPoolHandle,
	lockfile: std::sync::Mutex<Option<tokio::fs::File>>,
	messenger: Messenger,
	oauth: OAuth,
	options: Options,
	remote: Option<Box<dyn tg::Handle>>,
	#[cfg(target_os = "linux")]
	runtime_artifacts:
		tokio::sync::RwLock<HashMap<tg::Triple, RuntimeArtifactIds, fnv::FnvBuildHasher>>,
	shutdown: tokio::sync::watch::Sender<bool>,
	shutdown_task: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
	vfs: std::sync::Mutex<Option<self::vfs::Server>>,
}

struct BuildState {
	permit: Arc<tokio::sync::Mutex<Option<Permit>>>,
	stop: tokio::sync::watch::Sender<bool>,
	task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct Permit(
	Either<tokio::sync::OwnedSemaphorePermit, tokio::sync::OwnedMutexGuard<Option<Self>>>,
);

#[derive(Debug)]
struct OAuth {
	github: Option<oauth2::basic::BasicClient>,
}

#[cfg(target_os = "linux")]
#[derive(Clone)]
struct RuntimeArtifactIds {
	env: tg::file::Id,
	sh: tg::file::Id,
}

#[derive(Clone)]
struct Http {
	inner: Arc<HttpInner>,
}

struct HttpInner {
	tg: Box<dyn tg::Handle>,
	task: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
	stop: tokio::sync::watch::Sender<bool>,
}

struct Tmp {
	path: PathBuf,
	preserve: bool,
}

impl Server {
	pub async fn start(options: Options) -> Result<Server> {
		// Get the address.
		let address = options.address.clone();

		// Get the path.
		let path = &options.path;

		// Ensure the path exists.
		tokio::fs::create_dir_all(path)
			.await
			.map_err(|error| error!(source = error, "failed to create the directory"))?;

		// Acquire the lockfile.
		let lockfile = tokio::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(path.join("lock"))
			.await
			.map_err(|error| error!(source = error, "failed to open the lockfile"))?;
		let ret = unsafe { libc::flock(lockfile.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
		if ret != 0 {
			return Err(error!(
				source = std::io::Error::last_os_error(),
				"failed to acquire the lockfile"
			));
		}
		let lockfile = std::sync::Mutex::new(Some(lockfile));

		// Migrate the path.
		Self::migrate(path).await?;

		// Write the PID file.
		tokio::fs::write(&path.join("pid"), std::process::id().to_string())
			.await
			.map_err(|error| error!(source = error, "failed to write the PID file"))?;

		// Remove an existing socket file.
		rmrf(&path.join("socket"))
			.await
			.map_err(|error| error!(source = error, "failed to remove an existing socket file"))?;

		// Create the build queue task.
		let build_queue_task = std::sync::Mutex::new(None);

		// Create the build queue task stop channel.
		let (build_queue_task_stop, build_queue_task_stop_receiver) =
			tokio::sync::watch::channel(false);

		// Create the build state.
		let build_state = std::sync::RwLock::new(HashMap::default());

		// Create the build semaphore.
		let build_semaphore = Arc::new(tokio::sync::Semaphore::new(options.build.max_concurrency));

		// Create the database.
		let database = match &options.database {
			self::options::Database::Sqlite(sqlite) => {
				let database_path = path.join("database");
				Database::new_sqlite(database_path, sqlite.max_connections).await?
			},
			self::options::Database::Postgres(postgres) => {
				Database::new_postgres(postgres.url.clone(), postgres.max_connections).await?
			},
		};

		// Create the database.
		let messenger = match &options.messenger {
			self::options::Messenger::Local => Messenger::new_channel(),
			self::options::Messenger::Nats(nats) => {
				let client = nats::connect(nats.url.to_string())
					.await
					.map_err(|error| error!(source = error, "failed to create the NATS client"))?;
				Messenger::new_nats(client)
			},
		};

		// Create the file system semaphore.
		let file_descriptor_semaphore =
			tokio::sync::Semaphore::new(options.advanced.file_descriptor_semaphore_size);

		// Create the http server.
		let http = std::sync::Mutex::new(None);

		// Create the local pool.
		let local_pool_handle = tokio_util::task::LocalPoolHandle::new(
			std::thread::available_parallelism().unwrap().get(),
		);

		// Create the oauth clients.
		let github = if let Some(oauth) = &options.oauth.github {
			let client_id = oauth2::ClientId::new(oauth.client_id.clone());
			let client_secret = oauth2::ClientSecret::new(oauth.client_secret.clone());
			let auth_url = oauth2::AuthUrl::new(oauth.auth_url.clone())
				.map_err(|error| error!(source = error, "failed to create the auth URL"))?;
			let token_url = oauth2::TokenUrl::new(oauth.token_url.clone())
				.map_err(|error| error!(source = error, "failed to create the token URL"))?;
			let oauth_client = oauth2::basic::BasicClient::new(
				client_id,
				Some(client_secret),
				auth_url,
				Some(token_url),
			);
			Some(oauth_client)
		} else {
			None
		};
		let oauth = OAuth { github };

		// Get the remote.
		let remote = options.remote.as_ref().map(|remote| remote.tg.clone_box());

		// Create the runtime artifacts placeholder map.
		#[cfg(target_os = "linux")]
		let runtime_artifacts = tokio::sync::RwLock::new(HashMap::default());

		// Create the shutdown channel.
		let (shutdown, _) = tokio::sync::watch::channel(false);

		// Create the shutdown task.
		let shutdown_task = std::sync::Mutex::new(None);

		// Create the vfs.
		let vfs = std::sync::Mutex::new(None);

		// Create the server.
		let inner = Arc::new(Inner {
			build_queue_task,
			build_queue_task_stop,
			build_semaphore,
			build_state,
			database,
			file_descriptor_semaphore,
			http,
			local_pool_handle,
			lockfile,
			messenger,
			oauth,
			options,
			remote,
			#[cfg(target_os = "linux")]
			runtime_artifacts,
			shutdown,
			shutdown_task,
			vfs,
		});
		let server = Server { inner };

		// Cancel unfinished builds.
		if let Database::Sqlite(database) = &server.inner.database {
			let connection = database.get().await?;
			let statement = r#"
				update builds
				set
					outcome = json('{"kind":"canceled"}'),
					status = 'finished'
				where status != 'finished';
			"#;
			let mut statement = connection
				.prepare_cached(statement)
				.map_err(|error| error!(source = error, "failed to prepare the query"))?;
			let params = sqlite_params![];
			statement
				.execute(params)
				.map_err(|error| error!(source = error, "failed to execute the statement"))?;
		}

		// Start the VFS if necessary and set up the checkouts directory.
		let artifacts_path = server.artifacts_path();
		self::vfs::unmount(&artifacts_path).await.ok();
		if server.inner.options.vfs.enable {
			// Create the artifacts directory.
			tokio::fs::create_dir_all(&artifacts_path)
				.await
				.map_err(|error| {
					error!(source = error, "failed to create the artifacts directory")
				})?;

			// Start the VFS server.
			let vfs = self::vfs::Server::start(&server, &artifacts_path)
				.await
				.map_err(|error| error!(source = error, "failed to start the VFS"))?;

			server.inner.vfs.lock().unwrap().replace(vfs);
		} else {
			// Remove the artifacts directory.
			rmrf(&artifacts_path).await.map_err(|error| {
				error!(source = error, "failed to remove the artifacts directory")
			})?;

			// Create a symlink from the artifacts directory to the checkouts directory.
			tokio::fs::symlink("checkouts", artifacts_path)
				.await
				.map_err(|error| error!(source = error, "failed to create a symlink from the artifacts directory to the checkouts directory"))?;
		}

		// Create and store the Linux runtime artifacts.
		#[cfg(target_os = "linux")]
		load_runtime_artifacts(&server).await?;

		// Start the build queue task.
		if server.inner.options.build.enable {
			let task = tokio::spawn({
				let server = server.clone();
				async move {
					let result = server
						.build_queue_task(build_queue_task_stop_receiver)
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
			server.inner.build_queue_task.lock().unwrap().replace(task);
		}

		// Start the http server.
		server
			.inner
			.http
			.lock()
			.unwrap()
			.replace(Http::start(&server, address));

		Ok(server)
	}

	pub fn stop(&self) {
		let server = self.clone();
		let task = tokio::spawn(async move {
			// Stop the http server.
			if let Some(http) = server.inner.http.lock().unwrap().as_ref() {
				http.stop();
			}

			// Stop the build queue task.
			server.inner.build_queue_task_stop.send_replace(true);

			// Join the http server.
			let http = server.inner.http.lock().unwrap().take();
			if let Some(http) = http {
				http.join().await?;
			}

			// Join the build queue task.
			let local_queue_task = server.inner.build_queue_task.lock().unwrap().take();
			if let Some(local_queue_task) = local_queue_task {
				local_queue_task.await.unwrap()?;
			}

			// Stop all builds.
			for context in server.inner.build_state.read().unwrap().values() {
				context.stop.send_replace(true);
			}

			// Join all builds.
			let tasks = server
				.inner
				.build_state
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
				.ok();

			// Clear all build state.
			server.inner.build_state.write().unwrap().clear();

			// Join the vfs server.
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

	#[must_use]
	pub fn artifacts_path(&self) -> PathBuf {
		self.inner.options.path.join("artifacts")
	}

	#[must_use]
	pub fn checkouts_path(&self) -> PathBuf {
		self.inner.options.path.join("checkouts")
	}

	#[must_use]
	pub fn database_path(&self) -> PathBuf {
		self.inner.options.path.join("database")
	}

	#[must_use]
	pub fn tmp_path(&self) -> PathBuf {
		self.inner.options.path.join("tmp")
	}

	fn create_tmp(&self) -> Tmp {
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		let id = uuid::Uuid::now_v7();
		let id = ENCODING.encode(&id.into_bytes());
		let path = self.tmp_path().join(id);
		let preserve = self.inner.options.advanced.preserve_temp_directories;
		Tmp { path, preserve }
	}
}

impl Http {
	fn start(tg: &dyn tg::Handle, address: tg::Address) -> Self {
		let tg = tg.clone_box();
		let task = std::sync::Mutex::new(None);
		let (stop_sender, stop_receiver) = tokio::sync::watch::channel(false);
		let stop = stop_sender;
		let inner = Arc::new(HttpInner { tg, task, stop });
		let server = Self { inner };
		let task = tokio::spawn({
			let server = server.clone();
			async move {
				match server.serve(address, stop_receiver).await {
					Ok(()) => Ok(()),
					Err(error) => {
						tracing::error!(?error);
						Err(error)
					},
				}
			}
		});
		server.inner.task.lock().unwrap().replace(task);
		server
	}

	fn stop(&self) {
		self.inner.stop.send_replace(true);
	}

	async fn join(&self) -> Result<()> {
		let task = self.inner.task.lock().unwrap().take();
		if let Some(task) = task {
			match task.await {
				Ok(result) => Ok(result),
				Err(error) if error.is_cancelled() => Ok(Ok(())),
				Err(error) => Err(error),
			}
			.unwrap()?;
		}
		Ok(())
	}

	async fn serve(
		self,
		address: tg::Address,
		mut stop: tokio::sync::watch::Receiver<bool>,
	) -> Result<()> {
		// Create the tasks.
		let mut tasks = tokio::task::JoinSet::new();

		// Create the listener.
		let listener = match &address {
			tg::Address::Unix(path) => {
				tokio_util::either::Either::Left(UnixListener::bind(path).map_err(|error| {
					error!(source = error, "failed to create the UNIX listener")
				})?)
			},
			tg::Address::Inet(inet) => tokio_util::either::Either::Right(
				TcpListener::bind(inet.to_string())
					.await
					.map_err(|error| error!(source = error, "failed to create the TCP listener"))?,
			),
		};

		tracing::info!("🚀 serving on {address:?}");

		loop {
			// Accept a new connection.
			let accept = async {
				let stream = match &listener {
					tokio_util::either::Either::Left(listener) => tokio_util::either::Either::Left(
						listener
							.accept()
							.await
							.map_err(|error| {
								error!(source = error, "failed to accept a new connection")
							})?
							.0,
					),
					tokio_util::either::Either::Right(listener) => {
						tokio_util::either::Either::Right(
							listener
								.accept()
								.await
								.map_err(|error| {
									error!(source = error, "failed to accept a new connection")
								})?
								.0,
						)
					},
				};
				Ok::<_, Error>(TokioIo::new(stream))
			};
			let stop_ = stop.wait_for(|stop| *stop).map(|_| ());
			let stream = match future::select(pin!(accept), pin!(stop_)).await {
				future::Either::Left((result, _)) => result?,
				future::Either::Right(((), _)) => {
					break;
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
					let stop = stop.wait_for(|stop| *stop).map(|_| ());
					let result = match future::select(pin!(connection), pin!(stop)).await {
						future::Either::Left((result, _)) => result,
						future::Either::Right(((), mut connection)) => {
							connection.as_mut().graceful_shutdown();
							connection.await
						},
					};
					if let Err(error) = result {
						tracing::error!(?error, "failed to serve the connection");
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
		let user = self.inner.tg.get_user_for_token(&token).await?;

		Ok(user)
	}

	async fn handle_request(
		&self,
		mut request: http::Request<Incoming>,
	) -> http::Response<Outgoing> {
		let id = tg::Id::new_uuidv7(tg::id::Kind::Request);
		request.extensions_mut().insert(id.clone());

		tracing::trace!(?id, method = ?request.method(), path = ?request.uri().path(), "received request");

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
			(http::Method::GET, ["builds", _]) => {
				self.handle_get_build_request(request).map(Some).boxed()
			},
			(http::Method::PUT, ["builds", _]) => {
				self.handle_put_build_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["builds", _, "push"]) => {
				self.handle_push_build_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["builds", _, "pull"]) => {
				self.handle_pull_build_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["builds"]) => self
				.handle_get_or_create_build_request(request)
				.map(Some)
				.boxed(),
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

			// Language
			(http::Method::POST, ["format"]) => {
				self.handle_format_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["lsp"]) => self.handle_lsp_request(request).map(Some).boxed(),

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
			(http::Method::POST, ["packages"]) => self
				.handle_publish_package_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["packages", _, "check"]) => {
				self.handle_check_package_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["packages", _, "format"]) => self
				.handle_format_package_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["runtime", "js", "doc"]) => self
				.handle_get_runtime_doc_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["packages", _, "doc"]) => self
				.handle_get_package_doc_request(request)
				.map(Some)
				.boxed(),

			// Server
			(http::Method::GET, ["health"]) => {
				self.handle_health_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["path"]) => self.handle_path_request(request).map(Some).boxed(),
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
			(http::Method::GET, ["login"]) => self
				.handle_create_oauth_url_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["oauth", _]) => self
				.handle_oauth_callback_request(request)
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

		// Add the request ID to the response.
		let key = http::HeaderName::from_static("x-tangram-request-id");
		let value = http::HeaderValue::from_str(&id.to_string()).unwrap();
		response.headers_mut().insert(key, value);

		// Add tracing for response body errors.
		let response = response.map(|body| {
			Outgoing::new(body.map_err(|error| {
				tracing::error!(?error, "response body error");
				error
			}))
		});

		tracing::trace!(?id, status = ?response.status(), "sending response");

		response
	}
}

#[async_trait]
impl tg::Handle for Server {
	fn clone_box(&self) -> Box<dyn tg::Handle> {
		Box::new(self.clone())
	}

	async fn path(&self) -> Result<Option<tg::Path>> {
		self.path().await
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

	async fn list_builds(&self, args: tg::build::ListArg) -> Result<tg::build::ListOutput> {
		self.list_builds(args).await
	}

	async fn try_get_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetArg,
	) -> Result<Option<tg::build::GetOutput>> {
		self.try_get_build(id, arg).await
	}

	async fn put_build(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		arg: &tg::build::PutArg,
	) -> Result<()> {
		self.put_build(user, id, arg).await
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

	async fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Status>>>> {
		self.try_get_build_status(id, arg, stop).await
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
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::children::Chunk>>>> {
		self.try_get_build_children(id, arg, stop).await
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
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::log::Chunk>>>> {
		self.try_get_build_log(id, arg, stop).await
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
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<Option<tg::build::Outcome>>> {
		self.try_get_build_outcome(id, arg, stop).await
	}

	async fn set_build_outcome(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<()> {
		self.set_build_outcome(user, id, outcome).await
	}

	async fn format(&self, text: String) -> Result<String> {
		self.format(text).await
	}

	async fn lsp(
		&self,
		input: Box<dyn AsyncRead + Send + Unpin + 'static>,
		output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> Result<()> {
		self.lsp(input, output).await
	}

	async fn try_get_object(&self, id: &tg::object::Id) -> Result<Option<tg::object::GetOutput>> {
		self.try_get_object(id).await
	}

	async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: &tg::object::PutArg,
	) -> Result<tg::object::PutOutput> {
		self.put_object(id, arg).await
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

	async fn check_package(&self, dependency: &tg::Dependency) -> Result<Vec<tg::Diagnostic>> {
		self.check_package(dependency).await
	}

	async fn format_package(&self, dependency: &tg::Dependency) -> Result<()> {
		self.format_package(dependency).await
	}

	async fn get_runtime_doc(&self) -> Result<serde_json::Value> {
		self.get_runtime_doc().await
	}

	async fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<serde_json::Value>> {
		self.try_get_package_doc(dependency).await
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

	async fn create_oauth_url(&self, id: &tg::Id) -> Result<Url> {
		self.create_oauth_url(id).await
	}

	async fn complete_login(&self, id: &tg::Id, code: String) -> Result<()> {
		self.complete_login(id, code).await
	}
}

impl AsRef<Path> for Tmp {
	fn as_ref(&self) -> &Path {
		&self.path
	}
}

impl Drop for Tmp {
	fn drop(&mut self) {
		if !self.preserve {
			tokio::spawn(rmrf(self.path.clone()));
		}
	}
}

#[cfg(target_os = "linux")]
async fn load_runtime_artifacts(server: &Server) -> Result<()> {
	// Load the files, returning their IDs.
	let artifact_ids = futures::future::try_join_all(
		[
			include_bytes!(concat!(
				env!("CARGO_MANIFEST_DIR"),
				"/src/runtime/linux/bin/env_aarch64_linux"
			)),
			include_bytes!(concat!(
				env!("CARGO_MANIFEST_DIR"),
				"/src/runtime/linux/bin/sh_aarch64_linux"
			)),
			include_bytes!(concat!(
				env!("CARGO_MANIFEST_DIR"),
				"/src/runtime/linux/bin/env_x86_64_linux"
			)),
			include_bytes!(concat!(
				env!("CARGO_MANIFEST_DIR"),
				"/src/runtime/linux/bin/sh_x86_64_linux"
			)),
		]
		.map(|contents| async move {
			let blob = tg::Blob::with_reader(server, contents).await?;
			let file = tg::File::builder(blob).executable(true).build();
			let id = file.id(server).await?.clone();
			Ok::<_, tangram_error::Error>(id)
		}),
	)
	.await?;

	// Insert the mappings into the server.
	*server.inner.runtime_artifacts.write().await = [
		(
			tg::Triple::arch_os("aarch64", "linux"),
			RuntimeArtifactIds {
				env: artifact_ids[0].clone(),
				sh: artifact_ids[1].clone(),
			},
		),
		(
			tg::Triple::arch_os("x86_64", "linux"),
			RuntimeArtifactIds {
				env: artifact_ids[2].clone(),
				sh: artifact_ids[3].clone(),
			},
		),
	]
	.iter()
	.cloned()
	.collect::<HashMap<_, _, _>>();

	Ok(())
}
