pub use self::options::Options;
use self::{
	database::Database,
	messenger::Messenger,
	runtime::Runtime,
	util::{
		fs::rmrf,
		http::{full, get_token, Incoming, Outgoing},
	},
};
use async_nats as nats;
use bytes::Bytes;
use either::Either;
use futures::{future, stream::FuturesUnordered, FutureExt, Stream, TryFutureExt, TryStreamExt};
use http_body_util::BodyExt;
use hyper_util::rt::{TokioExecutor, TokioIo};
use indoc::formatdoc;
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
use tangram_database::{self as db, prelude::*};
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
mod util;
mod vfs;

/// A server.
#[derive(Clone)]
pub struct Server {
	inner: Arc<Inner>,
}

struct Inner {
	build_queue_task: std::sync::Mutex<Option<tokio::task::JoinHandle<tg::Result<()>>>>,
	build_queue_task_stop: tokio::sync::watch::Sender<bool>,
	build_semaphore: Arc<tokio::sync::Semaphore>,
	build_state: std::sync::RwLock<HashMap<tg::build::Id, Arc<BuildState>, fnv::FnvBuildHasher>>,
	database: Database,
	file_descriptor_semaphore: tokio::sync::Semaphore,
	http: std::sync::Mutex<Option<Http<Server>>>,
	local_pool_handle: tokio_util::task::LocalPoolHandle,
	lockfile: std::sync::Mutex<Option<tokio::fs::File>>,
	messenger: Messenger,
	#[allow(dead_code)]
	oauth: OAuth,
	options: Options,
	path: PathBuf,
	remotes: Vec<tg::Client>,
	runtimes: std::sync::RwLock<HashMap<String, Runtime>>,
	status: tokio::sync::watch::Sender<Status>,
	stop_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
	vfs: std::sync::Mutex<Option<self::vfs::Server>>,
}

#[derive(Debug)]
struct BuildState {
	permit: Arc<tokio::sync::Mutex<Option<Permit>>>,
	stop: tokio::sync::watch::Sender<bool>,
	task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Status {
	Created,
	Started,
	Stopping,
	Stopped,
}

#[derive(Debug)]
struct Permit(
	#[allow(dead_code)]
	Either<tokio::sync::OwnedSemaphorePermit, tokio::sync::OwnedMutexGuard<Option<Self>>>,
);

#[derive(Debug)]
struct OAuth {
	#[allow(dead_code)]
	github: Option<oauth2::basic::BasicClient>,
}

#[derive(Clone)]
struct Http<H>
where
	H: tg::Handle,
{
	inner: Arc<HttpInner<H>>,
}

struct HttpInner<H>
where
	H: tg::Handle,
{
	tg: H,
	task: std::sync::Mutex<Option<tokio::task::JoinHandle<tg::Result<()>>>>,
	stop: tokio::sync::watch::Sender<bool>,
}

struct Tmp {
	path: PathBuf,
	preserve: bool,
}

impl Server {
	pub async fn start(options: Options) -> tg::Result<Server> {
		// Ensure the path exists.
		tokio::fs::create_dir_all(&options.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;

		// Canonicalize the path.
		let path = tokio::fs::canonicalize(&options.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;

		// Acquire the lockfile.
		let lockfile = tokio::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(true)
			.open(path.join("lock"))
			.await
			.map_err(|source| tg::error!(!source, "failed to open the lockfile"))?;
		let ret = unsafe { libc::flock(lockfile.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
		if ret != 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to acquire the lockfile"
			));
		}
		let lockfile = std::sync::Mutex::new(Some(lockfile));

		// Migrate the path.
		Self::migrate(&path).await?;

		// Write the PID file.
		tokio::fs::write(&path.join("pid"), std::process::id().to_string())
			.await
			.map_err(|source| tg::error!(!source, "failed to write the PID file"))?;

		// Remove an existing socket file.
		rmrf(&path.join("socket"))
			.await
			.map_err(|source| tg::error!(!source, "failed to remove an existing socket file"))?;

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
		let database_options = match &options.database {
			self::options::Database::Sqlite(options) => {
				database::Options::Sqlite(db::sqlite::Options {
					path: path.join("database"),
					max_connections: options.max_connections,
				})
			},
			self::options::Database::Postgres(options) => {
				database::Options::Postgres(db::postgres::Options {
					url: options.url.clone(),
					max_connections: options.max_connections,
				})
			},
		};
		let database = Database::new(database_options)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the database"))?;

		// Create the database.
		let messenger = match &options.messenger {
			self::options::Messenger::Channel => Messenger::new_channel(),
			self::options::Messenger::Nats(nats) => {
				let client = nats::connect(nats.url.to_string())
					.await
					.map_err(|source| tg::error!(!source, "failed to create the NATS client"))?;
				Messenger::new_nats(client)
			},
		};

		// Create the file system semaphore.
		let file_descriptor_semaphore =
			tokio::sync::Semaphore::new(options.advanced.file_descriptor_semaphore_size);

		// Create the http server.
		let http = std::sync::Mutex::new(None);

		// Create the local pool handle.
		let local_pool_handle = tokio_util::task::LocalPoolHandle::new(
			std::thread::available_parallelism().unwrap().get(),
		);

		// Create the oauth clients.
		let github = if let Some(oauth) = &options.oauth.github {
			let client_id = oauth2::ClientId::new(oauth.client_id.clone());
			let client_secret = oauth2::ClientSecret::new(oauth.client_secret.clone());
			let auth_url = oauth2::AuthUrl::new(oauth.auth_url.clone())
				.map_err(|source| tg::error!(!source, "failed to create the auth URL"))?;
			let token_url = oauth2::TokenUrl::new(oauth.token_url.clone())
				.map_err(|source| tg::error!(!source, "failed to create the token URL"))?;
			let oauth_client = oauth2::basic::BasicClient::new(
				client_id,
				Some(client_secret),
				auth_url,
				Some(token_url),
			);
			let redirect_url = oauth2::RedirectUrl::new(oauth.redirect_url.clone())
				.map_err(|source| tg::error!(!source, "failed to create the redirect URL"))?;
			let oauth_client = oauth_client.set_redirect_uri(redirect_url);
			Some(oauth_client)
		} else {
			None
		};
		let oauth = OAuth { github };

		// Get the remotes.
		let remotes = options
			.remotes
			.iter()
			.map(|remote| remote.client.clone())
			.collect();

		// Create the runtimes.
		let runtimes = std::sync::RwLock::new(HashMap::default());

		// Create the status.
		let (status, _) = tokio::sync::watch::channel(Status::Created);

		// Create the stop task.
		let stop_task = std::sync::Mutex::new(None);

		// Get the URL.
		let url = options.url.clone();

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
			path,
			remotes,
			runtimes,
			status,
			stop_task,
			vfs,
		});
		let server = Server { inner };

		// Cancel unfinished builds.
		if let Database::Sqlite(database) = &server.inner.database {
			let connection = database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
			let statement = formatdoc!(
				r#"
					update builds
					set
						outcome = '{{"kind":"canceled"}}',
						status = 'finished'
					where status != 'finished';
				"#
			);
			let params = db::params![];
			connection
				.execute(statement, params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))
				.await?;
		}

		// Start the VFS if necessary and set up the checkouts directory.
		let artifacts_path = server.artifacts_path();
		self::vfs::unmount(&artifacts_path).await.ok();
		if server.inner.options.vfs.enable {
			// Create the artifacts directory.
			tokio::fs::create_dir_all(&artifacts_path)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to create the artifacts directory")
				})?;

			// Start the VFS server.
			let vfs = self::vfs::Server::start(&server, &artifacts_path)
				.await
				.map_err(|source| tg::error!(!source, "failed to start the VFS"))?;

			server.inner.vfs.lock().unwrap().replace(vfs);
		} else {
			// Remove the artifacts directory.
			rmrf(&artifacts_path).await.map_err(|source| {
				tg::error!(!source, "failed to remove the artifacts directory")
			})?;

			// Create a symlink from the artifacts directory to the checkouts directory.
			tokio::fs::symlink("checkouts", artifacts_path)
				.await
				.map_err(|source|tg::error!(!source, "failed to create a symlink from the artifacts directory to the checkouts directory"))?;
		}

		// Add the runtimes.
		let triple = "js".to_owned();
		let runtime = self::runtime::Runtime::Js(self::runtime::js::Runtime::new(&server));
		server
			.inner
			.runtimes
			.write()
			.unwrap()
			.insert(triple, runtime);
		#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
		{
			let triple = "aarch64-darwin".to_owned();
			let runtime =
				self::runtime::Runtime::Darwin(self::runtime::darwin::Runtime::new(&server));
			server
				.inner
				.runtimes
				.write()
				.unwrap()
				.insert(triple, runtime);
		}
		#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
		{
			let triple = "aarch64-linux".to_owned();
			let runtime =
				self::runtime::Runtime::Linux(self::runtime::linux::Runtime::new(&server).await?);
			server
				.inner
				.runtimes
				.write()
				.unwrap()
				.insert(triple, runtime);
		}
		#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
		{
			let triple = "x86_64-darwin".to_owned();
			let runtime =
				self::runtime::Runtime::Darwin(self::runtime::darwin::Runtime::new(&server));
			server
				.inner
				.runtimes
				.write()
				.unwrap()
				.insert(triple, runtime);
		}
		#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
		{
			let triple = "x86_64-linux".to_owned();
			let runtime =
				self::runtime::Runtime::Linux(self::runtime::linux::Runtime::new(&server).await?);
			server
				.inner
				.runtimes
				.write()
				.unwrap()
				.insert(triple, runtime);
		}

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
		let http = Http::start(&server, url);
		server.inner.http.lock().unwrap().replace(http);

		// Set the status.
		server.inner.status.send_replace(Status::Started);

		Ok(server)
	}

	pub fn stop(&self) {
		self.inner.status.send_replace(Status::Stopping);
		let server = self.clone();
		let task = tokio::spawn(async move {
			// Stop the http server.
			if let Some(http) = server.inner.http.lock().unwrap().as_ref() {
				http.stop();
			}

			// Join the http server.
			let http = server.inner.http.lock().unwrap().take();
			if let Some(http) = http {
				http.join().await.ok();
			}

			// Stop the build queue task.
			server.inner.build_queue_task_stop.send_replace(true);

			// Join the build queue task.
			let build_queue_task = server.inner.build_queue_task.lock().unwrap().take();
			if let Some(build_queue_task) = build_queue_task {
				build_queue_task.await.ok();
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

			// Clear the build state.
			server.inner.build_state.write().unwrap().clear();

			// Join the vfs server.
			let vfs = server.inner.vfs.lock().unwrap().clone();
			if let Some(vfs) = vfs {
				vfs.stop();
				vfs.join().await.ok();
			}

			// Remove the runtimes.
			server.inner.runtimes.write().unwrap().clear();

			// Release the lockfile.
			server.inner.lockfile.lock().unwrap().take();

			// Set the status.
			server.inner.status.send_replace(Status::Stopped);
		});
		self.inner.stop_task.lock().unwrap().replace(task);
	}

	pub async fn join(&self) -> tg::Result<()> {
		self.inner
			.status
			.subscribe()
			.wait_for(|status| *status == Status::Stopped)
			.await
			.unwrap();
		Ok(())
	}

	#[must_use]
	pub fn artifacts_path(&self) -> PathBuf {
		self.inner.path.join("artifacts")
	}

	#[must_use]
	pub fn checkouts_path(&self) -> PathBuf {
		self.inner.path.join("checkouts")
	}

	#[must_use]
	pub fn database_path(&self) -> PathBuf {
		self.inner.path.join("database")
	}

	#[must_use]
	pub fn tmp_path(&self) -> PathBuf {
		self.inner.path.join("tmp")
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

impl<H> Http<H>
where
	H: tg::Handle,
{
	fn start(tg: &H, url: Url) -> Self {
		let tg = tg.clone();
		let task = std::sync::Mutex::new(None);
		let (stop_sender, stop_receiver) = tokio::sync::watch::channel(false);
		let stop = stop_sender;
		let inner = Arc::new(HttpInner { tg, task, stop });
		let server = Self { inner };
		let task = tokio::spawn({
			let server = server.clone();
			async move {
				match server.serve(url, stop_receiver).await {
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

	async fn join(&self) -> tg::Result<()> {
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

	async fn serve(self, url: Url, mut stop: tokio::sync::watch::Receiver<bool>) -> tg::Result<()> {
		// Create the tasks.
		let mut tasks = tokio::task::JoinSet::new();

		// Create the listener.
		let listener = match url.scheme() {
			"unix" => {
				let path = Path::new(url.path());
				let listener = UnixListener::bind(path)
					.map_err(|source| tg::error!(!source, "failed to create the UNIX listener"))?;
				tokio_util::either::Either::Left(listener)
			},
			"http" => {
				let host = url.host().ok_or_else(|| tg::error!("invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!("invalid url"))?;
				let listener = TcpListener::bind(format!("{host}:{port}"))
					.await
					.map_err(|source| tg::error!(!source, "failed to create the TCP listener"))?;
				tokio_util::either::Either::Right(listener)
			},
			_ => {
				return Err(tg::error!("invalid url"));
			},
		};

		tracing::info!("🚀 serving on {url}");

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
					result.ok();
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
	) -> tg::Result<Option<tg::user::User>> {
		// Get the token.
		let Some(token) = get_token(request, None) else {
			return Ok(None);
		};

		// Get the user.
		let Some(user) = self.inner.tg.get_user_for_token(&token).await? else {
			return Ok(None);
		};

		Ok(Some(user))
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
			// Artifacts.
			(http::Method::POST, ["artifacts", "checkin"]) => self
				.handle_check_in_artifact_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["artifacts", _, "checkout"]) => self
				.handle_check_out_artifact_request(request)
				.map(Some)
				.boxed(),

			// Builds.
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

			// Objects.
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

			// Language.
			(http::Method::POST, ["format"]) => {
				self.handle_format_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["lsp"]) => self.handle_lsp_request(request).map(Some).boxed(),

			// Packages.
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
			(http::Method::POST, ["packages", _, "outdated"]) => self
				.handle_outdated_package_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["runtimes", "js", "doc"]) => self
				.handle_get_runtime_doc_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["packages", _, "doc"]) => self
				.handle_get_package_doc_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["packages", _, "yank"]) => {
				self.handle_yank_package_request(request).map(Some).boxed()
			},

			// Server.
			(http::Method::GET, ["health"]) => {
				self.handle_health_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["path"]) => self.handle_path_request(request).map(Some).boxed(),
			(http::Method::POST, ["clean"]) => self.handle_clean_request(request).map(Some).boxed(),
			(http::Method::POST, ["stop"]) => self.handle_stop_request(request).map(Some).boxed(),

			// Users.
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

impl tg::Handle for Server {
	async fn path(&self) -> tg::Result<Option<tg::Path>> {
		self.path().await
	}

	fn file_descriptor_semaphore(&self) -> &tokio::sync::Semaphore {
		&self.inner.file_descriptor_semaphore
	}

	async fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> tg::Result<tg::artifact::CheckInOutput> {
		self.check_in_artifact(arg).await
	}

	async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::CheckOutArg,
	) -> tg::Result<tg::artifact::CheckOutOutput> {
		self.check_out_artifact(id, arg).await
	}

	async fn list_builds(&self, args: tg::build::ListArg) -> tg::Result<tg::build::ListOutput> {
		self.list_builds(args).await
	}

	async fn try_get_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetArg,
	) -> tg::Result<Option<tg::build::GetOutput>> {
		self.try_get_build(id, arg).await
	}

	async fn put_build(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		arg: &tg::build::PutArg,
	) -> tg::Result<()> {
		self.put_build(user, id, arg).await
	}

	async fn push_build(&self, user: Option<&tg::User>, id: &tg::build::Id) -> tg::Result<()> {
		self.push_build(user, id).await
	}

	async fn pull_build(&self, id: &tg::build::Id) -> tg::Result<()> {
		self.pull_build(id).await
	}

	async fn get_or_create_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::GetOrCreateArg,
	) -> tg::Result<tg::build::GetOrCreateOutput> {
		self.get_or_create_build(user, arg).await
	}

	async fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>> {
		self.try_get_build_status(id, arg, stop).await
	}

	async fn set_build_status(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> tg::Result<()> {
		self.set_build_status(user, id, status).await
	}

	async fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::build::children::Chunk>> + Send + 'static>,
	> {
		self.try_get_build_children(id, arg, stop).await
	}

	async fn add_build_child(
		&self,
		user: Option<&tg::User>,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> tg::Result<()> {
		self.add_build_child(user, build_id, child_id).await
	}

	async fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::log::Chunk>> + Send + 'static>>
	{
		self.try_get_build_log(id, arg, stop).await
	}

	async fn add_build_log(
		&self,
		user: Option<&tg::User>,
		build_id: &tg::build::Id,
		bytes: Bytes,
	) -> tg::Result<()> {
		self.add_build_log(user, build_id, bytes).await
	}

	async fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<Option<Option<tg::build::Outcome>>> {
		self.try_get_build_outcome(id, arg, stop).await
	}

	async fn set_build_outcome(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> tg::Result<()> {
		self.set_build_outcome(user, id, outcome).await
	}

	async fn format(&self, text: String) -> tg::Result<String> {
		self.format(text).await
	}

	async fn lsp(
		&self,
		input: Box<dyn AsyncRead + Send + Unpin + 'static>,
		output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> tg::Result<()> {
		self.lsp(input, output).await
	}

	async fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::GetOutput>> {
		self.try_get_object(id).await
	}

	async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: &tg::object::PutArg,
	) -> tg::Result<tg::object::PutOutput> {
		self.put_object(id, arg).await
	}

	async fn push_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		self.push_object(id).await
	}

	async fn pull_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		self.pull_object(id).await
	}

	async fn search_packages(
		&self,
		arg: tg::package::SearchArg,
	) -> tg::Result<tg::package::SearchOutput> {
		self.search_packages(arg).await
	}

	async fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> tg::Result<Option<tg::package::GetOutput>> {
		self.try_get_package(dependency, arg).await
	}

	async fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<Option<Vec<String>>> {
		self.try_get_package_versions(dependency).await
	}

	async fn publish_package(
		&self,
		user: Option<&tg::User>,
		id: &tg::directory::Id,
	) -> tg::Result<()> {
		self.publish_package(user, id).await
	}

	async fn yank_package(
		&self,
		user: Option<&tg::User>,
		id: &tg::directory::Id,
	) -> tg::Result<()> {
		self.yank_package(user, id).await
	}

	async fn check_package(&self, dependency: &tg::Dependency) -> tg::Result<Vec<tg::Diagnostic>> {
		self.check_package(dependency).await
	}

	async fn format_package(&self, dependency: &tg::Dependency) -> tg::Result<()> {
		self.format_package(dependency).await
	}

	async fn get_package_outdated(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<tg::package::OutdatedOutput> {
		self.get_package_outdated(dependency).await
	}

	async fn get_runtime_doc(&self) -> tg::Result<serde_json::Value> {
		self.get_runtime_doc().await
	}

	async fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<Option<serde_json::Value>> {
		self.try_get_package_doc(dependency).await
	}

	async fn health(&self) -> tg::Result<tg::server::Health> {
		self.health().await
	}

	async fn clean(&self) -> tg::Result<()> {
		self.clean().await
	}

	async fn stop(&self) -> tg::Result<()> {
		self.stop();
		Ok(())
	}

	async fn create_login(&self) -> tg::Result<tg::user::Login> {
		self.create_login().await
	}

	async fn get_login(&self, id: &tg::Id) -> tg::Result<Option<tg::user::Login>> {
		self.get_login(id).await
	}

	async fn get_user_for_token(&self, token: &str) -> tg::Result<Option<tg::user::User>> {
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
		if !self.preserve {
			tokio::spawn(rmrf(self.path.clone()));
		}
	}
}
