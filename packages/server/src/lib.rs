pub use self::options::Options;
use self::{
	database::{Database, Transaction},
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
use futures::{
	future, stream::FuturesUnordered, Future, FutureExt as _, Stream, TryFutureExt as _,
	TryStreamExt as _,
};
use http_body_util::BodyExt as _;
use hyper_util::rt::{TokioExecutor, TokioIo};
use indoc::formatdoc;
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
use tangram_database::{self as db, prelude::*};
use tokio::{
	io::{AsyncBufRead, AsyncRead, AsyncWrite, AsyncWriteExt},
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
mod tmp;
mod user;
mod util;
mod vfs;

/// A server.
#[derive(Clone)]
pub struct Server(Arc<Inner>);

pub struct Inner {
	build_queue_task: std::sync::Mutex<Option<tokio::task::JoinHandle<tg::Result<()>>>>,
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

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Status {
	Created,
	Started,
	Stopping,
	Stopped,
}

#[derive(Clone)]
struct Http<H>(Arc<HttpInner<H>>)
where
	H: tg::Handle;

struct HttpInner<H>
where
	H: tg::Handle,
{
	handle: H,
	task: std::sync::Mutex<Option<tokio::task::JoinHandle<tg::Result<()>>>>,
	stop: tokio::sync::watch::Sender<bool>,
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
		let mut lockfile = tokio::fs::OpenOptions::new()
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
		lockfile
			.set_len(0)
			.await
			.map_err(|source| tg::error!(!source, "failed to truncate the lockfile"))?;
		let pid = std::process::id();
		lockfile
			.write_all(pid.to_string().as_bytes())
			.await
			.map_err(|source| tg::error!(!source, "failed to write the pid to the lockfile"))?;
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

		// Create the build state.
		let build_state = std::sync::RwLock::new(HashMap::default());

		// Create the build semaphore.
		let permits = options
			.build
			.as_ref()
			.map(|build| build.concurrency)
			.unwrap_or_default();
		let build_semaphore = Arc::new(tokio::sync::Semaphore::new(permits));

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
		let server = Self(Arc::new(Inner {
			build_queue_task,
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
		}));

		// Cancel unfinished builds.
		if let Either::Left(database) = &server.database {
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
		if server.options.vfs {
			util::fs::rmrf(&artifacts_path)
				.await
				.map_err(|source| tg::error!(!source, %path = artifacts_path.display(), "failed to remove the artifacts directory"))?;
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

			server.vfs.lock().unwrap().replace(vfs);
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
		server.runtimes.write().unwrap().insert(triple, runtime);
		#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
		{
			let triple = "aarch64-darwin".to_owned();
			let runtime =
				self::runtime::Runtime::Darwin(self::runtime::darwin::Runtime::new(&server));
			server.runtimes.write().unwrap().insert(triple, runtime);
		}
		#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
		{
			let triple = "aarch64-linux".to_owned();
			let runtime =
				self::runtime::Runtime::Linux(self::runtime::linux::Runtime::new(&server).await?);
			server.runtimes.write().unwrap().insert(triple, runtime);
		}
		#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
		{
			let triple = "x86_64-darwin".to_owned();
			let runtime =
				self::runtime::Runtime::Darwin(self::runtime::darwin::Runtime::new(&server));
			server.runtimes.write().unwrap().insert(triple, runtime);
		}
		#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
		{
			let triple = "x86_64-linux".to_owned();
			let runtime =
				self::runtime::Runtime::Linux(self::runtime::linux::Runtime::new(&server).await?);
			server.runtimes.write().unwrap().insert(triple, runtime);
		}

		// Start the build queue task.
		if server.options.build.is_some() {
			let task = tokio::spawn({
				let server = server.clone();
				async move {
					let result = server.run_build_queue().await;
					match result {
						Ok(()) => Ok(()),
						Err(error) => {
							tracing::error!(?error);
							Err(error)
						},
					}
				}
			});
			server.build_queue_task.lock().unwrap().replace(task);
		}

		// Start the http server.
		let http = Http::start(&server, url);
		server.http.lock().unwrap().replace(http);

		// Set the status.
		server.status.send_replace(Status::Started);

		Ok(server)
	}

	pub fn stop(&self) {
		self.status.send_replace(Status::Stopping);
		let server = self.clone();
		let task = tokio::spawn(async move {
			// Stop the http server.
			if let Some(http) = server.http.lock().unwrap().as_ref() {
				http.stop();
			}

			// Join the http server.
			let http = server.http.lock().unwrap().take();
			if let Some(http) = http {
				http.join().await.ok();
			}

			// Stop and join the build queue task.
			let build_queue_task = server.build_queue_task.lock().unwrap().take();
			if let Some(build_queue_task) = build_queue_task {
				build_queue_task.abort();
				build_queue_task.await.ok();
			}

			// Stop all builds.
			for context in server.build_state.read().unwrap().values() {
				context.stop.send_replace(true);
			}

			// Join all builds.
			let tasks = server
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
			server.build_state.write().unwrap().clear();

			// Join the vfs server.
			let vfs = server.vfs.lock().unwrap().take();
			if let Some(vfs) = vfs {
				vfs.stop();
				vfs.join().await.ok();
			}

			// Remove the runtimes.
			server.runtimes.write().unwrap().clear();

			// Release the lockfile.
			let lockfile = server.lockfile.lock().unwrap().take();
			if let Some(lockfile) = lockfile {
				lockfile.set_len(0).await.ok();
			}

			// Set the status.
			server.status.send_replace(Status::Stopped);
		});
		self.stop_task.lock().unwrap().replace(task);
	}

	pub async fn join(&self) -> tg::Result<()> {
		self.status
			.subscribe()
			.wait_for(|status| *status == Status::Stopped)
			.await
			.unwrap();
		Ok(())
	}

	#[must_use]
	pub fn artifacts_path(&self) -> PathBuf {
		self.path.join("artifacts")
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
	pub fn tmp_path(&self) -> PathBuf {
		self.path.join("tmp")
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	fn start(handle: &H, url: Url) -> Self {
		let handle = handle.clone();
		let task = std::sync::Mutex::new(None);
		let (stop_sender, stop_receiver) = tokio::sync::watch::channel(false);
		let stop = stop_sender;
		let server = Self(Arc::new(HttpInner { handle, task, stop }));
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
		server.task.lock().unwrap().replace(task);
		server
	}

	fn stop(&self) {
		self.stop.send_replace(true);
	}

	async fn join(&self) -> tg::Result<()> {
		let task = self.task.lock().unwrap().take();
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
			"http+unix" => {
				let path = url.host_str().ok_or_else(|| tg::error!("invalid url"))?;
				let path = urlencoding::decode(path)
					.map_err(|source| tg::error!(!source, "invalid url"))?;
				let path = Path::new(path.as_ref());
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
		let Some(user) = self.handle.get_user(&token).await? else {
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
			(http::Method::POST, ["artifacts", _, "archive"]) => self
				.handle_archive_artifact_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["artifacts", "extract"]) => self
				.handle_extract_artifact_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["artifacts", _, "bundle"]) => self
				.handle_bundle_artifact_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["artifacts", "checkin"]) => self
				.handle_check_in_artifact_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["artifacts", _, "checkout"]) => self
				.handle_check_out_artifact_request(request)
				.map(Some)
				.boxed(),

			// Blobs.
			(http::Method::POST, ["blobs"]) => {
				self.handle_create_blob_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["blobs", _, "compress"]) => {
				self.handle_compress_blob_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["blobs", _, "decompress"]) => self
				.handle_decompress_blob_request(request)
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
			(http::Method::POST, ["builds", "dequeue"]) => {
				self.handle_dequeue_build_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["builds", _, "touch"]) => {
				self.handle_touch_build_request(request).map(Some).boxed()
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
			(http::Method::POST, ["packages", _, "check"]) => {
				self.handle_check_package_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["packages", _, "doc"]) => self
				.handle_get_package_doc_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["packages", _, "format"]) => self
				.handle_format_package_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["packages", _, "outdated"]) => self
				.handle_outdated_package_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["packages"]) => self
				.handle_publish_package_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["packages", _, "versions"]) => self
				.handle_get_package_versions_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["packages", _, "yank"]) => {
				self.handle_yank_package_request(request).map(Some).boxed()
			},

			// Runtimes.
			(http::Method::GET, ["runtimes", "js", "doc"]) => self
				.handle_get_js_runtime_doc_request(request)
				.map(Some)
				.boxed(),

			// Server.
			(http::Method::POST, ["clean"]) => {
				self.handle_server_clean_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["health"]) => {
				self.handle_server_health_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["stop"]) => {
				self.handle_server_stop_request(request).map(Some).boxed()
			},

			// Users.
			(http::Method::GET, ["user"]) => {
				self.handle_get_user_request(request).map(Some).boxed()
			},

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
	type Transaction<'a> = Transaction<'a>;

	fn archive_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::ArchiveArg,
	) -> impl Future<Output = tg::Result<tg::artifact::ArchiveOutput>> {
		self.archive_artifact(id, arg)
	}

	fn extract_artifact(
		&self,
		arg: tg::artifact::ExtractArg,
	) -> impl Future<Output = tg::Result<tg::artifact::ExtractOutput>> {
		self.extract_artifact(arg)
	}

	fn bundle_artifact(
		&self,
		id: &tg::artifact::Id,
	) -> impl Future<Output = tg::Result<tg::artifact::BundleOutput>> {
		self.bundle_artifact(id)
	}

	fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> impl Future<Output = tg::Result<tg::artifact::CheckInOutput>> {
		self.check_in_artifact(arg)
	}

	fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::CheckOutArg,
	) -> impl Future<Output = tg::Result<tg::artifact::CheckOutOutput>> {
		self.check_out_artifact(id, arg)
	}

	fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
		transaction: Option<&Self::Transaction<'_>>,
	) -> impl Future<Output = tg::Result<tg::blob::Id>> {
		self.create_blob(reader, transaction)
	}

	fn compress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::CompressArg,
	) -> impl Future<Output = tg::Result<tg::blob::CompressOutput>> {
		self.compress_blob(id, arg)
	}

	fn decompress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::DecompressArg,
	) -> impl Future<Output = tg::Result<tg::blob::DecompressOutput>> {
		self.decompress_blob(id, arg)
	}

	fn list_builds(
		&self,
		args: tg::build::ListArg,
	) -> impl Future<Output = tg::Result<tg::build::ListOutput>> {
		self.list_builds(args)
	}

	fn try_get_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetArg,
	) -> impl Future<Output = tg::Result<Option<tg::build::GetOutput>>> {
		self.try_get_build(id, arg)
	}

	fn put_build(
		&self,
		id: &tg::build::Id,
		arg: &tg::build::PutArg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_build(id, arg)
	}

	fn push_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> {
		self.push_build(id)
	}

	fn pull_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> {
		self.pull_build(id)
	}

	fn get_or_create_build(
		&self,
		arg: tg::build::GetOrCreateArg,
	) -> impl Future<Output = tg::Result<tg::build::GetOrCreateOutput>> {
		self.get_or_create_build(arg)
	}

	fn try_dequeue_build(
		&self,
		arg: tg::build::DequeueArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<Output = tg::Result<Option<tg::build::DequeueOutput>>> {
		self.try_dequeue_build(arg, stop)
	}

	fn touch_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> {
		self.touch_build(id)
	}

	fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>,
		>,
	> {
		self.try_get_build_status(id, arg, stop)
	}

	fn set_build_status(
		&self,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> impl Future<Output = tg::Result<()>> {
		self.set_build_status(id, status)
	}

	fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::children::Chunk>> + Send + 'static>,
		>,
	> {
		self.try_get_build_children(id, arg, stop)
	}

	fn add_build_child(
		&self,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<()>> {
		self.add_build_child(build_id, child_id)
	}

	fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::log::Chunk>> + Send + 'static>,
		>,
	> {
		self.try_get_build_log(id, arg, stop)
	}

	fn add_build_log(
		&self,
		build_id: &tg::build::Id,
		bytes: Bytes,
	) -> impl Future<Output = tg::Result<()>> {
		self.add_build_log(build_id, bytes)
	}

	fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<Output = tg::Result<Option<Option<tg::build::Outcome>>>> {
		self.try_get_build_outcome(id, arg, stop)
	}

	fn set_build_outcome(
		&self,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> impl Future<Output = tg::Result<()>> {
		self.set_build_outcome(id, outcome)
	}

	fn format(&self, text: String) -> impl Future<Output = tg::Result<String>> {
		self.format(text)
	}

	fn lsp(
		&self,
		input: Box<dyn AsyncBufRead + Send + Unpin + 'static>,
		output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> impl Future<Output = tg::Result<()>> {
		self.lsp(input, output)
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::GetOutput>>> {
		self.try_get_object(id)
	}

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::PutArg,
		transaction: Option<&Self::Transaction<'_>>,
	) -> impl Future<Output = tg::Result<tg::object::PutOutput>> {
		self.put_object(id, arg, transaction)
	}

	fn push_object(&self, id: &tg::object::Id) -> impl Future<Output = tg::Result<()>> {
		self.push_object(id)
	}

	fn pull_object(&self, id: &tg::object::Id) -> impl Future<Output = tg::Result<()>> {
		self.pull_object(id)
	}

	fn search_packages(
		&self,
		arg: tg::package::SearchArg,
	) -> impl Future<Output = tg::Result<tg::package::SearchOutput>> {
		self.search_packages(arg)
	}

	fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> impl Future<Output = tg::Result<Option<tg::package::GetOutput>>> {
		self.try_get_package(dependency, arg)
	}

	fn check_package(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Vec<tg::Diagnostic>>> {
		self.check_package(dependency)
	}

	fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Option<serde_json::Value>>> {
		self.try_get_package_doc(dependency)
	}

	fn format_package(&self, dependency: &tg::Dependency) -> impl Future<Output = tg::Result<()>> {
		self.format_package(dependency)
	}

	fn get_package_outdated(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<tg::package::OutdatedOutput>> {
		self.get_package_outdated(dependency)
	}

	fn publish_package(&self, id: &tg::directory::Id) -> impl Future<Output = tg::Result<()>> {
		self.publish_package(id)
	}

	fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Option<Vec<String>>>> {
		self.try_get_package_versions(dependency)
	}

	fn yank_package(&self, id: &tg::directory::Id) -> impl Future<Output = tg::Result<()>> {
		self.yank_package(id)
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

	async fn stop(&self) -> tg::Result<()> {
		self.stop();
		Ok(())
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

impl<H> std::ops::Deref for Http<H>
where
	H: tg::Handle,
{
	type Target = HttpInner<H>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
