use self::{
	database::{Database, Transaction},
	messenger::Messenger,
	runtime::Runtime,
	util::{
		fs::rmrf,
		http::{full, Incoming, Outgoing},
	},
};
use async_nats as nats;
use bytes::Bytes;
use dashmap::DashMap;
use either::Either;
use futures::{
	future::{self, BoxFuture},
	Future, FutureExt as _, Stream, TryFutureExt as _,
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
mod package;
mod root;
mod runtime;
mod server;
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
	checkouts: Checkouts,
	database: Database,
	file_descriptor_semaphore: tokio::sync::Semaphore,
	local_pool_handle: tokio_util::task::LocalPoolHandle,
	lockfile: std::sync::Mutex<Option<tokio::fs::File>>,
	messenger: Messenger,
	#[allow(dead_code)]
	oauth: OAuth,
	options: Options,
	path: PathBuf,
	remotes: Vec<tg::Client>,
	runtimes: std::sync::RwLock<HashMap<String, Runtime>>,
	vfs: std::sync::Mutex<Option<self::vfs::Server>>,
}

type BuildPermits =
	DashMap<tg::build::Id, Arc<tokio::sync::Mutex<Option<BuildPermit>>>, fnv::FnvBuildHasher>;

struct BuildPermit(
	#[allow(dead_code)]
	Either<tokio::sync::OwnedSemaphorePermit, tokio::sync::OwnedMutexGuard<Option<Self>>>,
);

type Checkouts = DashMap<tg::artifact::Id, CheckoutFuture, fnv::FnvBuildHasher>;

type CheckoutFuture =
	future::Shared<BoxFuture<'static, tg::Result<tg::artifact::checkout::Output>>>;

#[derive(Debug)]
struct OAuth {
	#[allow(dead_code)]
	github: Option<oauth2::basic::BasicClient>,
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

		// Create the build permits.
		let build_permits = DashMap::default();

		// Create the build semaphore.
		let permits = options
			.build
			.as_ref()
			.map(|build| build.concurrency)
			.unwrap_or_default();
		let build_semaphore = Arc::new(tokio::sync::Semaphore::new(permits));

		// Create the checkouts.
		let checkouts = DashMap::default();

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

		// Get the URL.
		let url = options.url.clone();

		// Create the vfs.
		let vfs = std::sync::Mutex::new(None);

		// Create the server.
		let server = Self(Arc::new(Inner {
			build_permits,
			build_semaphore,
			checkouts,
			database,
			file_descriptor_semaphore,
			local_pool_handle,
			lockfile,
			messenger,
			oauth,
			options,
			path,
			remotes,
			runtimes,
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
		let kind = if cfg!(target_os = "macos") {
			vfs::Kind::Nfs
		} else if cfg!(target_os = "linux") {
			vfs::Kind::Fuse
		} else {
			unreachable!()
		};
		if server.options.vfs {
			// Start the VFS server.
			let vfs = self::vfs::Server::start(&server, kind, &artifacts_path)
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

		// Start the build queue.
		if server.options.build.is_some() {
			tokio::spawn({
				let server = server.clone();
				async move { server.run_build_queue().await }
			});
		}

		// Start the http task.
		let (_, http_stop_receiver) = tokio::sync::watch::channel(false);
		tokio::spawn(Self::serve(server.clone(), url, http_stop_receiver));

		Ok(server)
	}

	pub fn stop(&self) {
		let server = self.clone();
		tokio::spawn(async move {
			// Stop the database.
			server.runtimes.write().unwrap().clear();

			// Remove the runtimes.
			server.runtimes.write().unwrap().clear();

			// Release the lockfile.
			let lockfile = server.lockfile.lock().unwrap().take();
			if let Some(lockfile) = lockfile {
				lockfile.set_len(0).await.ok();
			}
		});
	}

	pub async fn join(&self) {}

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

impl Server {
	async fn serve<H>(
		handle: H,
		url: Url,
		mut stop: tokio::sync::watch::Receiver<bool>,
	) -> tg::Result<()>
	where
		H: tg::Handle,
	{
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
			let stop_ = stop.wait_for(|stop| *stop).map(|_| ());
			let stream = match future::select(pin!(accept), pin!(stop_)).await {
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
		let path_components = request.uri().path().split('/').skip(1).collect_vec();
		let response = match (method, path_components.as_slice()) {
			// Artifacts.
			(http::Method::POST, ["artifacts", _, "archive"]) => {
				Self::handle_archive_artifact_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["artifacts", "extract"]) => {
				Self::handle_extract_artifact_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["artifacts", _, "bundle"]) => {
				Self::handle_bundle_artifact_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["artifacts", "checkin"]) => {
				Self::handle_check_in_artifact_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["artifacts", _, "checkout"]) => {
				Self::handle_check_out_artifact_request(handle, request)
					.map(Some)
					.boxed()
			},

			// Blobs.
			(http::Method::POST, ["blobs"]) => Self::handle_create_blob_request(handle, request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["blobs", _, "compress"]) => {
				Self::handle_compress_blob_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["blobs", _, "decompress"]) => {
				Self::handle_decompress_blob_request(handle, request)
					.map(Some)
					.boxed()
			},

			// Builds.
			(http::Method::GET, ["builds"]) => Self::handle_list_builds_request(handle, request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["builds", _]) => Self::handle_get_build_request(handle, request)
				.map(Some)
				.boxed(),
			(http::Method::PUT, ["builds", _]) => Self::handle_put_build_request(handle, request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["builds", _, "push"]) => {
				Self::handle_push_build_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["builds", _, "pull"]) => {
				Self::handle_pull_build_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["builds"]) => Self::handle_create_build_request(handle, request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["builds", "dequeue"]) => {
				Self::handle_dequeue_build_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["builds", _, "start"]) => {
				Self::handle_start_build_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::GET, ["builds", _, "status"]) => {
				Self::handle_get_build_status_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::GET, ["builds", _, "children"]) => {
				Self::handle_get_build_children_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["builds", _, "children"]) => {
				Self::handle_add_build_child_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::GET, ["builds", _, "log"]) => {
				Self::handle_get_build_log_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["builds", _, "log"]) => {
				Self::handle_add_build_log_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::GET, ["builds", _, "outcome"]) => {
				Self::handle_get_build_outcome_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["builds", _, "finish"]) => {
				Self::handle_finish_build_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["builds", _, "touch"]) => {
				Self::handle_touch_build_request(handle, request)
					.map(Some)
					.boxed()
			},

			// Objects.
			(http::Method::GET, ["objects", _]) => Self::handle_get_object_request(handle, request)
				.map(Some)
				.boxed(),
			(http::Method::PUT, ["objects", _]) => Self::handle_put_object_request(handle, request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["objects", _, "push"]) => {
				Self::handle_push_object_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["objects", _, "pull"]) => {
				Self::handle_pull_object_request(handle, request)
					.map(Some)
					.boxed()
			},

			// Language.
			(http::Method::POST, ["format"]) => Self::handle_format_request(handle, request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["lsp"]) => {
				Self::handle_lsp_request(handle, request).map(Some).boxed()
			},

			// Packages.
			(http::Method::GET, ["packages"]) => {
				Self::handle_list_packages_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::GET, ["packages", _]) => {
				Self::handle_get_package_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["packages", _, "check"]) => {
				Self::handle_check_package_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::GET, ["packages", _, "doc"]) => {
				Self::handle_get_package_doc_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["packages", _, "format"]) => {
				Self::handle_format_package_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["packages", _, "outdated"]) => {
				Self::handle_outdated_package_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["packages"]) => {
				Self::handle_publish_package_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::GET, ["packages", _, "versions"]) => {
				Self::handle_get_package_versions_request(handle, request)
					.map(Some)
					.boxed()
			},
			(http::Method::POST, ["packages", _, "yank"]) => {
				Self::handle_yank_package_request(handle, request)
					.map(Some)
					.boxed()
			},

			// Roots.
			(http::Method::GET, ["roots"]) => Self::handle_list_roots_request(handle, request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["roots", _]) => Self::handle_get_root_request(handle, request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["roots"]) => Self::handle_add_root_request(handle, request)
				.map(Some)
				.boxed(),
			(http::Method::DELETE, ["roots", _]) => {
				Self::handle_remove_root_request(handle, request)
					.map(Some)
					.boxed()
			},

			// Runtimes.
			(http::Method::GET, ["runtimes", "js", "doc"]) => {
				Self::handle_get_js_runtime_doc_request(handle, request)
					.map(Some)
					.boxed()
			},

			// Server.
			(http::Method::POST, ["clean"]) => Self::handle_server_clean_request(handle, request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["health"]) => Self::handle_server_health_request(handle, request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["stop"]) => Self::handle_server_stop_request(handle, request)
				.map(Some)
				.boxed(),

			// Users.
			(http::Method::GET, ["user"]) => Self::handle_get_user_request(handle, request)
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
	type Transaction<'a> = Transaction<'a>;

	fn archive_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::archive::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::archive::Output>> {
		self.archive_artifact(id, arg)
	}

	fn extract_artifact(
		&self,
		arg: tg::artifact::extract::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::extract::Output>> {
		self.extract_artifact(arg)
	}

	fn bundle_artifact(
		&self,
		id: &tg::artifact::Id,
	) -> impl Future<Output = tg::Result<tg::artifact::bundle::Output>> {
		self.bundle_artifact(id)
	}

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
		transaction: Option<&Self::Transaction<'_>>,
	) -> impl Future<Output = tg::Result<tg::blob::Id>> {
		self.create_blob(reader, transaction)
	}

	fn compress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::compress::Arg,
	) -> impl Future<Output = tg::Result<tg::blob::compress::Output>> {
		self.compress_blob(id, arg)
	}

	fn decompress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::decompress::Arg,
	) -> impl Future<Output = tg::Result<tg::blob::decompress::Output>> {
		self.decompress_blob(id, arg)
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

	fn push_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> {
		self.push_build(id)
	}

	fn pull_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> {
		self.pull_build(id)
	}

	fn create_build(
		&self,
		arg: tg::build::create::Arg,
	) -> impl Future<Output = tg::Result<tg::build::create::Output>> {
		self.create_build(arg)
	}

	fn try_dequeue_build(
		&self,
		arg: tg::build::dequeue::Arg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<Output = tg::Result<Option<tg::build::dequeue::Output>>> {
		self.try_dequeue_build(arg, stop)
	}

	fn try_start_build(
		&self,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<Option<bool>>> {
		self.try_start_build(id)
	}

	fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::Arg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>,
		>,
	> {
		self.try_get_build_status(id, arg, stop)
	}

	fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::Arg,
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
		arg: tg::build::log::Arg,
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
		arg: tg::build::outcome::Arg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<Output = tg::Result<Option<Option<tg::build::Outcome>>>> {
		self.try_get_build_outcome(id, arg, stop)
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

	fn push_object(&self, id: &tg::object::Id) -> impl Future<Output = tg::Result<()>> {
		self.push_object(id)
	}

	fn pull_object(&self, id: &tg::object::Id) -> impl Future<Output = tg::Result<()>> {
		self.pull_object(id)
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
	) -> impl Future<Output = tg::Result<tg::package::outdated::Output>> {
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

	fn put_root(&self, arg: tg::root::add::Arg) -> impl Future<Output = tg::Result<()>> {
		self.add_root(arg)
	}

	fn remove_root(&self, name: &str) -> impl Future<Output = tg::Result<()>> {
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
