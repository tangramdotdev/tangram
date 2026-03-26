use {
	crate::common,
	dashmap::DashMap,
	futures::{FutureExt as _, future},
	std::{convert::Infallible, net::SocketAddr, ops::Deref, path::Path, sync::Arc},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tokio::sync::Mutex,
};

mod kill;
mod spawn;
mod stdio;
mod tty;
mod wait;

#[derive(Clone)]
pub struct Server(Arc<State>);

pub struct State {
	#[cfg(target_os = "linux")]
	pub(crate) pids: DashMap<libc::pid_t, tg::process::Id>,
	pub(crate) processes: DashMap<tg::process::Id, ChildProcess>,
	pub(crate) stdio: DashMap<tg::process::Id, ChildStdio>,
}

pub(crate) struct ChildProcess {
	#[cfg(target_os = "macos")]
	pub(crate) child: tokio::process::Child,
	#[cfg(target_os = "linux")]
	pub(crate) notify: Arc<tokio::sync::Notify>,
	#[cfg(target_os = "linux")]
	pub(crate) pid: libc::pid_t,
	pub(crate) status: Option<u8>,
}

pub(crate) struct ChildStdio {
	pub(crate) stdin: Mutex<common::InputStream>,
	pub(crate) stdout: Mutex<common::OutputStream>,
	pub(crate) stderr: Mutex<common::OutputStream>,
	pub(crate) pty: Option<Mutex<common::Pty>>,
}

pub(crate) type Listener =
	tokio_util::either::Either<tokio::net::UnixListener, tokio::net::TcpListener>;

impl Server {
	pub fn new() -> Self {
		let server = Self(Arc::new(State {
			#[cfg(target_os = "linux")]
			pids: DashMap::default(),
			processes: DashMap::default(),
			stdio: DashMap::default(),
		}));

		#[cfg(target_os = "linux")]
		{
			tokio::spawn({
				let server = server.clone();
				async move {
					server
						.reaper_task()
						.await
						.inspect_err(|error| tracing::error!(?error, "failed to reap children"))
						.ok();
				}
			});
		}

		server
	}

	pub(crate) fn listen(
		addr: &tokio_util::either::Either<&Path, SocketAddr>,
	) -> tg::Result<(Listener, u16)> {
		match addr {
			tokio_util::either::Either::Left(path) => {
				let listener = tokio::net::UnixListener::bind(path).map_err(
					|source| tg::error!(!source, path = %path.display(), "failed to bind"),
				)?;
				Ok((tokio_util::either::Either::Left(listener), 0))
			},
			tokio_util::either::Either::Right(addr) => {
				let listener = std::net::TcpListener::bind(addr)
					.map_err(|source| tg::error!(!source, "failed to bind"))?;
				listener
					.set_nonblocking(true)
					.map_err(|source| tg::error!(!source, "failed to set nonblocking mode"))?;
				let port = listener
					.local_addr()
					.map_err(|source| tg::error!(!source, "failed to get the local address"))?
					.port();
				let listener = tokio::net::TcpListener::from_std(listener)
					.map_err(|source| tg::error!(!source, "failed to create the tcp listener"))?;
				Ok((tokio_util::either::Either::Right(listener), port))
			},
		}
	}

	pub(crate) async fn serve(&self, listener: Listener) {
		loop {
			let stream = match &listener {
				tokio_util::either::Either::Left(unix) => {
					let Ok((stream, _)) = unix.accept().await else {
						continue;
					};
					tokio_util::either::Either::Left(stream)
				},
				tokio_util::either::Either::Right(tcp) => {
					let Ok((stream, _)) = tcp.accept().await else {
						continue;
					};
					tokio_util::either::Either::Right(stream)
				},
			};
			let server = self.clone();
			tokio::spawn(async move {
				let service =
					tower::service_fn(move |request: http::Request<hyper::body::Incoming>| {
						let server = server.clone();
						async move {
							let request = request.boxed_body();
							let response = server.handle_request(request).await;
							Ok::<_, Infallible>(response)
						}
					});
				let service = hyper_util::service::TowerToHyperService::new(service);
				let stream = hyper_util::rt::TokioIo::new(stream);
				let mut builder = hyper_util::server::conn::auto::Builder::new(
					hyper_util::rt::TokioExecutor::new(),
				);
				builder
					.http2()
					.max_concurrent_streams(None)
					.max_pending_accept_reset_streams(None)
					.max_local_error_reset_streams(None);
				let _ = builder
					.serve_connection_with_upgrades(stream, service)
					.await;
			});
		}
	}

	async fn handle_request(&self, request: http::Request<BoxBody>) -> http::Response<BoxBody> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		let response = match (method, path_components.as_slice()) {
			(http::Method::POST, ["spawn"]) => self.handle_spawn_request(request).boxed(),
			(http::Method::GET, ["processes", process, "stdio"]) => {
				self.handle_read_stdio_request(request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "stdio"]) => {
				self.handle_write_stdio_request(request, process).boxed()
			},
			(http::Method::POST, ["tty", "size"]) => {
				self.handle_set_tty_size_request(request).boxed()
			},
			(http::Method::POST, ["kill"]) => self.handle_kill_request(request).boxed(),
			(http::Method::POST, ["wait"]) => self.handle_wait_request(request).boxed(),
			(_, _) => future::ok(
				http::Response::builder()
					.status(http::StatusCode::NOT_FOUND)
					.bytes("not found")
					.unwrap()
					.boxed_body(),
			)
			.boxed(),
		}
		.await;
		response.unwrap_or_else(|error| {
			tracing::error!(error = %error.trace());
			let bytes = error
				.state()
				.object()
				.and_then(|object| serde_json::to_string(&object.unwrap_error_ref().to_data()).ok())
				.unwrap_or_default();
			http::Response::builder()
				.status(http::StatusCode::INTERNAL_SERVER_ERROR)
				.bytes(bytes)
				.unwrap()
				.boxed_body()
		})
	}
}

impl Deref for Server {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
