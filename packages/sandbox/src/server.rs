use {
	crate::{Command, pty},
	dashmap::DashMap,
	futures::{FutureExt as _, future},
	std::{convert::Infallible, ops::Deref, path::Path, sync::Arc},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_uri::Uri,
	tokio::io::{AsyncRead, AsyncWrite},
};

mod get;
mod kill;
mod spawn;
mod stdio;
mod tty;
mod wait;

pub struct Arg {
	pub library_paths: Vec<std::path::PathBuf>,
	pub output_path: std::path::PathBuf,
	pub tangram_path: std::path::PathBuf,
}

#[derive(Clone)]
pub struct Server(Arc<State>);

pub struct State {
	library_paths: Vec<std::path::PathBuf>,
	output_path: std::path::PathBuf,
	processes: DashMap<tg::process::Id, Process>,
	tangram_path: std::path::PathBuf,
}

struct Process {
	command: Command,
	location: Option<tg::location::Location>,
	pid: libc::pid_t,
	retry: bool,
	stdin: Option<Arc<tokio::sync::Mutex<tokio::process::ChildStdin>>>,
	stdout: Option<Arc<tokio::sync::Mutex<tokio::process::ChildStdout>>>,
	stderr: Option<Arc<tokio::sync::Mutex<tokio::process::ChildStderr>>>,
	pty: Option<Arc<pty::Pty>>,
	task: tangram_futures::task::Shared<tg::Result<u8>>,
}

pub enum Listener {
	Unix(tokio::net::UnixListener),
	Tcp(tokio::net::TcpListener),
	#[cfg(feature = "vsock")]
	Vsock(tokio_vsock::VsockListener)
}

enum Stream {
	Unix(tokio::net::UnixStream),
	Tcp(tokio::net::TcpStream),
	#[cfg(feature = "vsock")]
	Vsock(tokio_vsock::VsockStream),
}

impl Server {
	pub fn new(arg: Arg) -> Self {
		Self(Arc::new(State {
			library_paths: arg.library_paths,
			output_path: arg.output_path,
			processes: DashMap::default(),
			tangram_path: arg.tangram_path,
		}))
	}

	pub async fn listen(url: &Uri) -> tg::Result<Listener> {
		let listener = match url.scheme() {
			Some("http+unix") => {
				let path = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let path = Path::new(path);
				let listener = tokio::net::UnixListener::bind(path).map_err(
					|source| tg::error!(!source, path = %path.display(), "failed to bind"),
				)?;
				Listener::Unix(listener)
			},
			Some("http") => {
				let host = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let listener = tokio::net::TcpListener::bind((host, port))
					.await
					.map_err(|source| tg::error!(!source, "failed to bind"))?;
				Listener::Tcp(listener)
			},
			Some("http+vsock") => {
				#[cfg(not(feature = "vsock"))]
				{
					return Err(tg::error!("vsock is not enabled"));
				}
				#[cfg(feature = "vsock")]
				{
					let cid = url
						.host()
						.ok_or_else(|| tg::error!(%url, "invalid url"))?
						.parse::<u32>()
						.map_err(|source| tg::error!(!source, %url, "invalid url"))?;
					let port = url.port().ok_or_else(|| tg::error!(%url, "invalid url"))?;
					let addr = tokio_vsock::VsockAddr::new(cid, u32::from(port));
					let listener = tokio_vsock::VsockListener::bind(addr)
						.map_err(|source| tg::error!(!source, "failed to bind"))?;
					Listener::Vsock(listener)
				}
			},
			_ => return Err(tg::error!(%url, "invalid url")),
		};
		Ok(listener)
	}

	pub async fn serve(&self, listener: Listener) {
		loop {
			let stream = match &listener {
				Listener::Unix(listener) => {
					let Ok((stream, _)) = listener.accept().await else {
						continue;
					};
					Stream::Unix(stream)
				},
				Listener::Tcp(listener) => {
					let Ok((stream, _)) = listener.accept().await else {
						continue;
					};
					Stream::Tcp(stream)
				},
				#[cfg(feature = "vsock")]
				Listener::Vsock(listener) => {
					let Ok((stream, _)) = listener.accept().await else {
						continue;
					};
					Stream::Vsock(stream)
				},
			};
			let server = self.clone();
			tokio::spawn(async move {
				match stream {
					Stream::Unix(stream) => server.serve_stream(stream).await,
					Stream::Tcp(stream) => server.serve_stream(stream).await,
					#[cfg(feature = "vsock")]
					Stream::Vsock(stream) => server.serve_stream(stream).await,
				}
			});
		}
	}

	pub async fn serve_url(&self, url: &Uri) -> tg::Result<()> {
		let stream = match url.scheme() {
			Some("http+unix") => {
				let path = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				Stream::Unix(
					tokio::net::UnixStream::connect(path)
						.await
						.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?,
				)
			},
			Some("http") => {
				let host = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!(%url, "invalid url"))?;
				Stream::Tcp(
					tokio::net::TcpStream::connect((host, port))
						.await
						.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?,
				)
			},
			Some("http+vsock") => {
				#[cfg(not(feature = "vsock"))]
				{
					return Err(tg::error!("vsock is not enabled"));
				}
				#[cfg(feature = "vsock")]
				{
					let cid = url
						.host()
						.ok_or_else(|| tg::error!(%url, "invalid url"))?
						.parse::<u32>()
						.map_err(|source| tg::error!(!source, %url, "invalid url"))?;
					let port = url.port().ok_or_else(|| tg::error!(%url, "invalid url"))?;
					let addr = tokio_vsock::VsockAddr::new(cid, u32::from(port));
					Stream::Vsock(
						tokio_vsock::VsockStream::connect(addr)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to connect to the socket")
							})?,
					)
				}
			},
			_ => return Err(tg::error!(%url, "invalid url")),
		};
		match stream {
			Stream::Unix(stream) => self.serve_stream(stream).await,
			Stream::Tcp(stream) => self.serve_stream(stream).await,
			#[cfg(feature = "vsock")]
			Stream::Vsock(stream) => self.serve_stream(stream).await,
		}
		Ok(())
	}

	pub async fn serve_stream<S>(&self, stream: S)
	where
		S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
	{
		let service = tower::service_fn({
			let server = self.clone();
			move |request: http::Request<hyper::body::Incoming>| {
				let server = server.clone();
				async move {
					let request = request.boxed_body();
					let response = server.handle_request(request).await;
					Ok::<_, Infallible>(response)
				}
			}
		});
		let service = hyper_util::service::TowerToHyperService::new(service);
		let stream = hyper_util::rt::TokioIo::new(stream);
		let mut builder =
			hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
		builder
			.http2()
			.max_concurrent_streams(None)
			.max_pending_accept_reset_streams(None)
			.max_local_error_reset_streams(None);
		let result = builder
			.serve_connection_with_upgrades(stream, service)
			.await;
		result
			.inspect_err(|error| {
				tracing::trace!(?error, "connection failed");
			})
			.ok();
	}

	async fn handle_request(&self, request: http::Request<BoxBody>) -> http::Response<BoxBody> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		let response = match (method, path_components.as_slice()) {
			(http::Method::POST, ["processes", "spawn"]) => {
				self.handle_spawn_request(request).boxed()
			},
			(http::Method::GET, ["processes", process]) => {
				self.handle_get_process_request(request, process).boxed()
			},
			(http::Method::GET, ["processes", process, "stdio"]) => {
				self.handle_read_stdio_request(request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "stdio"]) => {
				self.handle_write_stdio_request(request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "tty", "size"]) => {
				self.handle_set_tty_size_request(request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "kill"]) => {
				self.handle_kill_request(request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "wait"]) => {
				self.handle_wait_request(request, process).boxed()
			},
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
