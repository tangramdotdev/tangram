use {
	std::{ops::Deref, path::Path, sync::Arc, time::Duration},
	tangram_client::prelude::*,
	tangram_http::request::Ext as _,
	tangram_uri::Uri,
	tokio::io::{AsyncRead, AsyncWrite},
	tower::{Service as _, util::BoxCloneSyncService},
};

pub mod get;
pub mod kill;
pub mod spawn;
pub mod stdio;
pub mod tty;
pub mod wait;

#[derive(Clone)]
pub struct Client(Arc<State>);

pub struct State {
	service: Service,
}

type Sender = Arc<
	tokio::sync::Mutex<Option<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>>>,
>;

type Service = BoxCloneSyncService<
	http::Request<tangram_http::body::Boxed>,
	http::Response<tangram_http::body::Boxed>,
	Error,
>;

#[derive(Debug, derive_more::Display, derive_more::Error)]
enum Error {
	Hyper(hyper::Error),
	Other(tg::Error),
}

impl Client {
	#[expect(dead_code)]
	pub fn new(url: &Uri) -> Self {
		let sender = Arc::new(tokio::sync::Mutex::new(None));
		let service = Self::service(url.clone(), sender);
		Self(Arc::new(State { service }))
	}

	pub async fn with_listener(listener: &crate::server::Listener) -> tg::Result<Self> {
		match listener {
			crate::server::Listener::Tcp(listener) => {
				let (stream, _) = listener
					.accept()
					.await
					.map_err(|source| tg::error!(!source, "failed to accept the connection"))?;
				Self::with_stream(stream).await
			},
			crate::server::Listener::Unix(listener) => {
				let (stream, _) = listener
					.accept()
					.await
					.map_err(|source| tg::error!(!source, "failed to accept the connection"))?;
				Self::with_stream(stream).await
			},
			#[cfg(feature = "vsock")]
			crate::server::Listener::Vsock(listener) => {
				let (stream, _) = listener
					.accept()
					.await
					.map_err(|source| tg::error!(!source, "failed to accept the connection"))?;
				Self::with_stream(stream).await
			},
		}
	}

	pub async fn with_stream<S>(stream: S) -> tg::Result<Self>
	where
		S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
	{
		let sender = Self::handshake_h2(stream).await?;
		let sender = Arc::new(tokio::sync::Mutex::new(Some(sender)));
		let url = Uri::builder()
			.scheme("http+stdio")
			.path("")
			.build()
			.map_err(|source| tg::error!(!source, "failed to build the URL"))?;
		let service = Self::service(url, sender);
		let client = Self(Arc::new(State { service }));
		Ok(client)
	}

	fn service(url: Uri, sender: Sender) -> Service {
		let service = tower::service_fn(move |request| {
			let sender = sender.clone();
			let url = url.clone();
			async move {
				let mut guard = sender.lock().await;
				let mut sender = match guard.as_ref() {
					Some(sender) if sender.is_ready() => sender.clone(),
					_ => {
						let sender = Self::connect_h2(&url).await.map_err(Error::Other)?;
						guard.replace(sender.clone());
						sender
					},
				};
				drop(guard);
				sender
					.send_request(request)
					.await
					.map(tangram_http::response::Ext::boxed_body)
					.map_err(Error::Hyper)
			}
		});
		let service = tower::ServiceBuilder::new()
			.layer(tangram_http::layer::tracing::TracingLayer::new())
			.layer(tower::layer::layer_fn(|service| {
				let service = Service::new(service);
				tower::service_fn(move |request| {
					let future = service.clone().call(request);
					async move {
						match tokio::time::timeout(Duration::from_mins(1), future).await {
							Ok(result) => result,
							Err(_) => Err(Error::Other(tg::error!("request timed out"))),
						}
					}
				})
			}))
			.service(service);
		Service::new(service)
	}

	async fn connect_h2(
		url: &Uri,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>> {
		match url.scheme() {
			Some("http") => {
				let host = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let stream = Self::connect_tcp(host, port).await?;
				Self::handshake_h2(stream).await
			},
			Some("http+unix") => {
				let path = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let stream = Self::connect_unix(Path::new(path)).await?;
				Self::handshake_h2(stream).await
			},
			Some("http+vsock") => {
				#[cfg(not(feature = "vsock"))]
				{
					Err(tg::error!("vsock is not enabled"))
				}
				#[cfg(feature = "vsock")]
				{
					let cid = url
						.host()
						.ok_or_else(|| tg::error!(%url, "invalid url"))?
						.parse::<u32>()
						.map_err(|source| tg::error!(!source, %url, "invalid url"))?;
					let port = url.port().ok_or_else(|| tg::error!(%url, "invalid url"))?;
					let stream = Self::connect_vsock(cid, u32::from(port)).await?;
					Self::handshake_h2(stream).await
				}
			},
			_ => Err(tg::error!(%url, "invalid url")),
		}
	}

	async fn connect_unix(path: &Path) -> tg::Result<tokio::net::UnixStream> {
		tokio::net::UnixStream::connect(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))
	}

	async fn connect_tcp(host: &str, port: u16) -> tg::Result<tokio::net::TcpStream> {
		tokio::net::TcpStream::connect((host, port))
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))
	}

	#[cfg(feature = "vsock")]
	async fn connect_vsock(cid: u32, port: u32) -> tg::Result<tokio_vsock::VsockStream> {
		tokio_vsock::VsockStream::connect(tokio_vsock::VsockAddr::new(cid, port))
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))
	}

	async fn handshake_h2<S>(
		stream: S,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>>
	where
		S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
	{
		// Perform the HTTP handshake.
		let executor = hyper_util::rt::TokioExecutor::new();
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http2::Builder::new(executor)
			.max_concurrent_streams(None)
			.max_concurrent_reset_streams(usize::MAX)
			.handshake(io)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the HTTP handshake"))?;

		// Spawn the connection.
		tokio::spawn(async move {
			connection
				.await
				.inspect_err(|error| {
					tracing::error!(error = ?error, "the connection failed");
				})
				.ok();
		});

		// Wait for the sender to be ready.
		sender
			.ready()
			.await
			.map_err(|source| tg::error!(!source, "failed to ready the sender"))?;

		Ok(sender)
	}

	pub(crate) async fn send<B>(
		&self,
		request: http::Request<B>,
	) -> tg::Result<http::Response<tangram_http::body::Boxed>>
	where
		B: http_body::Body<Data = bytes::Bytes> + Send + Unpin + 'static,
		B::Error: Into<tangram_http::Error> + Send,
	{
		let request = request.boxed_body();
		let future = self.service.clone().call(request);
		let response = future.await.map_err(|error| match error {
			Error::Hyper(source) => tg::error!(!source, "failed to send the request"),
			Error::Other(error) => error,
		})?;
		let response = response.map(Into::into);
		Ok(response)
	}
}

impl Deref for Client {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
