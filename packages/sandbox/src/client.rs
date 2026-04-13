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
	sender: Sender,
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
	pub fn new(url: &Uri) -> tg::Result<Self> {
		match url.scheme() {
			Some("http+unix") => {
				url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
			},
			Some("http") => {
				url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				url.port_or_known_default()
					.ok_or_else(|| tg::error!(%url, "invalid url"))?;
			},
			Some("http+vsock") => {
				#[cfg(not(feature = "vsock"))]
				{
					return Err(tg::error!("vsock is not enabled"));
				}
				#[cfg(feature = "vsock")]
				{
					url.host()
						.ok_or_else(|| tg::error!(%url, "invalid url"))?
						.parse::<u32>()
						.map_err(|source| tg::error!(!source, %url, "invalid url"))?;
					url.port().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				}
			},
			_ => return Err(tg::error!(%url, "invalid url")),
		}
		let sender = Arc::new(tokio::sync::Mutex::new(None));
		let url = Some(url.clone());
		let service = Self::service(sender.clone(), url.clone());
		Ok(Self(Arc::new(State { sender, service })))
	}

	pub async fn with_listener(listener: &crate::server::Listener) -> tg::Result<Self> {
		let sender = Arc::new(tokio::sync::Mutex::new(None));
		let service = Self::service(sender.clone(), None);
		let client = Self(Arc::new(State { sender, service }));
		match listener {
			crate::server::Listener::Unix(listener) => {
				let (stream, _) = listener
					.accept()
					.await
					.map_err(|source| tg::error!(!source, "failed to accept the connection"))?;
				let sender = Self::handshake_h2(stream).await?;
				let mut guard = client.sender.lock().await;
				guard.replace(sender);
			},
			crate::server::Listener::Tcp(listener) => {
				let (stream, _) = listener
					.accept()
					.await
					.map_err(|source| tg::error!(!source, "failed to accept the connection"))?;
				let sender = Self::handshake_h2(stream).await?;
				let mut guard = client.sender.lock().await;
				guard.replace(sender);
			},
		}
		Ok(client)
	}

	fn service(sender: Sender, url: Option<Uri>) -> Service {
		let service = tower::service_fn(move |request| {
			let sender = sender.clone();
			let url = url.clone();
			async move {
				let mut guard = sender.lock().await;
				let mut sender = match guard.as_ref() {
					Some(sender) if sender.is_ready() => sender.clone(),
					_ => {
						let url = url.as_ref().ok_or_else(|| {
							Error::Other(tg::error!("failed to send the request"))
						})?;
						let sender = Self::connect_h2(url).await.map_err(Error::Other)?;
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
			Some("http+unix") => {
				let path = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				Self::connect_unix_h2(Path::new(path)).await
			},
			Some("http") => {
				let host = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!(%url, "invalid url"))?;
				Self::connect_tcp_h2(host, port).await
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
					Self::connect_vsock_h2(cid, u32::from(port)).await
				}
			},
			_ => Err(tg::error!(%url, "invalid url")),
		}
	}

	async fn connect_unix_h2(
		path: &Path,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>> {
		let stream = tokio::net::UnixStream::connect(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?;
		Self::handshake_h2(stream).await
	}

	async fn connect_tcp_h2(
		host: &str,
		port: u16,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>> {
		let stream = tokio::net::TcpStream::connect((host, port))
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?;
		Self::handshake_h2(stream).await
	}

	#[cfg(feature = "vsock")]
	async fn connect_vsock_h2(
		cid: u32,
		port: u32,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>> {
		let stream = tokio_vsock::VsockStream::connect(tokio_vsock::VsockAddr::new(cid, port))
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?;
		Self::handshake_h2(stream).await
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
