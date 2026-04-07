use {
	std::{
		ops::Deref,
		path::{Path, PathBuf},
		sync::Arc,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_http::request::Ext as _,
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
	addr: Option<Address>,
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

#[derive(Clone)]
enum Address {
	Unix(PathBuf),
	Tcp(u16),
	#[cfg(feature = "vsock")]
	Vsock {
		cid: u32,
		port: u32,
	},
}

impl Client {
	pub fn new() -> Self {
		let sender = Arc::new(tokio::sync::Mutex::new(None));
		let service = Self::service(sender.clone(), None);
		Self(Arc::new(State {
			addr: None,
			sender,
			service,
		}))
	}

	#[expect(dead_code)]
	pub fn new_unix(path: PathBuf) -> Self {
		let sender = Arc::new(tokio::sync::Mutex::new(None));
		let addr = Some(Address::Unix(path));
		let service = Self::service(sender.clone(), addr.clone());
		Self(Arc::new(State {
			addr,
			sender,
			service,
		}))
	}

	#[expect(dead_code)]
	pub fn new_tcp(port: u16) -> Self {
		let sender = Arc::new(tokio::sync::Mutex::new(None));
		let addr = Some(Address::Tcp(port));
		let service = Self::service(sender.clone(), addr.clone());
		Self(Arc::new(State {
			addr,
			sender,
			service,
		}))
	}

	#[cfg(feature = "vsock")]
	#[expect(dead_code)]
	pub fn new_vsock(cid: u32, port: u32) -> Self {
		let sender = Arc::new(tokio::sync::Mutex::new(None));
		let addr = Some(Address::Vsock { cid, port });
		let service = Self::service(sender.clone(), addr.clone());
		Self(Arc::new(State {
			addr,
			sender,
			service,
		}))
	}

	fn service(sender: Sender, addr: Option<Address>) -> Service {
		let service = tower::service_fn(move |request| {
			let sender = sender.clone();
			let addr = addr.clone();
			async move {
				let mut guard = sender.lock().await;
				let mut sender = match guard.as_ref() {
					Some(sender) if sender.is_ready() => sender.clone(),
					_ => {
						let addr = addr.as_ref().ok_or_else(|| {
							Error::Other(tg::error!("failed to send the request"))
						})?;
						let sender = match addr {
							Address::Unix(path) => {
								Self::connect_unix_h2(path).await.map_err(Error::Other)?
							},
							Address::Tcp(port) => {
								Self::connect_tcp_h2(*port).await.map_err(Error::Other)?
							},
							#[cfg(feature = "vsock")]
							Address::Vsock { cid, port } => Self::connect_vsock_h2(*cid, *port)
								.await
								.map_err(Error::Other)?,
						};
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

	async fn connect_unix_h2(
		path: &Path,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>> {
		let stream = tokio::net::UnixStream::connect(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?;
		Self::connect_h2(stream).await
	}

	async fn connect_tcp_h2(
		port: u16,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>> {
		let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?;
		Self::connect_h2(stream).await
	}

	#[cfg(feature = "vsock")]
	async fn connect_vsock_h2(
		cid: u32,
		port: u32,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>> {
		let stream = tokio_vsock::VsockStream::connect(tokio_vsock::VsockAddr::new(cid, port))
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?;
		Self::connect_h2(stream).await
	}

	pub async fn accept<S>(&self, stream: S) -> tg::Result<()>
	where
		S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
	{
		let sender = Self::connect_h2(stream).await?;
		let mut guard = self.sender.lock().await;
		guard.replace(sender);
		Ok(())
	}

	#[expect(dead_code)]
	pub(crate) async fn connect(&self) -> tg::Result<()> {
		let mut guard = self.sender.lock().await;
		if let Some(sender) = guard.as_ref()
			&& sender.is_ready()
		{
			return Ok(());
		}
		let addr = self
			.addr
			.as_ref()
			.ok_or_else(|| tg::error!("failed to connect to the socket"))?;
		let sender = match addr {
			Address::Unix(path) => Self::connect_unix_h2(path)
				.await
				.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?,
			Address::Tcp(port) => Self::connect_tcp_h2(*port)
				.await
				.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?,
			#[cfg(feature = "vsock")]
			Address::Vsock { cid, port } => Self::connect_vsock_h2(*cid, *port)
				.await
				.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?,
		};
		guard.replace(sender);
		Ok(())
	}

	async fn connect_h2<S>(
		stream: S,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>>
	where
		S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
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
