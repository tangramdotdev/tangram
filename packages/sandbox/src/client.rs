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

pub mod kill;
pub mod spawn;
pub mod stdio;
pub mod tty;
pub mod wait;

pub struct Client(Arc<State>);

pub struct State {
	addr: tokio_util::either::Either<PathBuf, u16>,
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
	pub fn new_unix(path: PathBuf) -> Self {
		let addr = tokio_util::either::Either::Left(path);
		let (sender, service) = Self::service(addr.clone());
		Self(Arc::new(State {
			addr,
			sender,
			service,
		}))
	}

	pub fn new_tcp(port: u16) -> Self {
		let addr = tokio_util::either::Either::Right(port);
		let (sender, service) = Self::service(addr.clone());
		Self(Arc::new(State {
			addr,
			sender,
			service,
		}))
	}

	fn service(addr: tokio_util::either::Either<PathBuf, u16>) -> (Sender, Service) {
		let sender = Arc::new(tokio::sync::Mutex::new(
			None::<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>>,
		));
		let service = tower::service_fn({
			let sender = sender.clone();
			move |request| {
				let addr = addr.clone();
				let sender = sender.clone();
				async move {
					let mut guard = sender.lock().await;
					let mut sender = match guard.as_ref() {
						Some(sender) if sender.is_ready() => sender.clone(),
						_ => {
							let sender = match &addr {
								tokio_util::either::Either::Left(path) => {
									Self::connect_unix_h2(path).await.map_err(Error::Other)?
								},
								tokio_util::either::Either::Right(port) => {
									Self::connect_tcp_h2(*port).await.map_err(Error::Other)?
								},
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
		let service = Service::new(service);
		(sender, service)
	}

	async fn connect_unix_h2(
		path: &Path,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>> {
		// Connect via UNIX.
		let stream = tokio::net::UnixStream::connect(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?;

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

	async fn connect_tcp_h2(
		port: u16,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>> {
		// Connect via TCP.
		let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?;

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

	pub(crate) async fn connect(&self) -> tg::Result<()> {
		let mut guard = self.sender.lock().await;
		if let Some(sender) = guard.as_ref()
			&& sender.is_ready()
		{
			return Ok(());
		}
		let sender = match &self.addr {
			tokio_util::either::Either::Left(path) => Self::connect_unix_h2(path)
				.await
				.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?,
			tokio_util::either::Either::Right(port) => Self::connect_tcp_h2(*port)
				.await
				.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?,
		};
		guard.replace(sender);
		Ok(())
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
