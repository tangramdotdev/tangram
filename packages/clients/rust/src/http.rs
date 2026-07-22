#[cfg(feature = "tls")]
use rustls_platform_verifier::BuilderVerifierExt as _;
use tangram_http::body::Ext as _;
use {
	crate::prelude::*,
	std::{
		error::Error as _,
		ops::ControlFlow,
		path::Path,
		pin::Pin,
		str::FromStr,
		task::{Context, Poll},
		time::Duration,
	},
	tangram_http::request::Ext as _,
	tangram_uri::Uri,
	time::format_description::well_known::Rfc3339,
	tokio::io::{AsyncRead, AsyncWrite},
	tower::{Service as _, util::BoxCloneSyncService},
	tower_http::ServiceBuilderExt as _,
};

pub(crate) type Pool = tangram_pool::Pool<Connection, tg::Error>;

pub(crate) type Service = BoxCloneSyncService<
	http::Request<tangram_http::body::Boxed>,
	http::Response<tangram_http::body::Boxed>,
	Error,
>;

pub(crate) struct Connection {
	sender: hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>,
}

struct Body {
	body: tangram_http::body::Boxed,
	guard: Option<tangram_pool::SharedGuard<Connection, tg::Error>>,
}

#[derive(Debug, derive_more::Display, derive_more::Error)]
pub(crate) enum Error {
	Hyper(hyper::Error),
	Other(tg::Error),
}

impl tg::Client {
	pub(crate) fn pool(
		options: tangram_pool::Options,
		reconnect: &tangram_futures::retry::Options,
		url: &Uri,
	) -> Pool {
		let reconnect = reconnect.clone();
		let url = url.clone();
		Pool::new(options, move || {
			let reconnect = reconnect.clone();
			let url = url.clone();
			async move {
				tangram_futures::retry(&reconnect, || {
					let url = url.clone();
					async move {
						match Self::connect_h2(&url).await {
							Ok(sender) => Ok(ControlFlow::Break(Connection::new(sender))),
							Err(error) => Ok(ControlFlow::Continue(error)),
						}
					}
				})
				.await
			}
		})
	}

	pub(crate) fn service(version: &str, pool: &Pool) -> Service {
		let service = tower::service_fn({
			let pool = pool.clone();
			move |request| {
				let pool = pool.clone();
				async move {
					let connection = pool
						.get_shared(tangram_pool::Priority::default())
						.await
						.map_err(Error::Other)?;
					match connection.send(request).await {
						Ok(response) => {
							let response = response.map(|body| {
								let body = Body {
									body,
									guard: Some(connection),
								};
								body.boxed()
							});
							Ok(response)
						},
						Err(error) => {
							if is_retryable_error(&error) {
								connection.discard();
							}
							Err(Error::Hyper(error))
						},
					}
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
			.insert_request_header_if_not_present(
				http::HeaderName::from_str("x-tg-compatibility-date").unwrap(),
				http::HeaderValue::from_str(&Self::compatibility_date().format(&Rfc3339).unwrap())
					.unwrap(),
			)
			.insert_request_header_if_not_present(
				http::HeaderName::from_str("x-tg-version").unwrap(),
				http::HeaderValue::from_str(version).unwrap(),
			)
			.layer(
				tangram_http::layer::compression::RequestCompressionLayer::new(|parts, _| {
					let has_content_length =
						parts.headers.get(http::header::CONTENT_LENGTH).is_some();
					let is_sync =
						parts
							.headers
							.get(http::header::CONTENT_TYPE)
							.is_some_and(|content_type| {
								matches!(content_type.to_str(), Ok(tg::sync::CONTENT_TYPE))
							});
					if has_content_length || is_sync {
						Some((tangram_http::body::compression::Algorithm::Zstd, 3))
					} else {
						None
					}
				}),
			)
			.layer(tangram_http::layer::compression::ResponseDecompressionLayer)
			.service(service);
		Service::new(service)
	}

	pub async fn connect(&self) -> tg::Result<()> {
		loop {
			let guard = self
				.pool
				.get_shared(tangram_pool::Priority::default())
				.await?;
			if guard.is_closed() {
				guard.discard();
				continue;
			}
			return Ok(());
		}
	}

	pub async fn disconnect(&self) {
		self.pool.clear();
	}

	pub(crate) async fn connect_h1(
		url: &Uri,
	) -> tg::Result<hyper::client::conn::http1::SendRequest<tangram_http::body::Boxed>> {
		match url.scheme() {
			Some("http+stdio") => {
				let stream = tokio::io::join(tokio::io::stdin(), tokio::io::stdout());
				Self::handshake_h1(stream).await
			},
			Some("http") => {
				let host = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!(%url, "invalid url"))?
					.try_into()
					.map_err(|_| tg::error!("invalid port"))?;
				let stream = Self::connect_tcp(host, port).await?;
				Self::handshake_h1(stream).await
			},
			Some("https") => {
				#[cfg(not(feature = "tls"))]
				{
					Err(tg::error!("tls is not enabled"))
				}
				#[cfg(feature = "tls")]
				{
					let host = url
						.domain()
						.ok_or_else(|| tg::error!(%url, "invalid url"))?;
					let port = url
						.port_or_known_default()
						.ok_or_else(|| tg::error!(%url, "invalid url"))?
						.try_into()
						.map_err(|_| tg::error!("invalid port"))?;
					let stream =
						Self::connect_tcp_tls(host, port, vec![b"http/1.1".into()]).await?;
					Self::verify_alpn_protocol(&stream, b"http/1.1")?;
					Self::handshake_h1(stream).await
				}
			},
			Some("http+unix") => {
				let path = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let stream = Self::connect_unix(Path::new(path)).await?;
				Self::handshake_h1(stream).await
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
						.map_err(|error| tg::error!(!error, %url, "invalid url"))?;
					let port = url.port().ok_or_else(|| tg::error!(%url, "invalid url"))?;
					let addr = tokio_vsock::VsockAddr::new(cid, port);
					let stream = Self::connect_vsock(addr).await?;
					Self::handshake_h1(stream).await
				}
			},
			_ => Err(tg::error!(%url, "invalid url")),
		}
	}

	async fn connect_h2(
		url: &Uri,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>> {
		match url.scheme() {
			Some("http+stdio") => {
				let stream = tokio::io::join(tokio::io::stdin(), tokio::io::stdout());
				Self::handshake_h2(stream).await
			},
			Some("http") => {
				let host = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!("invalid url"))?
					.try_into()
					.map_err(|_| tg::error!("invalid port"))?;
				let stream = Self::connect_tcp(host, port).await?;
				Self::handshake_h2(stream).await
			},
			Some("https") => {
				#[cfg(not(feature = "tls"))]
				{
					Err(tg::error!("tls is not enabled"))
				}
				#[cfg(feature = "tls")]
				{
					let host = url
						.domain()
						.ok_or_else(|| tg::error!(%url, "invalid url"))?;
					let port = url
						.port_or_known_default()
						.ok_or_else(|| tg::error!(%url, "invalid url"))?
						.try_into()
						.map_err(|_| tg::error!("invalid port"))?;
					let stream = Self::connect_tcp_tls(host, port, vec![b"h2".into()]).await?;
					Self::verify_alpn_protocol(&stream, b"h2")?;
					Self::handshake_h2(stream).await
				}
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
						.map_err(|error| tg::error!(!error, %url, "invalid url"))?;
					let port = url.port().ok_or_else(|| tg::error!(%url, "invalid url"))?;
					let addr = tokio_vsock::VsockAddr::new(cid, port);
					let stream = Self::connect_vsock(addr).await?;
					Self::handshake_h2(stream).await
				}
			},
			_ => Err(tg::error!(%url, "invalid url")),
		}
	}

	async fn handshake_h1<S>(
		stream: S,
	) -> tg::Result<hyper::client::conn::http1::SendRequest<tangram_http::body::Boxed>>
	where
		S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
	{
		// Perform the HTTP handshake.
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
			.await
			.map_err(|error| tg::error!(!error, "failed to perform the HTTP handshake"))?;

		// Spawn the connection.
		tokio::spawn(async move {
			connection
				.with_upgrades()
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
			.map_err(|error| tg::error!(!error, "failed to ready the sender"))?;

		Ok(sender)
	}

	pub(crate) async fn handshake_h2<S>(
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
			.map_err(|error| tg::error!(!error, "failed to perform the HTTP handshake"))?;

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
			.map_err(|error| tg::error!(!error, "failed to ready the sender"))?;

		Ok(sender)
	}

	pub(crate) async fn connect_tcp(host: &str, port: u16) -> tg::Result<tokio::net::TcpStream> {
		let addr = format!("{host}:{port}");
		tokio::time::timeout(Duration::from_secs(1), tokio::net::TcpStream::connect(addr))
			.await
			.map_err(|_| tg::error!("connection timeout"))?
			.map_err(|error| tg::error!(!error, "failed to create the TCP connection"))
	}

	#[cfg(feature = "tls")]
	pub(crate) async fn connect_tcp_tls(
		host: &str,
		port: u16,
		protocols: Vec<Vec<u8>>,
	) -> tg::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
		let stream = Self::connect_tcp(host, port).await?;

		// Create the connector.
		let mut config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
			rustls::crypto::aws_lc_rs::default_provider(),
		))
		.with_safe_default_protocol_versions()
		.unwrap()
		.with_platform_verifier()
		.map_err(|error| tg::error!(!error, "failed to create the tls config"))?
		.with_no_client_auth();
		config.alpn_protocols = protocols;
		let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));

		// Create the server name.
		let server_name = rustls::pki_types::ServerName::try_from(host.to_string().as_str())
			.map_err(|error| tg::error!(!error, "failed to create the server name"))?
			.to_owned();

		// Connect via TLS.
		let stream = connector
			.connect(server_name, stream)
			.await
			.map_err(|error| tg::error!(!error, "failed to connect"))?;

		Ok(stream)
	}

	async fn connect_unix(path: &Path) -> tg::Result<tokio::net::UnixStream> {
		let path_ = tangram_util::io::unix::resolve(path).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to resolve the socket path"),
		)?;
		tokio::net::UnixStream::connect(path_.as_ref())
			.await
			.map_err(
				|error| tg::error!(!error, path = %path.display(), "failed to connect to the socket"),
			)
	}

	#[cfg(feature = "vsock")]
	pub(crate) async fn connect_vsock(
		addr: tokio_vsock::VsockAddr,
	) -> tg::Result<tokio_vsock::VsockStream> {
		tokio::time::timeout(
			Duration::from_secs(1),
			tokio_vsock::VsockStream::connect(addr),
		)
		.await
		.map_err(|error| tg::error!(!error, "timed out connecting to the socket"))?
		.map_err(|error| tg::error!(!error, "failed to connect to the socket"))
	}

	#[cfg(feature = "tls")]
	fn verify_alpn_protocol(
		stream: &tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
		protocol: &[u8],
	) -> tg::Result<()> {
		if stream
			.get_ref()
			.1
			.alpn_protocol()
			.is_some_and(|actual| actual == protocol)
		{
			Ok(())
		} else {
			Err(tg::error!("failed to negotiate the protocol"))
		}
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
			Error::Hyper(source) => tg::error!(source = source, "failed to send the request"),
			Error::Other(error) => error,
		})?;
		let response = response.map(Into::into);
		Ok(response)
	}
}

impl Connection {
	pub(crate) fn new(
		sender: hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>,
	) -> Self {
		Self { sender }
	}

	fn is_closed(&self) -> bool {
		self.sender.is_closed()
	}

	async fn send(
		&self,
		request: http::Request<tangram_http::body::Boxed>,
	) -> Result<http::Response<tangram_http::body::Boxed>, hyper::Error> {
		let mut sender = self.sender.clone();
		sender.ready().await?;
		sender
			.send_request(request)
			.await
			.map(tangram_http::response::Ext::boxed_body)
	}
}

impl tg::Session {
	pub(crate) fn apply_context_headers<B>(&self, request: &mut http::Request<B>) {
		let headers = request.headers_mut();
		if let Some(token) = self.context().token() {
			headers
				.entry(http::header::AUTHORIZATION)
				.or_insert_with(|| {
					http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap()
				});
		}
	}

	pub(crate) async fn send<B>(
		&self,
		mut request: http::Request<B>,
	) -> tg::Result<http::Response<tangram_http::body::Boxed>>
	where
		B: http_body::Body<Data = bytes::Bytes> + Send + Unpin + 'static,
		B::Error: Into<tangram_http::Error> + Send,
	{
		self.apply_context_headers(&mut request);
		self.client().send(request).await
	}

	pub(crate) async fn send_with_retry<B>(
		&self,
		mut request: http::Request<B>,
	) -> tg::Result<http::Response<tangram_http::body::Boxed>>
	where
		B: http_body::Body<Data = bytes::Bytes> + Clone + Send + Unpin + 'static,
		B::Error: Into<tangram_http::Error> + Clone + Send,
	{
		self.apply_context_headers(&mut request);
		let session = self.clone();
		tangram_futures::retry(&self.client().retry, || {
			let session = session.clone();
			let request = request.clone();
			async move {
				let request = request.boxed_body();
				let future = session.client().service.clone().call(request);
				match future.await {
					Ok(response) => {
						let response = response.map(Into::into);
						Ok(ControlFlow::Break(response))
					},
					Err(Error::Hyper(error)) if is_retryable_error(&error) => Ok(
						ControlFlow::Continue(tg::error!(!error, "failed to send the request")),
					),
					Err(error) => Err(match error {
						Error::Hyper(source) => {
							tg::error!(source = source, "failed to send the request")
						},
						Error::Other(error) => error,
					}),
				}
			}
		})
		.await
	}
}

impl http_body::Body for Body {
	type Data = bytes::Bytes;
	type Error = tangram_http::Error;

	fn poll_frame(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
		let this = self.get_mut();
		let result = Pin::new(&mut this.body).poll_frame(cx);
		match &result {
			Poll::Ready(None) => {
				this.guard.take();
			},
			Poll::Ready(Some(Err(_))) => {
				if let Some(guard) = this.guard.take() {
					guard.discard();
				}
			},
			Poll::Ready(Some(Ok(_))) | Poll::Pending => (),
		}
		result
	}

	fn is_end_stream(&self) -> bool {
		self.body.is_end_stream()
	}

	fn size_hint(&self) -> http_body::SizeHint {
		self.body.size_hint()
	}
}

fn is_retryable_error(error: &hyper::Error) -> bool {
	error.is_closed()
		|| error.is_canceled()
		|| error.is_incomplete_message()
		|| io_error_kind(error).is_some_and(|kind| {
			matches!(
				kind,
				std::io::ErrorKind::BrokenPipe
					| std::io::ErrorKind::ConnectionAborted
					| std::io::ErrorKind::ConnectionReset
					| std::io::ErrorKind::NotConnected
					| std::io::ErrorKind::UnexpectedEof
			)
		})
}

fn io_error_kind(error: &hyper::Error) -> Option<std::io::ErrorKind> {
	let mut source = error.source();
	while let Some(error) = source {
		if let Some(error) = error.downcast_ref::<std::io::Error>() {
			return Some(error.kind());
		}
		source = error.source();
	}
	None
}
