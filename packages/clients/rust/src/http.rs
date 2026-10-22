#[cfg(feature = "tls")]
use rustls_platform_verifier::BuilderVerifierExt as _;
use tangram_http::body::Ext as _;
use {
	crate::prelude::*,
	std::{
		ops::ControlFlow,
		path::Path,
		pin::Pin,
		str::FromStr,
		sync::{Arc, Mutex, Weak},
		task::{Context, Poll},
		time::Duration,
	},
	tangram_http::{request::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	time::format_description::well_known::Rfc3339,
	tokio::io::{AsyncRead, AsyncWrite},
	tower::{Service as _, util::BoxCloneSyncService},
	tower_http::ServiceBuilderExt as _,
};

pub(crate) mod frame;

type Affinities = Arc<Mutex<weak_table::WeakValueHashMap<Affinity, Weak<Guard>>>>;

type Guard = tangram_pool::SharedGuard<Connection, Error>;

pub(crate) type Pool = tangram_pool::Pool<Connection, Error>;

pub(crate) type Service = BoxCloneSyncService<
	http::Request<tangram_http::body::Boxed>,
	http::Response<tangram_http::body::Boxed>,
	Error,
>;

/// The key of a group of correlated requests, which the client sends over one connection, so that one server answers them all over its local channel.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum Affinity {
	Nonce(tg::process::Nonce),
	Process(tg::process::Id),
	Sandbox(tg::sandbox::Id),
}

pub(crate) struct Connection {
	sender: hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>,
}

struct Body {
	body: tangram_http::body::Boxed,
	guard: Option<Arc<Guard>>,
}

#[derive(Debug, derive_more::Display, derive_more::Error)]
pub(crate) enum Error {
	/// A failure to get a connection.
	Connection(tg::Error),

	Hyper(hyper::Error),

	Other(tg::Error),

	/// A failure to receive a response in time, after which the server may have handled the request.
	Timeout(tg::Error),
}

impl Affinity {
	#[must_use]
	pub(crate) fn with_process(process: &tg::Either<tg::process::Nonce, tg::process::Id>) -> Self {
		match process {
			tg::Either::Left(nonce) => Self::Nonce(*nonce),
			tg::Either::Right(id) => Self::Process(id.clone()),
		}
	}
}

impl tg::Client {
	pub(crate) fn pool(
		options: tangram_pool::Options,
		reconnect: &tangram_futures::retry::Options,
		url: &Uri,
		http: tg::Http,
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
						match Self::connect_h2(&url, http).await {
							Ok(ControlFlow::Break(sender)) => {
								Ok(ControlFlow::Break(Connection::new(sender)))
							},
							Ok(ControlFlow::Continue(error)) => {
								Ok(ControlFlow::Continue(Error::Connection(error)))
							},
							Err(error) => Err(Error::Other(error)),
						}
					}
				})
				.await
			}
		})
	}

	pub(crate) fn service(version: &str, pool: &Pool, url: &Uri, http: tg::Http) -> Service {
		let origin = match (url.scheme(), url.authority_raw()) {
			(Some(scheme @ ("http" | "https")), Some(authority)) => {
				Some(format!("{scheme}://{authority}"))
			},
			_ => None,
		};
		let affinities = Affinities::default();
		let service = tower::service_fn({
			let pool = pool.clone();
			move |request: http::Request<tangram_http::body::Boxed>| {
				let affinities = affinities.clone();
				let pool = pool.clone();
				let origin = origin.clone();
				async move {
					let request = with_origin(request, origin.as_deref())?;
					let affinity = request.extensions().get::<Affinity>().cloned();
					let connection = Self::connection(&pool, &affinities, affinity).await?;
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
							if is_retryable_error(&error)
								&& let Ok(connection) = Arc::try_unwrap(connection)
							{
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
			.layer(tower::layer::layer_fn(move |service| {
				let service = Service::new(service);
				tower::service_fn(move |request| {
					let future = service.clone().call(request);
					async move {
						match tokio::time::timeout(http.request_timeout, future).await {
							Ok(result) => result,
							Err(_) => Err(Error::Timeout(tg::error!("the request timed out"))),
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
			.layer(
				tangram_http::layer::coalescing::RequestCoalescingLayer::new(
					http.coalescing_target_size,
				),
			)
			.layer(tangram_http::layer::compression::ResponseDecompressionLayer)
			.service(service);
		Service::new(service)
	}

	/// Get a connection for a request, which is the open connection of the request's affinity if it has one.
	async fn connection(
		pool: &Pool,
		affinities: &Affinities,
		affinity: Option<Affinity>,
	) -> Result<Arc<Guard>, Error> {
		// Use the open connection of the affinity.
		if let Some(affinity) = &affinity
			&& let Some(connection) = affinities.lock().unwrap().get(affinity)
			&& !connection.is_closed()
		{
			return Ok(connection);
		}

		// Get an open connection from the pool.
		let connection = loop {
			let connection = pool.get_shared(tangram_pool::Priority::default()).await?;
			if connection.is_closed() {
				connection.discard();
				continue;
			}
			break Arc::new(connection);
		};

		// Store the connection for the affinity, unless a concurrent request stored an open connection first.
		if let Some(affinity) = affinity {
			let mut affinities = affinities.lock().unwrap();
			if let Some(existing) = affinities.get(&affinity)
				&& !existing.is_closed()
			{
				return Ok(existing);
			}
			affinities.insert(affinity, connection.clone());
		}

		Ok(connection)
	}

	pub async fn connect(&self) -> tg::Result<()> {
		loop {
			let guard = self
				.pool
				.get_shared(tangram_pool::Priority::default())
				.await
				.map_err(|error| match error {
					Error::Connection(error) | Error::Other(error) | Error::Timeout(error) => error,
					Error::Hyper(source) => tg::error!(!source, "failed to connect"),
				})?;
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
		connect_timeout: Duration,
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
				let stream = Self::connect_tcp(host, port, connect_timeout).await?;
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
					let stream = Self::connect_tcp_tls(
						host,
						port,
						vec![b"http/1.1".into()],
						connect_timeout,
					)
					.await?;
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
					let stream = Self::connect_vsock(addr, connect_timeout).await?;
					Self::handshake_h1(stream).await
				}
			},
			_ => Err(tg::error!(%url, "invalid url")),
		}
	}

	async fn connect_h2(
		url: &Uri,
		http: tg::Http,
	) -> tg::Result<
		ControlFlow<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>, tg::Error>,
	> {
		let connect_timeout = http.connect_timeout;
		match url.scheme() {
			Some("http+stdio") => {
				let stream = tokio::io::join(tokio::io::stdin(), tokio::io::stdout());
				Ok(Self::handshake_h2(stream, http)
					.await
					.map_or_else(ControlFlow::Continue, ControlFlow::Break))
			},
			Some("http") => {
				let host = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!("invalid url"))?
					.try_into()
					.map_err(|_| tg::error!("invalid port"))?;
				let stream = match Self::connect_tcp(host, port, connect_timeout).await {
					Ok(stream) => stream,
					Err(error) => return Ok(ControlFlow::Continue(error)),
				};
				Ok(Self::handshake_h2(stream, http)
					.await
					.map_or_else(ControlFlow::Continue, ControlFlow::Break))
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
					let stream = match Self::connect_tcp_tls(
						host,
						port,
						vec![b"h2".into()],
						connect_timeout,
					)
					.await
					{
						Ok(stream) => stream,
						Err(error) => return Ok(ControlFlow::Continue(error)),
					};
					Self::verify_alpn_protocol(&stream, b"h2")?;
					Ok(Self::handshake_h2(stream, http)
						.await
						.map_or_else(ControlFlow::Continue, ControlFlow::Break))
				}
			},
			Some("http+unix") => {
				let path = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let stream = match Self::connect_unix(Path::new(path)).await {
					Ok(stream) => stream,
					Err(error) => return Ok(ControlFlow::Continue(error)),
				};
				Ok(Self::handshake_h2(stream, http)
					.await
					.map_or_else(ControlFlow::Continue, ControlFlow::Break))
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
					let stream = match Self::connect_vsock(addr, connect_timeout).await {
						Ok(stream) => stream,
						Err(error) => return Ok(ControlFlow::Continue(error)),
					};
					Ok(Self::handshake_h2(stream, http)
						.await
						.map_or_else(ControlFlow::Continue, ControlFlow::Break))
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
		let (mut sender, connection) =
			hyper::client::conn::http1::handshake(io)
				.await
				.map_err(|error| {
					tg::error!(unavailable, !error, "failed to perform the HTTP handshake")
				})?;

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
			.map_err(|error| tg::error!(unavailable, !error, "failed to ready the sender"))?;

		Ok(sender)
	}

	pub(crate) async fn handshake_h2<S>(
		stream: S,
		http: tg::Http,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>>
	where
		S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
	{
		// Perform the HTTP handshake.
		let executor = hyper_util::rt::TokioExecutor::new();
		let io = hyper_util::rt::TokioIo::new(stream);
		let http2 = http.http2;
		let mut builder = hyper::client::conn::http2::Builder::new(executor);
		builder
			.timer(hyper_util::rt::TokioTimer::new())
			.initial_connection_window_size(http2.connection_window_size)
			.initial_stream_window_size(http2.stream_window_size)
			.max_concurrent_streams(http2.max_concurrent_streams)
			.max_concurrent_reset_streams(http2.max_concurrent_reset_streams);
		if let Some(interval) = http.keep_alive_interval {
			builder.keep_alive_interval(interval);
		}
		if let Some(timeout) = http.keep_alive_timeout {
			builder.keep_alive_timeout(timeout);
		}
		let (mut sender, connection) = builder.handshake(io).await.map_err(|error| {
			tg::error!(unavailable, !error, "failed to perform the HTTP handshake")
		})?;

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
			.map_err(|error| tg::error!(unavailable, !error, "failed to ready the sender"))?;

		Ok(sender)
	}

	pub(crate) async fn connect_tcp(
		host: &str,
		port: u16,
		connect_timeout: Duration,
	) -> tg::Result<tokio::net::TcpStream> {
		let addr = format!("{host}:{port}");
		let stream = tokio::time::timeout(connect_timeout, tokio::net::TcpStream::connect(addr))
			.await
			.map_err(|_| tg::error!(unavailable, "timed out creating the TCP connection"))?
			.map_err(|error| {
				tg::error!(unavailable, !error, "failed to create the TCP connection")
			})?;
		stream
			.set_nodelay(true)
			.map_err(|error| tg::error!(!error, "failed to set nodelay on the TCP connection"))?;
		Ok(stream)
	}

	#[cfg(feature = "tls")]
	pub(crate) async fn connect_tcp_tls(
		host: &str,
		port: u16,
		protocols: Vec<Vec<u8>>,
		connect_timeout: Duration,
	) -> tg::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
		let stream = Self::connect_tcp(host, port, connect_timeout).await?;

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
		// Mark a missing socket unavailable, because it means the server is not running.
		let path_ = tangram_util::io::unix::resolve(path).map_err(|error| {
			if error.kind() == std::io::ErrorKind::NotFound {
				tg::error!(unavailable, !error, path = %path.display(), "failed to resolve the socket path")
			} else {
				tg::error!(!error, path = %path.display(), "failed to resolve the socket path")
			}
		})?;
		tokio::net::UnixStream::connect(path_.as_ref())
			.await
			.map_err(|error| {
				tg::error!(unavailable, !error, path = %path.display(), "failed to connect to the socket")
			})
	}

	#[cfg(feature = "vsock")]
	pub(crate) async fn connect_vsock(
		addr: tokio_vsock::VsockAddr,
		connect_timeout: Duration,
	) -> tg::Result<tokio_vsock::VsockStream> {
		tokio::time::timeout(connect_timeout, tokio_vsock::VsockStream::connect(addr))
			.await
			.map_err(|error| tg::error!(unavailable, !error, "timed out connecting to the socket"))?
			.map_err(|error| tg::error!(unavailable, !error, "failed to connect to the socket"))
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
			Error::Connection(error) | Error::Other(error) | Error::Timeout(error) => error,
			Error::Hyper(source) if is_transport_error(&source) => {
				tg::error!(unavailable, source = source, "failed to send the request")
			},
			Error::Hyper(source) => tg::error!(source = source, "failed to send the request"),
		})?;
		let response = response.map(Into::into);
		Ok(response)
	}

	/// Send a request, continuing if the request failed to reach the server, timed out, or the server responded that it is unavailable.
	pub(crate) async fn send_attempt<B>(
		&self,
		request: http::Request<B>,
	) -> tg::Result<ControlFlow<http::Response<tangram_http::body::Boxed>, tg::Error>>
	where
		B: http_body::Body<Data = bytes::Bytes> + Send + Unpin + 'static,
		B::Error: Into<tangram_http::Error> + Send,
	{
		let request = request.boxed_body();
		let future = self.service.clone().call(request);
		let response = match future.await {
			Ok(response) => response,
			Err(Error::Connection(error) | Error::Timeout(error)) => {
				return Ok(ControlFlow::Continue(error));
			},
			Err(Error::Hyper(source)) if is_transport_error(&source) => {
				let error = tg::error!(unavailable, source = source, "failed to send the request");
				return Ok(ControlFlow::Continue(error));
			},
			Err(Error::Hyper(source)) => {
				return Err(tg::error!(source = source, "failed to send the request"));
			},
			Err(Error::Other(error)) => return Err(error),
		};
		let output = response_attempt(response).await;

		Ok(output)
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
		if let Some(process) = self.context().process() {
			headers
				.entry(tg::process::PROCESS_HEADER)
				.or_insert_with(|| http::HeaderValue::from_str(&process.to_string()).unwrap());
		}
		if let Some(sandbox) = self.context().sandbox() {
			headers
				.entry(tg::sandbox::SANDBOX_HEADER)
				.or_insert_with(|| http::HeaderValue::from_str(&sandbox.to_string()).unwrap());
		}

		// Send a request that claims a process or a sandbox over the connection of the process or sandbox, so that the server that verifies the claim answers it over its local channel.
		let affinity = match (self.context().process(), self.context().sandbox()) {
			(Some(process), _) => Some(Affinity::Process(process.clone())),
			(None, Some(sandbox)) => Some(Affinity::Sandbox(sandbox.clone())),
			(None, None) => None,
		};
		if let Some(affinity) = affinity
			&& request.extensions().get::<Affinity>().is_none()
		{
			request.extensions_mut().insert(affinity);
		}
	}

	pub async fn send<B>(
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

	/// Send a request, continuing if the request failed to reach the server or the server responded that it is unavailable.
	pub(crate) async fn send_attempt<B>(
		&self,
		mut request: http::Request<B>,
	) -> tg::Result<ControlFlow<http::Response<tangram_http::body::Boxed>, tg::Error>>
	where
		B: http_body::Body<Data = bytes::Bytes> + Send + Unpin + 'static,
		B::Error: Into<tangram_http::Error> + Send,
	{
		self.apply_context_headers(&mut request);
		self.client().send_attempt(request).await
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
		tangram_futures::retry(&self.client().retry, || {
			let request = request.clone().boxed_body();
			async move {
				// Continue only if the request cannot have been handled, because a request that timed out may have been handled.
				let future = self.client().service.clone().call(request);
				let response = match future.await {
					Ok(response) => response,
					Err(Error::Connection(error)) => return Ok(ControlFlow::Continue(error)),
					Err(Error::Hyper(source)) if is_retryable_error(&source) => {
						let error =
							tg::error!(unavailable, source = source, "failed to send the request");
						return Ok(ControlFlow::Continue(error));
					},
					Err(Error::Hyper(source)) => {
						return Err(tg::error!(source = source, "failed to send the request"));
					},
					Err(Error::Other(error) | Error::Timeout(error)) => return Err(error),
				};
				let output = response_attempt(response).await;

				Ok(output)
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
				// Discard the connection if the stream error closed it and no other request in its affinity holds it.
				if let Some(guard) = this.guard.take()
					&& guard.is_closed()
					&& let Ok(guard) = Arc::try_unwrap(guard)
				{
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

fn with_origin(
	mut request: http::Request<tangram_http::body::Boxed>,
	origin: Option<&str>,
) -> Result<http::Request<tangram_http::body::Boxed>, Error> {
	if request.uri().scheme().is_none()
		&& let Some(origin) = origin
	{
		let uri = format!("{origin}{}", request.uri())
			.parse()
			.map_err(|error| {
				Error::Other(tg::error!(!error, "failed to create the request uri"))
			})?;
		*request.uri_mut() = uri;
	}
	Ok(request)
}

async fn response_attempt(
	response: http::Response<tangram_http::body::Boxed>,
) -> ControlFlow<http::Response<tangram_http::body::Boxed>, tg::Error> {
	// Continue if the server responded that it is unavailable, including the server's error when the body has one.
	if response.status() == http::StatusCode::SERVICE_UNAVAILABLE {
		let error = if let Ok(error) = response.json::<tg::Error>().await {
			tg::error!(!error, "the server is unavailable")
		} else {
			tg::error!("the server is unavailable")
		};
		return ControlFlow::Continue(error);
	}

	ControlFlow::Break(response)
}

fn is_transport_error(error: &hyper::Error) -> bool {
	!(error.is_parse() || error.is_parse_status() || error.is_user())
}

fn is_retryable_error(error: &hyper::Error) -> bool {
	tangram_http::error::is_retryable(error)
}
