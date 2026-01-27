use {
	crate::prelude::*,
	std::{
		path::{Path, PathBuf},
		str::FromStr,
		sync::Arc,
		time::Duration,
	},
	tangram_futures::retry,
	tangram_http::Body,
	tangram_uri::Uri,
	time::format_description::well_known::Rfc3339,
	tokio::net::{TcpStream, UnixStream},
	tower::{Service as _, util::BoxCloneSyncService},
	tower_http::ServiceBuilderExt as _,
};

pub(crate) type Sender =
	Arc<tokio::sync::Mutex<Option<hyper::client::conn::http2::SendRequest<Body>>>>;

pub(crate) type Service = BoxCloneSyncService<http::Request<Body>, http::Response<Body>, Error>;

#[derive(Clone, Debug)]
pub(crate) enum Error {
	Error(tg::Error),
	Disconnected,
}

impl tg::Client {
	pub(crate) fn service(version: &str) -> (Sender, Service) {
		let sender = Arc::new(tokio::sync::Mutex::new(
			None::<hyper::client::conn::http2::SendRequest<Body>>,
		));
		let service = tower::service_fn({
			let sender = sender.clone();
			move |request: http::Request<Body>| {
				let sender = sender.clone();
				async move {
					// Attempt to get the sender.
					let mut guard = sender.lock().await;
					let mut sender_ = match guard.as_ref() {
						Some(sender) if sender.is_closed() => {
							guard.take();
							return Err(Error::Disconnected);
						},
						Some(sender) => sender.clone(),
						None => {
							return Err(Error::Disconnected);
						},
					};
					drop(guard);

					// Try to send the request.
					let method = request.method().clone();
					let uri = request.uri().clone();
					let response = match sender_.try_send_request(request).await {
						Ok(response) => response.map(Body::new),
						Err(error)
							if error.message().is_some()
								|| std::error::Error::source(error.error())
									.and_then(|error| error.downcast_ref::<std::io::Error>())
									.is_some_and(|error| {
										matches!(error.kind(), std::io::ErrorKind::ConnectionReset)
									}) =>
						{
							sender.lock().await.take();
							return Err(Error::Disconnected);
						},
						Err(error) => {
							let error = error.into_error();
							return Err(Error::Error(tg::error!(
								source = error,
								%method,
								%uri,
								"failed to send the request"
							)));
						},
					};

					Ok(response)
				}
			}
		});
		let service = tower::ServiceBuilder::new()
			.layer(tangram_http::layer::tracing::TracingLayer::new())
			.map_err(
				|error: Box<dyn std::error::Error + Send + Sync + 'static>| {
					if let Some(error) = error.downcast_ref::<Error>() {
						error.clone()
					} else {
						Error::Error(tg::Error::from(error))
					}
				},
			)
			.layer(tower::timeout::TimeoutLayer::new(Duration::from_secs(60)))
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
		let service = Service::new(service);
		(sender, service)
	}

	pub async fn connect(&self) -> tg::Result<()> {
		let mut guard = self.sender.lock().await;
		match guard.as_ref() {
			Some(sender) if sender.is_ready() => (),
			_ => {
				let sender = Self::connect_h2(&self.url).await?;
				guard.replace(sender.clone());
			},
		}
		Ok(())
	}

	pub async fn disconnect(&self) {
		self.sender.lock().await.take();
	}

	pub(crate) async fn connect_h1(
		url: &Uri,
	) -> tg::Result<hyper::client::conn::http1::SendRequest<Body>> {
		match url.scheme() {
			Some("http+unix") => {
				let path = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let path = PathBuf::from(path);
				Self::connect_unix_h1(&path).await
			},
			Some("http") => {
				let host = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!(%url, "invalid url"))?;
				Self::connect_tcp_h1(host, port).await
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
						.ok_or_else(|| tg::error!(%url, "invalid url"))?;
					Self::connect_tcp_tls_h1(host, port).await
				}
			},
			_ => Err(tg::error!(%url, "invalid url")),
		}
	}

	async fn connect_h2(url: &Uri) -> tg::Result<hyper::client::conn::http2::SendRequest<Body>> {
		match url.scheme() {
			Some("http+unix") => {
				let path = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let path = PathBuf::from(path);
				Self::connect_unix_h2(&path).await
			},
			Some("http") => {
				let host = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!("invalid url"))?;
				Self::connect_tcp_h2(host, port).await
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
						.ok_or_else(|| tg::error!(%url, "invalid url"))?;
					Self::connect_tcp_tls_h2(host, port).await
				}
			},
			_ => Err(tg::error!(%url, "invalid url")),
		}
	}

	async fn connect_unix_h1(
		path: &Path,
	) -> tg::Result<hyper::client::conn::http1::SendRequest<Body>> {
		// Connect via UNIX.
		let stream = UnixStream::connect(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?;

		// Perform the HTTP handshake.
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the HTTP handshake"))?;

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
			.map_err(|source| tg::error!(!source, "failed to ready the sender"))?;

		Ok(sender)
	}

	pub(crate) async fn connect_unix_h2(
		path: &Path,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<Body>> {
		// Connect via UNIX.
		let stream = UnixStream::connect(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?;

		// Perform the HTTP handshake.
		let executor = hyper_util::rt::TokioExecutor::new();
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http2::Builder::new(executor)
			.max_concurrent_streams(None)
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

	pub(crate) async fn connect_tcp_h1(
		host: &str,
		port: u16,
	) -> tg::Result<hyper::client::conn::http1::SendRequest<Body>> {
		// Connect via TCP.
		let addr = format!("{host}:{port}");
		let stream = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(addr))
			.await
			.map_err(|_| tg::error!("connection timeout"))?
			.map_err(|source| tg::error!(!source, "failed to create the TCP connection"))?;

		// Perform the HTTP handshake.
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the HTTP handshake"))?;

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
			.map_err(|source| tg::error!(!source, "failed to ready the sender"))?;

		Ok(sender)
	}

	pub(crate) async fn connect_tcp_h2(
		host: &str,
		port: u16,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<Body>> {
		// Connect via TCP.
		let addr = format!("{host}:{port}");
		let stream = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(addr))
			.await
			.map_err(|_| tg::error!("connection timeout"))?
			.map_err(|source| tg::error!(!source, "failed to create the TCP connection"))?;

		// Perform the HTTP handshake.
		let executor = hyper_util::rt::TokioExecutor::new();
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http2::handshake(executor, io)
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

	#[cfg(feature = "tls")]
	pub(crate) async fn connect_tcp_tls_h1(
		host: &str,
		port: u16,
	) -> tg::Result<hyper::client::conn::http1::SendRequest<Body>> {
		// Connect via TLS over TCP.
		let stream = Self::connect_tcp_tls(host, port, vec![b"http/1.1".into()]).await?;

		// Verify the negotiated protocol.
		let success = stream
			.get_ref()
			.1
			.alpn_protocol()
			.is_some_and(|protocol| protocol == b"http/1.1");
		if !success {
			return Err(tg::error!("failed to negotiate the protocol"));
		}

		// Perform the HTTP handshake.
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the HTTP handshake"))?;

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
			.map_err(|source| tg::error!(!source, "failed to ready the sender"))?;

		Ok(sender)
	}

	#[cfg(feature = "tls")]
	pub(crate) async fn connect_tcp_tls_h2(
		host: &str,
		port: u16,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<Body>> {
		// Connect via TLS over TCP.
		let stream = Self::connect_tcp_tls(host, port, vec![b"h2".into()]).await?;

		// Verify the negotiated protocol.
		let success = stream
			.get_ref()
			.1
			.alpn_protocol()
			.is_some_and(|protocol| protocol == b"h2");
		if !success {
			return Err(tg::error!("failed to negotiate the protocol"));
		}

		// Perform the HTTP handshake.
		let executor = hyper_util::rt::TokioExecutor::new();
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http2::handshake(executor, io)
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

	#[cfg(feature = "tls")]
	pub(crate) async fn connect_tcp_tls(
		host: &str,
		port: u16,
		protocols: Vec<Vec<u8>>,
	) -> tg::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
		// Connect via TCP.
		let addr = format!("{host}:{port}");
		let stream = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(addr))
			.await
			.map_err(|_| tg::error!("connection timeout"))?
			.map_err(|source| tg::error!(!source, "failed to create the TCP connection"))?;

		// Create the connector.
		let mut root_store = rustls::RootCertStore::empty();
		root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
		let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
			rustls::crypto::ring::default_provider(),
		))
		.with_safe_default_protocol_versions()
		.unwrap()
		.with_root_certificates(root_store)
		.with_no_client_auth();
		config.alpn_protocols = protocols;
		let connector = tokio_rustls::TlsConnector::from(Arc::new(config));

		// Create the server name.
		let server_name = rustls::pki_types::ServerName::try_from(host.to_string().as_str())
			.map_err(|source| tg::error!(!source, "failed to create the server name"))?
			.to_owned();

		// Connect via TLS.
		let stream = connector
			.connect(server_name, stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect"))?;

		Ok(stream)
	}

	pub(crate) async fn ensure_connected(&self) -> tg::Result<()> {
		let mut guard = self.sender.lock().await;
		if guard
			.as_ref()
			.is_some_and(hyper::client::conn::http2::SendRequest::is_ready)
		{
			return Ok(());
		}
		guard.take();
		let options = retry::Options {
			max_delay: std::time::Duration::from_secs(10),
			backoff: std::time::Duration::from_millis(100),
			jitter: std::time::Duration::from_millis(50),
			max_retries: 16,
		};
		let sender = retry::retry(&options, {
			let url = self.url.clone();
			move || {
				let url = url.clone();
				async move {
					match Self::connect_h2(&url).await {
						Ok(sender) => Ok(std::ops::ControlFlow::Break(sender)),
						Err(error) => Ok(std::ops::ControlFlow::Continue(error)),
					}
				}
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to reconnect"))?;
		guard.replace(sender);
		Ok(())
	}

	pub(crate) async fn try_send(
		&self,
		mut request: http::Request<Body>,
	) -> tg::Result<Option<http::Response<Body>>> {
		// Reconnect if necessary.
		self.ensure_connected()
			.await
			.map_err(|source| tg::error!(!source, "failed to reconnect"))?;

		// Add the authorization header to the request.
		if let Some(token) = &self.token {
			request.headers_mut().insert(
				http::header::AUTHORIZATION,
				http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
			);
		}

		// Attempt to send the request.
		match self.service.clone().call(request).await {
			Ok(response) => Ok(Some(response)),
			Err(Error::Disconnected) => Ok(None),
			Err(Error::Error(error)) => Err(error),
		}
	}

	pub(crate) async fn send(
		&self,
		request: impl Fn() -> http::Request<Body>,
	) -> tg::Result<http::Response<Body>> {
		loop {
			let request = request();
			if let Some(response) = self.try_send(request).await? {
				return Ok(response);
			}
		}
	}
}

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Error::Error(error) => write!(f, "{error}"),
			Error::Disconnected => write!(f, "service disconnected"),
		}
	}
}

impl std::error::Error for Error {}
