use {
	crate::prelude::*,
	futures::TryFutureExt as _,
	std::{
		ops::ControlFlow,
		path::{Path, PathBuf},
		str::FromStr,
		sync::Arc,
		time::Duration,
	},
	tangram_http::Body,
	tangram_uri::Uri,
	time::format_description::well_known::Rfc3339,
	tokio::net::{TcpStream, UnixStream},
	tower::{Service as _, util::BoxCloneSyncService},
	tower_http::ServiceBuilderExt as _,
};

pub(crate) type Sender =
	Arc<tokio::sync::Mutex<Option<hyper::client::conn::http2::SendRequest<Body>>>>;

pub(crate) type Service = BoxCloneSyncService<http::Request<Body>, http::Response<Body>, tg::Error>;

impl tg::Client {
	#[expect(clippy::needless_pass_by_value)]
	pub(crate) fn service(
		url: Uri,
		version: String,
		token: Option<String>,
		reconnect: &tangram_futures::retry::Options,
	) -> (Sender, Service) {
		let sender = Arc::new(tokio::sync::Mutex::new(
			None::<hyper::client::conn::http2::SendRequest<Body>>,
		));
		let service = tower::service_fn({
			let sender = sender.clone();
			let reconnect = reconnect.clone();
			move |request| {
				let url = url.clone();
				let sender = sender.clone();
				let reconnect = reconnect.clone();
				async move {
					let mut guard = sender.lock().await;
					let mut sender = match guard.as_ref() {
						Some(sender) if sender.is_ready() => sender.clone(),
						_ => {
							let sender = tangram_futures::retry(&reconnect, || async {
								match Self::connect_h2(&url).await {
									Ok(sender) => Ok(ControlFlow::Break(sender)),
									Err(error) => Ok(ControlFlow::Continue(error)),
								}
							})
							.await?;
							guard.replace(sender.clone());
							sender
						},
					};
					drop(guard);
					sender
						.send_request(request)
						.map_ok(|response| response.map(Body::new))
						.map_err(|source| tg::error!(!source, "failed to send the request"))
						.await
				}
			}
		});
		let service = tower::ServiceBuilder::new()
			.layer(tangram_http::layer::tracing::TracingLayer::new())
			.map_err(tg::Error::from)
			.layer(tower::timeout::TimeoutLayer::new(Duration::from_secs(60)))
			.insert_request_header_if_not_present(
				http::HeaderName::from_str("x-tg-compatibility-date").unwrap(),
				http::HeaderValue::from_str(&Self::compatibility_date().format(&Rfc3339).unwrap())
					.unwrap(),
			)
			.insert_request_header_if_not_present(
				http::HeaderName::from_str("x-tg-version").unwrap(),
				http::HeaderValue::from_str(&version).unwrap(),
			)
			.option_layer(token.map(|token| {
				tower_http::set_header::SetRequestHeaderLayer::if_not_present(
					http::header::AUTHORIZATION,
					http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
				)
			}))
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
				let sender = tangram_futures::retry(&self.reconnect, || async {
					match Self::connect_h2(&self.url).await {
						Ok(sender) => Ok(ControlFlow::Break(sender)),
						Err(error) => Ok(ControlFlow::Continue(error)),
					}
				})
				.await?;
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

	pub(crate) async fn send(
		&self,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>> {
		let future = self.service.clone().call(request);
		let response = future
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		let response = response.map(Into::into);
		Ok(response)
	}
}
