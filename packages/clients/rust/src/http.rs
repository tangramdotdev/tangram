#[cfg(feature = "tls")]
use rustls_platform_verifier::BuilderVerifierExt as _;
use {
	crate::prelude::*,
	std::{
		error::Error as _, ops::ControlFlow, path::Path, str::FromStr, sync::Arc, time::Duration,
	},
	tangram_http::request::Ext as _,
	tangram_uri::Uri,
	time::format_description::well_known::Rfc3339,
	tokio::io::{AsyncRead, AsyncWrite},
	tower::{Service as _, util::BoxCloneSyncService},
	tower_http::ServiceBuilderExt as _,
};

pub(crate) type Sender = Arc<
	tokio::sync::Mutex<Option<hyper::client::conn::http2::SendRequest<tangram_http::body::Boxed>>>,
>;

pub(crate) type Service = BoxCloneSyncService<
	http::Request<tangram_http::body::Boxed>,
	http::Response<tangram_http::body::Boxed>,
	Error,
>;

#[derive(Debug, derive_more::Display, derive_more::Error)]
pub(crate) enum Error {
	Hyper(hyper::Error),
	Other(tg::Error),
}

impl tg::Client {
	pub(crate) fn service(arg: &tg::Arg, sender: &Sender) -> Service {
		let url = arg.url.clone().unwrap();
		let version = arg.version.clone().unwrap();
		let token = arg.token.clone();
		let process = arg.process.clone();
		let reconnect = arg.reconnect.as_ref().unwrap();
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
							let sender = tangram_futures::retry(&reconnect, || {
								let url = url.clone();
								async move {
									match Self::connect_h2(&url).await {
										Ok(sender) => Ok(ControlFlow::Break(sender)),
										Err(error) => Ok(ControlFlow::Continue(error)),
									}
								}
							})
							.await
							.map_err(Error::Other)?;
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
			.option_layer(process.map(|process| {
				tower_http::set_header::SetRequestHeaderLayer::if_not_present(
					http::HeaderName::from_static("x-tg-process"),
					http::HeaderValue::from_str(&process.to_string()).unwrap(),
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
		Service::new(service)
	}

	pub async fn connect(&self) -> tg::Result<()> {
		let mut guard = self.sender.lock().await;
		match guard.as_ref() {
			Some(sender) if sender.is_ready() => (),
			_ => {
				let sender =
					tangram_futures::retry(self.arg.reconnect.as_ref().unwrap(), || async {
						match Self::connect_h2(self.arg.url.as_ref().unwrap()).await {
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
					.ok_or_else(|| tg::error!(%url, "invalid url"))?;
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
						.ok_or_else(|| tg::error!(%url, "invalid url"))?;
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
						.map_err(|source| tg::error!(!source, %url, "invalid url"))?;
					let port = url.port().ok_or_else(|| tg::error!(%url, "invalid url"))?;
					let addr = tokio_vsock::VsockAddr::new(cid, u32::from(port));
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
					.ok_or_else(|| tg::error!("invalid url"))?;
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
						.ok_or_else(|| tg::error!(%url, "invalid url"))?;
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
						.map_err(|source| tg::error!(!source, %url, "invalid url"))?;
					let port = url.port().ok_or_else(|| tg::error!(%url, "invalid url"))?;
					let addr = tokio_vsock::VsockAddr::new(cid, u32::from(port));
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

	pub(crate) async fn connect_tcp(host: &str, port: u16) -> tg::Result<tokio::net::TcpStream> {
		let addr = format!("{host}:{port}");
		tokio::time::timeout(Duration::from_secs(1), tokio::net::TcpStream::connect(addr))
			.await
			.map_err(|_| tg::error!("connection timeout"))?
			.map_err(|source| tg::error!(!source, "failed to create the TCP connection"))
	}

	#[cfg(feature = "tls")]
	pub(crate) async fn connect_tcp_tls(
		host: &str,
		port: u16,
		protocols: Vec<Vec<u8>>,
	) -> tg::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
		let stream = Self::connect_tcp(host, port).await?;

		// Create the connector.
		let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
			rustls::crypto::aws_lc_rs::default_provider(),
		))
		.with_safe_default_protocol_versions()
		.unwrap()
		.with_platform_verifier()
		.map_err(|source| tg::error!(!source, "failed to create the tls config"))?
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

	async fn connect_unix(path: &Path) -> tg::Result<tokio::net::UnixStream> {
		tokio::net::UnixStream::connect(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))
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
		.map_err(|source| tg::error!(!source, "timed out connecting to the socket"))?
		.map_err(|source| tg::error!(!source, "failed to connect to the socket"))
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
			Error::Hyper(source) => tg::error!(!source, "failed to send the request"),
			Error::Other(error) => error,
		})?;
		let response = response.map(Into::into);
		Ok(response)
	}

	pub(crate) async fn send_with_retry<B>(
		&self,
		request: http::Request<B>,
	) -> tg::Result<http::Response<tangram_http::body::Boxed>>
	where
		B: http_body::Body<Data = bytes::Bytes> + Clone + Send + Unpin + 'static,
		B::Error: Into<tangram_http::Error> + Clone + Send,
	{
		let client = self.clone();
		tangram_futures::retry(self.arg.retry.as_ref().unwrap(), || {
			let client = client.clone();
			let request = request.clone();
			async move {
				let request = request.boxed_body();
				let future = self.service.clone().call(request);
				match future.await {
					Ok(response) => {
						let response = response.map(Into::into);
						Ok(ControlFlow::Break(response))
					},
					Err(Error::Hyper(error)) if is_retryable_error(&error) => {
						client.disconnect().await;
						Ok(ControlFlow::Continue(tg::error!(
							!error,
							"failed to send the request"
						)))
					},
					Err(error) => Err(match error {
						Error::Hyper(source) => {
							tg::error!(!source, "failed to send the request")
						},
						Error::Other(error) => error,
					}),
				}
			}
		})
		.await
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
