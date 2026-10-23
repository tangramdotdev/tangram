use {
	crate::{Context, Origin, Server},
	futures::{FutureExt as _, future},
	std::{
		convert::Infallible,
		path::Path,
		pin::{Pin, pin},
		task::Poll,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_http::{
		body::{Boxed as BoxBody, Ext as _},
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_uri::Uri,
	tokio::io::{AsyncRead, AsyncWrite, ReadBuf},
	tower::ServiceExt as _,
};

pub mod frame;

type Service =
	tower::util::BoxCloneSyncService<http::Request<BoxBody>, http::Response<BoxBody>, Infallible>;

pub enum Listener {
	Tcp(tokio::net::TcpListener),
	Unix {
		listener: tokio::net::UnixListener,
		guard: tangram_util::io::unix::Guard,
	},
	#[cfg(feature = "vsock")]
	Vsock(tokio_vsock::VsockListener),
}

pub(crate) enum Stream {
	Stdio(tokio::io::Join<tokio::io::Stdin, tokio::io::Stdout>),
	Tcp(tokio::net::TcpStream),
	Unix(tokio::net::UnixStream),
	#[cfg(feature = "vsock")]
	Vsock(tokio_vsock::VsockStream),
}

/// Create a response for a request that does not match a route.
#[must_use]
pub fn not_found() -> http::Response<BoxBody> {
	http::Response::builder()
		.status(http::StatusCode::NOT_FOUND)
		.bytes("not found")
		.unwrap()
		.boxed_body()
}

impl Server {
	pub(crate) async fn connect(url: &Uri) -> tg::Result<Stream> {
		let stream =
			match url.scheme() {
				Some("http+stdio") => {
					Stream::Stdio(tokio::io::join(tokio::io::stdin(), tokio::io::stdout()))
				},
				Some("http") => {
					let host = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
					let port = url
						.port_or_known_default()
						.ok_or_else(|| tg::error!(%url, "invalid url"))?
						.try_into()
						.map_err(|_| tg::error!("invalid port"))?;
					let stream = tokio::net::TcpStream::connect((host, port))
						.await
						.map_err(|error| tg::error!(!error, "failed to connect to the socket"))?;
					stream.set_nodelay(true).map_err(|error| {
						tg::error!(!error, "failed to set nodelay on the socket")
					})?;
					Stream::Tcp(stream)
				},
				Some("http+unix") => {
					let path = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
					let path = Path::new(path);
					let path_ = tangram_util::io::unix::resolve(path).map_err(
						|error| tg::error!(!error, path = %path.display(), "failed to resolve the socket path"),
					)?;
					Stream::Unix(
						tokio::net::UnixStream::connect(path_.as_ref())
							.await
							.map_err(
								|error| tg::error!(!error, path = %path.display(), "failed to connect to the socket"),
							)?,
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
							.map_err(|error| tg::error!(!error, %url, "invalid url"))?;
						let port = url.port().ok_or_else(|| tg::error!(%url, "invalid url"))?;
						let addr = tokio_vsock::VsockAddr::new(cid, port);
						Stream::Vsock(tokio_vsock::VsockStream::connect(addr).await.map_err(
							|error| tg::error!(!error, "failed to connect to the socket"),
						)?)
					}
				},
				_ => return Err(tg::error!(%url, "invalid url")),
			};
		Ok(stream)
	}

	pub async fn listen(url: &Uri) -> tg::Result<Listener> {
		let listener = match url.scheme() {
			Some("http+stdio") => return Err(tg::error!(%url, "cannot listen on stdio")),
			Some("http") => {
				let host = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let listener = tokio::net::TcpListener::bind(format!("{host}:{port}"))
					.await
					.map_err(|error| tg::error!(!error, "failed to bind"))?;
				Listener::Tcp(listener)
			},
			Some("https") => {
				let host = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let listener = tokio::net::TcpListener::bind(format!("{host}:{port}"))
					.await
					.map_err(|error| tg::error!(!error, "failed to bind"))?;
				Listener::Tcp(listener)
			},
			Some("http+unix") => {
				let path = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let path = Path::new(path);
				let (listener, guard) = tangram_util::io::unix::bind_listener(path).map_err(
					|error| tg::error!(!error, path = %path.display(), "failed to bind"),
				)?;
				Listener::Unix { listener, guard }
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
						.map_err(|error| tg::error!(!error, %url, "invalid url"))?;
					let port = url.port().ok_or_else(|| tg::error!(%url, "invalid url"))?;
					let addr = tokio_vsock::VsockAddr::new(cid, port);
					let listener = tokio_vsock::VsockListener::bind(addr)
						.map_err(|error| tg::error!(!error, "failed to bind"))?;
					Listener::Vsock(listener)
				}
			},
			_ => {
				return Err(tg::error!("invalid url"));
			},
		};
		Ok(listener)
	}

	pub async fn serve(
		&self,
		listener: Listener,
		config: crate::config::HttpListener,
		origin: Origin,
		stopper: Stopper,
	) {
		#[cfg(feature = "tls")]
		let tls = if matches!(config.url.scheme(), Some("https")) {
			let Some(tls_config) = config.tls.as_ref() else {
				tracing::error!(url = %config.url, "missing tls configuration");
				return;
			};
			match Self::create_tls_acceptor(tls_config).await {
				Ok(tls) => Some(tls),
				Err(error) => {
					tracing::error!(
						error = %error.trace(),
						url = %config.url,
						"failed to create the TLS acceptor"
					);
					return;
				},
			}
		} else {
			None
		};

		// Create the task tracker.
		let task_tracker = tokio_util::task::TaskTracker::new();
		let idle_timeout = self.http_idle_timeout();
		let http2 = self.config().http.http2;

		// Create the active connections counter.
		let active_connections = opentelemetry::global::meter("tangram_http")
			.i64_up_down_counter("http.connections.active")
			.with_description("Number of active HTTP connections")
			.build();

		let service = self.service(origin, stopper.clone());

		loop {
			// Accept a new connection.
			let accept = async {
				let stream = match &listener {
					Listener::Tcp(listener) => {
						let (stream, _) = listener.accept().await?;
						stream.set_nodelay(true)?;
						Stream::Tcp(stream)
					},
					Listener::Unix { listener, .. } => Stream::Unix(listener.accept().await?.0),
					#[cfg(feature = "vsock")]
					Listener::Vsock(listener) => Stream::Vsock(listener.accept().await?.0),
				};
				Ok::<_, std::io::Error>(stream)
			};
			let stream = match future::select(pin!(accept), pin!(stopper.wait())).await {
				future::Either::Left((result, _)) => match result {
					Ok(stream) => stream,
					Err(error) => {
						tracing::error!(?error, "failed to accept a connection");
						continue;
					},
				},
				future::Either::Right(((), _)) => {
					break;
				},
			};

			// Spawn a task to serve the connection.
			let guard = {
				active_connections.add(1, &[]);
				let active_connections = active_connections.clone();
				scopeguard::guard((), move |()| {
					active_connections.add(-1, &[]);
				})
			};
			task_tracker.spawn({
				let service = service.clone();
				let stopper = stopper.clone();
				#[cfg(feature = "tls")]
				let tls = tls.clone();
				async move {
					match stream {
						Stream::Stdio(stream) => {
							Server::serve_connection(stream, service, idle_timeout, http2, stopper)
								.await;
						},
						Stream::Tcp(stream) => {
							#[cfg(feature = "tls")]
							if let Some(tls) = tls {
								match tls.accept(stream).await {
									Ok(stream) => {
										Server::serve_connection(
											stream,
											service,
											idle_timeout,
											http2,
											stopper,
										)
										.await;
									},
									Err(error) => {
										tracing::error!(
											?error,
											"failed to perform the TLS handshake"
										);
									},
								}
							} else {
								Server::serve_connection(
									stream,
									service,
									idle_timeout,
									http2,
									stopper,
								)
								.await;
							}
							#[cfg(not(feature = "tls"))]
							Server::serve_connection(stream, service, idle_timeout, http2, stopper)
								.await;
						},
						Stream::Unix(stream) => {
							Server::serve_connection(stream, service, idle_timeout, http2, stopper)
								.await;
						},
						#[cfg(feature = "vsock")]
						Stream::Vsock(stream) => {
							Server::serve_connection(stream, service, idle_timeout, http2, stopper)
								.await;
						},
					}
					drop(guard);
				}
			});
		}

		// Log that the listener stopped accepting connections.
		tracing::info!("stopped listening on {}", config.url);

		// Wait for all tasks to finish.
		task_tracker.close();
		task_tracker.wait().await;
	}

	fn service(&self, origin: Origin, stopper: Stopper) -> Service {
		let builder = tower::ServiceBuilder::new()
			.layer(tangram_http::layer::metrics::MetricsLayer::new())
			.layer(tangram_http::layer::tracing::TracingLayer::new())
			.map_response(|response: http::Response<BoxBody>| {
				if response.status() == http::StatusCode::REQUEST_TIMEOUT {
					request_timeout_response()
				} else {
					response
				}
			})
			.layer(tower_http::timeout::TimeoutLayer::with_status_code(
				http::StatusCode::REQUEST_TIMEOUT,
				self.config.http.request_timeout,
			))
			.layer(tangram_http::layer::compression::RequestDecompressionLayer)
			.layer(
				tangram_http::layer::coalescing::ResponseCoalescingLayer::new(
					self.config.http.coalescing_target_size,
				),
			)
			.layer(
				tangram_http::layer::compression::ResponseCompressionLayer::new(
					|accept_encoding, parts, _| {
						let has_content_length =
							parts.headers.get(http::header::CONTENT_LENGTH).is_some();
						let is_sync = parts.headers.get(http::header::CONTENT_TYPE).is_some_and(
							|content_type| {
								matches!(content_type.to_str(), Ok(tg::sync::CONTENT_TYPE))
							},
						);
						if (has_content_length || is_sync)
							&& accept_encoding.is_some_and(|accept_encoding| {
								accept_encoding.preferences.iter().any(|preference| {
									preference.encoding == tangram_http::header::content_encoding::ContentEncoding::Zstd
								})
							}) {
							Some((tangram_http::body::compression::Algorithm::Zstd, 3))
						} else {
							None
						}
					},
				),
			);

		Service::new(builder.service_fn({
			let server = self.clone();
			move |mut request: http::Request<BoxBody>| {
				let origin = origin;
				let server = server.clone();
				let stopper = stopper.clone();
				async move {
					// Insert the max arg length, so that the request parser can bound the arg in the body.
					let max_length =
						tangram_http::body::arg::MaxLength(server.config().http.max_arg_length);
					request.extensions_mut().insert(max_length);

					let context = Context {
						billing_ready: false,
						id: None,
						local_process_control: false,
						origin,
						principal: tg::Principal::Anonymous,
						process: None,
						sandbox: None,
						stopper: Some(stopper),
						token: None,
					};
					let response = server.handle_request(request, context).await;
					Ok::<_, Infallible>(response)
				}
			}
		}))
	}

	pub(crate) async fn serve_stream<S>(&self, stream: S, origin: Origin, stopper: Stopper)
	where
		S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
	{
		let service = self.service(origin, stopper.clone());
		let idle_timeout = self.http_idle_timeout();
		let http2 = self.config().http.http2;
		Server::serve_connection(stream, service, idle_timeout, http2, stopper).await;
	}

	fn http_idle_timeout(&self) -> Duration {
		self.config().http.idle_timeout
	}

	async fn serve_connection<S, T>(
		stream: S,
		service: T,
		idle_timeout: Duration,
		http2: tangram_http::http2::Config,
		stopper: Stopper,
	) where
		S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
		T: tower::Service<
				http::Request<BoxBody>,
				Response = http::Response<BoxBody>,
				Error = Infallible,
			> + Clone
			+ Send
			+ 'static,
		T::Future: Send + 'static,
	{
		let idle = tangram_http::idle::Idle::new(idle_timeout);
		let executor = hyper_util::rt::TokioExecutor::new();
		let mut builder = hyper_util::server::conn::auto::Builder::new(executor);
		builder
			.http2()
			.adaptive_window(false)
			.initial_connection_window_size(http2.connection_window_size)
			.initial_stream_window_size(http2.stream_window_size)
			.max_concurrent_streams(http2.max_concurrent_streams)
			.max_pending_accept_reset_streams(None)
			.max_local_error_reset_streams(None);
		let service = service
			.map_request(|request: http::Request<hyper::body::Incoming>| request.boxed_body())
			.map_future({
				let idle = idle.clone();
				move |future: T::Future| {
					let token = idle.token();
					async move {
						let response = future.await?;
						let response = response.map(move |body| {
							BoxBody::new(tangram_http::idle::Body::new(token, body).map_err(
								|error| {
									tracing::error!(?error, "response body error");
									error
								},
							))
						});
						Ok::<_, Infallible>(response)
					}
				}
			});
		let service = hyper_util::service::TowerToHyperService::new(service);
		let stream = hyper_util::rt::TokioIo::new(stream);
		let connection = builder.serve_connection_with_upgrades(stream, service);

		let result = match future::select(
			pin!(connection),
			future::select(pin!(idle.wait()), pin!(stopper.wait())),
		)
		.await
		{
			future::Either::Left((result, _)) => result,
			future::Either::Right((_, mut connection)) => {
				connection.as_mut().graceful_shutdown();
				connection.await
			},
		};
		result
			.inspect_err(|error| {
				tracing::trace!(?error, "connection failed");
			})
			.ok();
	}

	#[cfg(feature = "tls")]
	pub(crate) async fn create_tls_acceptor(
		config: &crate::config::HttpTls,
	) -> tg::Result<tokio_rustls::TlsAcceptor> {
		let certificates = tokio::fs::read(&config.certificate)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					path = %config.certificate.display(),
					"failed to read the certificate file"
				)
			})?;
		let certificates = {
			let mut reader = std::io::BufReader::new(certificates.as_slice());
			rustls_pemfile::certs(&mut reader)
				.collect::<Result<Vec<_>, _>>()
				.map_err(|error| {
					tg::error!(
						!error,
						path = %config.certificate.display(),
						"failed to parse the certificate file"
					)
				})?
		};
		if certificates.is_empty() {
			return Err(tg::error!(
				path = %config.certificate.display(),
				"missing certificates in the certificate file"
			));
		}

		let private_key = tokio::fs::read(&config.key).await.map_err(|error| {
			tg::error!(
				!error,
				path = %config.key.display(),
				"failed to read the private key file"
			)
		})?;
		let private_key = {
			let mut reader = std::io::BufReader::new(private_key.as_slice());
			rustls_pemfile::private_key(&mut reader)
				.map_err(|error| {
					tg::error!(
						!error,
						path = %config.key.display(),
						"failed to parse the private key file"
					)
				})?
				.ok_or_else(|| {
					tg::error!(
						path = %config.key.display(),
						"missing private key in the private key file"
					)
				})?
		};

		let mut config = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(
			rustls::crypto::aws_lc_rs::default_provider(),
		))
		.with_safe_default_protocol_versions()
		.map_err(|error| tg::error!(!error, "failed to create the tls config"))?
		.with_no_client_auth()
		.with_single_cert(certificates, private_key)
		.map_err(|error| tg::error!(!error, "failed to create the tls config"))?;
		config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

		let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config));

		Ok(acceptor)
	}

	#[tracing::instrument(
		fields(id, method, origin, path),
		level = "trace",
		name = "request",
		skip_all
	)]
	async fn handle_request(
		&self,
		request: http::Request<BoxBody>,
		mut context: Context,
	) -> http::Response<BoxBody> {
		let id = tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes());
		context.id = Some(id.clone());

		let span = tracing::Span::current();
		span.record("id", id.as_str());
		span.record("method", request.method().as_str());
		span.record("origin", tracing::field::debug(&context.origin));
		span.record("path", request.uri().path());

		let method = request.method().clone();
		let path = request.uri().path().to_owned();

		// Authenticate, verifying a runner's claim to act for one of its processes.
		let token = request.token(None).map(str::to_owned);
		let process = request
			.headers()
			.get(tg::process::PROCESS_HEADER)
			.map(|value| {
				value
					.to_str()
					.ok()
					.and_then(|value| value.parse::<tg::process::Id>().ok())
					.ok_or_else(|| tg::error!(argument, "failed to parse the process header"))
			})
			.transpose();
		let sandbox = request
			.headers()
			.get(tg::sandbox::SANDBOX_HEADER)
			.map(|value| {
				value
					.to_str()
					.ok()
					.and_then(|value| value.parse::<tg::sandbox::Id>().ok())
					.ok_or_else(|| tg::error!(argument, "failed to parse the sandbox header"))
			})
			.transpose();
		let result = match (process, sandbox) {
			(Ok(process), Ok(sandbox)) => self
				.components
				.authentication
				.authenticate_with_claim(
					self,
					&context,
					token.as_deref(),
					process.clone(),
					sandbox.clone(),
				)
				.await
				.map(|authentication| (authentication, process, sandbox)),
			(Err(error), _) | (_, Err(error)) => Err(error),
		};
		context.token = token;
		let authentication = match result {
			Ok(authentication) => authentication,
			Err(error) => {
				let status = match error.kind() {
					Some(tg::error::Kind::Argument) => http::StatusCode::BAD_REQUEST,
					Some(tg::error::Kind::Internal) | None => {
						http::StatusCode::INTERNAL_SERVER_ERROR
					},
					Some(tg::error::Kind::Missing) => http::StatusCode::NOT_FOUND,
					Some(tg::error::Kind::Unauthenticated) => http::StatusCode::UNAUTHORIZED,
					Some(tg::error::Kind::Unauthorized) => http::StatusCode::FORBIDDEN,
					Some(tg::error::Kind::Unavailable) => http::StatusCode::SERVICE_UNAVAILABLE,
				};
				let bytes = match error.to_data_or_id() {
					tg::Either::Left(data) => serde_json::to_string(&data).unwrap(),
					tg::Either::Right(id) => id.to_string(),
				};
				let response = http::Response::builder()
					.status(status)
					.bytes(bytes)
					.unwrap()
					.boxed_body();
				return response;
			},
		};
		let (authentication, process, sandbox) = authentication;
		context.billing_ready = authentication.billing_ready.unwrap_or(false);
		context.principal = authentication.principal;
		context.process = process;
		context.sandbox = sandbox;

		let session = self.session(&context);

		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		let response = match (method, path_components.as_slice()) {
			(_, ["_", ..]) => session.server.components.get.request(&session, request),
			(_, ["check", ..]) => session.server.components.check.request(&session, request),
			(http::Method::POST, ["checkin"]) => session
				.server
				.components
				.checkin
				.checkin_request(&session, request),
			(_, ["checkout", ..]) => session
				.server
				.components
				.checkout
				.request(&session, request),
			(_, ["checkpoints", ..]) => session
				.server
				.components
				.checkpoint
				.request(&session, request),
			(_, ["children", ..]) => session
				.server
				.components
				.children
				.request(&session, request),
			(_, ["clean", ..]) => session.server.components.clean.request(&session, request),
			(_, ["clock", ..]) => session.server.components.clock.request(&session, request),
			(_, ["document", ..]) => session
				.server
				.components
				.document
				.request(&session, request),
			(_, ["format", ..]) => session.server.components.format.request(&session, request),
			(_, ["grants", ..]) => session.server.components.grant.request(&session, request),
			(_, ["groups", ..]) => session.server.components.group.request(&session, request),
			(_, ["health", ..]) => session.server.components.health.request(&session, request),
			(_, ["index"]) => session.server.components.indexer.request(&session, request),
			(_, ["list", ..]) => session.server.components.list.request(&session, request),
			(
				_,
				[
					"logins" | "login" | "user" | "logout" | "oauth" | "users",
					..,
				],
			) => session.server.components.user.request(&session, request),
			(_, ["lsp"]) => session.server.components.lsp.request(&session, request),
			(_, ["match", ..]) => session.server.components.match_.request(&session, request),
			(_, ["modules", ..]) => session.server.components.module.request(&session, request),
			(_, ["objects", ..]) => session.server.components.object.request(&session, request),
			(_, ["organizations", ..]) => session
				.server
				.components
				.organization
				.request(&session, request),
			(_, ["processes", ..]) => session.server.components.process.request(&session, request),
			(_, ["pull", ..]) => session.server.components.pull.request(&session, request),
			(_, ["push", ..]) => session.server.components.push.request(&session, request),
			(_, ["read", ..]) => session.server.components.read.request(&session, request),
			(_, ["remotes", ..]) => session.server.components.remote.request(&session, request),
			(_, ["runners", ..]) => session.server.components.runner.request(&session, request),
			(_, ["sandboxes", ..]) => session.server.components.sandbox.request(&session, request),
			(_, ["sync", ..]) => session.server.components.sync.request(&session, request),
			(_, ["tags", ..]) => session.server.components.tag.request(&session, request),
			(_, ["watches", ..]) => session.server.components.watch.request(&session, request),
			(_, ["webhooks", "stripe"]) => {
				session.server.components.billing.request(&session, request)
			},
			(_, ["write", ..]) => session.server.components.write.request(&session, request),
			(_, _) => future::ok(not_found()).boxed(),
		}
		.await;

		// Handle an error.
		let mut response = response.unwrap_or_else(|error| {
			tracing::error!(error = %error.trace());
			let status = match error.kind() {
				Some(tg::error::Kind::Argument) => http::StatusCode::BAD_REQUEST,
				Some(tg::error::Kind::Internal) | None => http::StatusCode::INTERNAL_SERVER_ERROR,
				Some(tg::error::Kind::Missing) => http::StatusCode::NOT_FOUND,
				Some(tg::error::Kind::Unauthenticated) => http::StatusCode::UNAUTHORIZED,
				Some(tg::error::Kind::Unauthorized) => http::StatusCode::FORBIDDEN,
				Some(tg::error::Kind::Unavailable) => http::StatusCode::SERVICE_UNAVAILABLE,
			};
			let bytes = match error.to_data_or_id() {
				tg::Either::Left(data) => serde_json::to_string(&data).unwrap(),
				tg::Either::Right(id) => id.to_string(),
			};
			http::Response::builder()
				.status(status)
				.bytes(bytes)
				.unwrap()
				.boxed_body()
		});

		// Add the request ID to the response.
		let key = http::HeaderName::from_static("x-tg-request-id");
		let value = http::HeaderValue::from_str(&id).unwrap();
		response.headers_mut().insert(key, value);

		response
	}
}

impl AsyncRead for Stream {
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		match self.as_mut().get_mut() {
			Self::Stdio(stream) => Pin::new(stream).poll_read(cx, buf),
			Self::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
			Self::Unix(stream) => Pin::new(stream).poll_read(cx, buf),
			#[cfg(feature = "vsock")]
			Self::Vsock(stream) => Pin::new(stream).poll_read(cx, buf),
		}
	}
}

impl AsyncWrite for Stream {
	fn poll_write(
		mut self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		match self.as_mut().get_mut() {
			Self::Stdio(stream) => Pin::new(stream).poll_write(cx, buf),
			Self::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
			Self::Unix(stream) => Pin::new(stream).poll_write(cx, buf),
			#[cfg(feature = "vsock")]
			Self::Vsock(stream) => Pin::new(stream).poll_write(cx, buf),
		}
	}

	fn poll_flush(
		mut self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<()>> {
		match self.as_mut().get_mut() {
			Self::Stdio(stream) => Pin::new(stream).poll_flush(cx),
			Self::Tcp(stream) => Pin::new(stream).poll_flush(cx),
			Self::Unix(stream) => Pin::new(stream).poll_flush(cx),
			#[cfg(feature = "vsock")]
			Self::Vsock(stream) => Pin::new(stream).poll_flush(cx),
		}
	}

	fn poll_shutdown(
		mut self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<()>> {
		match self.as_mut().get_mut() {
			Self::Stdio(stream) => Pin::new(stream).poll_shutdown(cx),
			Self::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
			Self::Unix(stream) => Pin::new(stream).poll_shutdown(cx),
			#[cfg(feature = "vsock")]
			Self::Vsock(stream) => Pin::new(stream).poll_shutdown(cx),
		}
	}
}

fn request_timeout_response() -> http::Response<BoxBody> {
	let error = tg::error!("the request timed out");
	let bytes = match error.to_data_or_id() {
		tg::Either::Left(data) => serde_json::to_string(&data).unwrap(),
		tg::Either::Right(id) => id.to_string(),
	};
	http::Response::builder()
		.status(http::StatusCode::REQUEST_TIMEOUT)
		.bytes(bytes)
		.unwrap()
		.boxed_body()
}
