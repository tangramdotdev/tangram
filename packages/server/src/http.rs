use {
	crate::{Context, Server},
	futures::{FutureExt as _, future},
	std::{
		convert::Infallible,
		path::Path,
		pin::{Pin, pin},
		sync::Arc,
		task::Poll,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_http::{
		body::Boxed as BoxBody, body::Ext as _, request::Ext as _, response::Ext as _,
		response::builder::Ext as _,
	},
	tangram_uri::Uri,
	tokio::io::{AsyncRead, AsyncWrite, ReadBuf},
	tower::ServiceExt as _,
};

pub(crate) enum Listener {
	Tcp(tokio::net::TcpListener),
	Unix(tokio::net::UnixListener),
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

type Service =
	tower::util::BoxCloneSyncService<http::Request<BoxBody>, http::Response<BoxBody>, Infallible>;

impl Server {
	pub(crate) async fn connect(url: &Uri) -> tg::Result<Stream> {
		let stream = match url.scheme() {
			Some("http+stdio") => {
				Stream::Stdio(tokio::io::join(tokio::io::stdin(), tokio::io::stdout()))
			},
			Some("http") => {
				let host = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!(%url, "invalid url"))?;
				Stream::Tcp(
					tokio::net::TcpStream::connect((host, port))
						.await
						.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?,
				)
			},
			Some("http+unix") => {
				let path = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				Stream::Unix(
					tokio::net::UnixStream::connect(path)
						.await
						.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?,
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
						.map_err(|source| tg::error!(!source, %url, "invalid url"))?;
					let port = url.port().ok_or_else(|| tg::error!(%url, "invalid url"))?;
					let addr = tokio_vsock::VsockAddr::new(cid, u32::from(port));
					Stream::Vsock(
						tokio_vsock::VsockStream::connect(addr)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to connect to the socket")
							})?,
					)
				}
			},
			_ => return Err(tg::error!(%url, "invalid url")),
		};
		Ok(stream)
	}

	pub(crate) async fn listen(url: &Uri) -> tg::Result<Listener> {
		let listener = match url.scheme() {
			Some("http+stdio") => return Err(tg::error!(%url, "cannot listen on stdio")),
			Some("http") => {
				let host = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let listener = tokio::net::TcpListener::bind(format!("{host}:{port}"))
					.await
					.map_err(|source| tg::error!(!source, "failed to bind"))?;
				Listener::Tcp(listener)
			},
			Some("https") => {
				let host = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let listener = tokio::net::TcpListener::bind(format!("{host}:{port}"))
					.await
					.map_err(|source| tg::error!(!source, "failed to bind"))?;
				Listener::Tcp(listener)
			},
			Some("http+unix") => {
				let path = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let path = Path::new(path);
				let listener = tokio::net::UnixListener::bind(path).map_err(
					|source| tg::error!(!source, path = %path.display(), "failed to bind"),
				)?;
				Listener::Unix(listener)
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
						.map_err(|source| tg::error!(!source, %url, "invalid url"))?;
					let port = url.port().ok_or_else(|| tg::error!(%url, "invalid url"))?;
					let addr = tokio_vsock::VsockAddr::new(cid, u32::from(port));
					let listener = tokio_vsock::VsockListener::bind(addr)
						.map_err(|source| tg::error!(!source, "failed to bind"))?;
					Listener::Vsock(listener)
				}
			},
			_ => {
				return Err(tg::error!("invalid url"));
			},
		};
		Ok(listener)
	}

	pub(crate) async fn serve(
		&self,
		listener: Listener,
		config: crate::config::HttpListener,
		process: Option<Arc<crate::context::Process>>,
		sandbox: Option<tg::sandbox::Id>,
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

		// Create the active connections counter.
		let active_connections = opentelemetry::global::meter("tangram_http")
			.i64_up_down_counter("http.connections.active")
			.with_description("Number of active HTTP connections")
			.build();

		let service = self.service(process, sandbox, stopper.clone());

		loop {
			// Accept a new connection.
			let accept = async {
				let stream = match &listener {
					Listener::Tcp(listener) => Stream::Tcp(listener.accept().await?.0),
					Listener::Unix(listener) => Stream::Unix(listener.accept().await?.0),
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
							Server::serve_connection(stream, service, stopper).await;
						},
						Stream::Tcp(stream) => {
							#[cfg(feature = "tls")]
							if let Some(tls) = tls {
								match tls.accept(stream).await {
									Ok(stream) => {
										Server::serve_connection(stream, service, stopper).await;
									},
									Err(error) => {
										tracing::error!(
											?error,
											"failed to perform the TLS handshake"
										);
									},
								}
							} else {
								Server::serve_connection(stream, service, stopper).await;
							}
							#[cfg(not(feature = "tls"))]
							Server::serve_connection(stream, service, stopper).await;
						},
						Stream::Unix(stream) => {
							Server::serve_connection(stream, service, stopper).await;
						},
						#[cfg(feature = "vsock")]
						Stream::Vsock(stream) => {
							Server::serve_connection(stream, service, stopper).await;
						},
					}
					drop(guard);
				}
			});
		}

		// Wait for all tasks to finish.
		task_tracker.close();
		task_tracker.wait().await;
	}

	fn service(
		&self,
		process: Option<Arc<crate::context::Process>>,
		sandbox: Option<tg::sandbox::Id>,
		stopper: Stopper,
	) -> Service {
		let builder = tower::ServiceBuilder::new()
			.layer(tangram_http::layer::metrics::MetricsLayer::new())
			.layer(tangram_http::layer::tracing::TracingLayer::new())
			.layer(tower_http::timeout::TimeoutLayer::with_status_code(
				http::StatusCode::REQUEST_TIMEOUT,
				Duration::from_mins(1),
			))
			.layer(tangram_http::layer::compression::RequestDecompressionLayer)
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
			let handle = self.clone();
			move |request| {
				let handle = handle.clone();
				let process = process.clone();
				let sandbox = sandbox.clone();
				let stopper = stopper.clone();
				async move {
					let response = handle
						.handle_request(
							request,
							Context {
								process,
								sandbox,
								stopper: Some(stopper),
								..Default::default()
							},
						)
						.await;
					Ok::<_, Infallible>(response)
				}
			}
		}))
	}

	pub(crate) async fn serve_stream<S>(
		&self,
		stream: S,
		process: Option<Arc<crate::context::Process>>,
		sandbox: Option<tg::sandbox::Id>,
		stopper: Stopper,
	) where
		S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
	{
		let service = self.service(process, sandbox, stopper.clone());
		Server::serve_connection(stream, service, stopper).await;
	}

	async fn serve_connection<S, T>(stream: S, service: T, stopper: Stopper)
	where
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
		let idle = tangram_http::idle::Idle::new(Duration::from_secs(30));
		let executor = hyper_util::rt::TokioExecutor::new();
		let mut builder = hyper_util::server::conn::auto::Builder::new(executor);
		builder
			.http2()
			.max_concurrent_streams(None)
			.max_pending_accept_reset_streams(None)
			.max_local_error_reset_streams(None);
		let service = service
			.map_request(|request: http::Request<hyper::body::Incoming>| request.boxed_body())
			.map_response({
				let idle = idle.clone();
				move |response: http::Response<BoxBody>| {
					response.map(move |body| {
						BoxBody::new(tangram_http::idle::Body::new(idle.token(), body).map_err(
							|error| {
								tracing::error!(?error, "response body error");
								error
							},
						))
					})
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
			.map_err(|source| {
				tg::error!(
					!source,
					path = %config.certificate.display(),
					"failed to read the certificate file"
				)
			})?;
		let certificates = {
			let mut reader = std::io::BufReader::new(certificates.as_slice());
			rustls_pemfile::certs(&mut reader)
				.collect::<Result<Vec<_>, _>>()
				.map_err(|source| {
					tg::error!(
						!source,
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

		let private_key = tokio::fs::read(&config.key).await.map_err(|source| {
			tg::error!(
				!source,
				path = %config.key.display(),
				"failed to read the private key file"
			)
		})?;
		let private_key = {
			let mut reader = std::io::BufReader::new(private_key.as_slice());
			rustls_pemfile::private_key(&mut reader)
				.map_err(|source| {
					tg::error!(
						!source,
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
			rustls::crypto::ring::default_provider(),
		))
		.with_safe_default_protocol_versions()
		.map_err(|source| tg::error!(!source, "failed to create the tls config"))?
		.with_no_client_auth()
		.with_single_cert(certificates, private_key)
		.map_err(|source| tg::error!(!source, "failed to create the tls config"))?;
		config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

		let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config));

		Ok(acceptor)
	}

	#[tracing::instrument(level = "trace", name = "request", skip_all, fields(id, method, path))]
	async fn handle_request(
		&self,
		request: http::Request<BoxBody>,
		mut context: Context,
	) -> http::Response<BoxBody> {
		let id = tg::Id::new_uuidv7(tg::id::Kind::Request);
		context.id = Some(id.clone());
		context.token = request.token(None).map(ToOwned::to_owned);
		context.untrusted = true;

		let span = tracing::Span::current();
		span.record("id", id.to_string());
		span.record("method", request.method().as_str());
		span.record("path", request.uri().path());

		let method = request.method().clone();
		let path = request.uri().path().to_owned();

		if let Err(response) = self
			.handle_request_context_for_process(request.headers(), &mut context)
			.await
		{
			return response;
		}

		let handle = self.handle(context);

		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		let response = match (method, path_components.as_slice()) {
			(http::Method::POST, ["cache"]) => handle.cache_request(request).boxed(),
			(http::Method::POST, ["check"]) => handle.check_request(request).boxed(),
			(http::Method::POST, ["checkin"]) => handle.checkin_request(request).boxed(),
			(http::Method::POST, ["checkout"]) => handle.checkout_request(request).boxed(),
			(http::Method::POST, ["clean"]) => handle.clean_request(request).boxed(),
			(http::Method::POST, ["document"]) => handle.document_request(request).boxed(),
			(http::Method::POST, ["format"]) => handle.format_request(request).boxed(),
			(http::Method::GET, ["health"]) => handle.health_request(request).boxed(),
			(http::Method::POST, ["index"]) => handle.index_request(request).boxed(),
			(http::Method::POST, ["lsp"]) => handle.lsp_request(request).boxed(),
			(http::Method::POST, ["pull"]) => handle.pull_request(request).boxed(),
			(http::Method::POST, ["push"]) => handle.push_request(request).boxed(),
			(http::Method::GET, ["read"]) => handle.try_read_stream_request(request).boxed(),
			(http::Method::POST, ["sync"]) => handle.sync_request(request).boxed(),
			(http::Method::POST, ["write"]) => handle.write_request(request).boxed(),
			(http::Method::GET, ["_", path @ ..]) => handle.try_get_request(request, path).boxed(),

			// Modules.
			(http::Method::POST, ["modules", "load"]) => {
				handle.load_module_request(request).boxed()
			},
			(http::Method::POST, ["modules", "resolve"]) => {
				handle.resolve_module_request(request).boxed()
			},

			// Objects.
			(http::Method::GET, ["objects", object, "metadata"]) => handle
				.try_get_object_metadata_request(request, object)
				.boxed(),
			(http::Method::GET, ["objects", object]) => {
				handle.try_get_object_request(request, object).boxed()
			},
			(http::Method::PUT, ["objects", object]) => {
				handle.put_object_request(request, object).boxed()
			},
			(http::Method::POST, ["objects", "batch"]) => {
				handle.post_object_batch_request(request).boxed()
			},
			(http::Method::POST, ["objects", object, "touch"]) => {
				handle.try_touch_object_request(request, object).boxed()
			},

			// Processes.
			(http::Method::GET, ["processes"]) => handle.list_processes_request(request).boxed(),
			(http::Method::POST, ["processes", "spawn"]) => {
				handle.try_spawn_process_request(request).boxed()
			},
			(http::Method::GET, ["processes", process, "metadata"]) => handle
				.try_get_process_metadata_request(request, process)
				.boxed(),
			(http::Method::GET, ["processes", process]) => {
				handle.try_get_process_request(request, process).boxed()
			},
			(http::Method::PUT, ["processes", process]) => {
				handle.put_process_request(request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "cancel"]) => {
				handle.try_cancel_process_request(request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "signal"]) => {
				handle.try_signal_process_request(request, process).boxed()
			},
			(http::Method::GET, ["processes", process, "signal"]) => handle
				.try_get_process_signal_stream_request(request, process)
				.boxed(),
			(http::Method::GET, ["processes", process, "status"]) => handle
				.try_get_process_status_stream_request(request, process)
				.boxed(),
			(http::Method::GET, ["processes", process, "children"]) => handle
				.try_get_process_children_stream_request(request, process)
				.boxed(),
			(http::Method::GET, ["processes", process, "tty", "size"]) => handle
				.try_get_process_tty_size_stream_request(request, process)
				.boxed(),
			(http::Method::PUT, ["processes", process, "tty", "size"]) => handle
				.try_set_process_tty_size_request(request, process)
				.boxed(),
			(http::Method::GET, ["processes", process, "stdio"]) => handle
				.try_read_process_stdio_request(request, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "stdio"]) => handle
				.try_write_process_stdio_request(request, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "touch"]) => {
				handle.try_touch_process_request(request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "finish"]) => {
				handle.try_finish_process_request(request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "wait"]) => handle
				.try_wait_process_future_request(request, process)
				.boxed(),

			// Sandboxes.
			(http::Method::POST, ["sandboxes"]) => handle.create_sandbox_request(request).boxed(),
			(http::Method::GET, ["sandboxes"]) => handle.list_sandboxes_request(request).boxed(),
			(http::Method::POST, ["sandboxes", "dequeue"]) => {
				handle.try_dequeue_sandbox_request(request).boxed()
			},
			(http::Method::GET, ["sandboxes", sandbox]) => {
				handle.try_get_sandbox_request(request, sandbox).boxed()
			},
			(http::Method::DELETE, ["sandboxes", sandbox]) => {
				handle.try_delete_sandbox_request(request, sandbox).boxed()
			},
			(http::Method::POST, ["sandboxes", sandbox, "processes", "dequeue"]) => handle
				.try_dequeue_sandbox_process_request(request, sandbox)
				.boxed(),
			(http::Method::POST, ["sandboxes", sandbox, "finish"]) => {
				handle.try_finish_sandbox_request(request, sandbox).boxed()
			},
			(http::Method::POST, ["sandboxes", sandbox, "heartbeat"]) => handle
				.try_heartbeat_sandbox_request(request, sandbox)
				.boxed(),
			(http::Method::GET, ["sandboxes", sandbox, "status"]) => handle
				.try_get_sandbox_status_stream_request(request, sandbox)
				.boxed(),

			// Remotes.
			(http::Method::GET, ["remotes"]) => handle.list_remotes_request(request).boxed(),
			(http::Method::GET, ["remotes", name]) => {
				handle.try_get_remote_request(request, name).boxed()
			},
			(http::Method::PUT, ["remotes", name]) => {
				handle.put_remote_request(request, name).boxed()
			},
			(http::Method::DELETE, ["remotes", name]) => {
				handle.try_delete_remote_request(request, name).boxed()
			},

			// Watches.
			(http::Method::GET, ["watches"]) => handle.list_watches_request(request).boxed(),
			(http::Method::DELETE, ["watches"]) => handle.try_delete_watch_request(request).boxed(),
			(http::Method::POST, ["watches", "touch"]) => {
				handle.touch_watch_request(request).boxed()
			},

			// Tags.
			(http::Method::GET, ["tags"]) => handle.list_tags_request(request).boxed(),
			(http::Method::POST, ["tags", "batch"]) => {
				handle.post_tag_batch_request(request).boxed()
			},
			(http::Method::PUT, ["tags", tag @ ..]) => handle.put_tag_request(request, tag).boxed(),
			(http::Method::DELETE, ["tags", tag @ ..]) => {
				handle.delete_tags_request(request, tag).boxed()
			},

			// Users.
			(http::Method::GET, ["user"]) => handle.get_user_request(request).boxed(),
			(http::Method::POST, ["user", "login"]) => handle.login_user_request(request).boxed(),

			(_, _) => future::ok(
				http::Response::builder()
					.status(http::StatusCode::NOT_FOUND)
					.bytes("not found")
					.unwrap()
					.boxed_body(),
			)
			.boxed(),
		}
		.await;

		// Handle an error.
		let mut response = response.unwrap_or_else(|error| {
			tracing::error!(error = %error.trace());
			let bytes = match error.to_data_or_id() {
				tg::Either::Left(data) => serde_json::to_string(&data).unwrap(),
				tg::Either::Right(id) => id.to_string(),
			};
			http::Response::builder()
				.status(http::StatusCode::INTERNAL_SERVER_ERROR)
				.bytes(bytes)
				.unwrap()
				.boxed_body()
		});

		// Add the request ID to the response.
		let key = http::HeaderName::from_static("x-tg-request-id");
		let value = http::HeaderValue::from_str(&id.to_string()).unwrap();
		response.headers_mut().insert(key, value);

		response
	}

	async fn handle_request_context_for_process(
		&self,
		headers: &http::HeaderMap,
		context: &mut Context,
	) -> Result<(), http::Response<BoxBody>> {
		let process_id = if context.process.is_some() || context.sandbox.is_none() {
			None
		} else {
			match headers.get("x-tg-process") {
				None => None,
				Some(value) => {
					let value = value.to_str().map_err(|source| {
						let error = tg::error!(!source, "failed to parse the x-tg-process header");
						tracing::debug!(error = %error.trace(), "failed to update the request context");
						let bytes = match error.to_data_or_id() {
							tg::Either::Left(data) => serde_json::to_string(&data).unwrap(),
							tg::Either::Right(id) => id.to_string(),
						};
						http::Response::builder()
							.status(http::StatusCode::BAD_REQUEST)
							.bytes(bytes)
							.unwrap()
							.boxed_body()
					})?;
					let value = value.parse::<tg::process::Id>().map_err(|source| {
						let error = tg::error!(!source, "failed to parse the x-tg-process header");
						tracing::debug!(error = %error.trace(), "failed to update the request context");
						let bytes = match error.to_data_or_id() {
							tg::Either::Left(data) => serde_json::to_string(&data).unwrap(),
							tg::Either::Right(id) => id.to_string(),
						};
						http::Response::builder()
							.status(http::StatusCode::BAD_REQUEST)
							.bytes(bytes)
							.unwrap()
							.boxed_body()
					})?;
					Some(value)
				},
			}
		};
		if context.process.is_some() {
			return Ok(());
		}
		let Some(sandbox_id) = context.sandbox.clone() else {
			return Ok(());
		};
		let Some(process_id) = process_id else {
			let error = tg::error!("missing x-tg-process header");
			tracing::debug!(error = %error.trace(), "failed to update the request context");
			let bytes = match error.to_data_or_id() {
				tg::Either::Left(data) => serde_json::to_string(&data).unwrap(),
				tg::Either::Right(id) => id.to_string(),
			};
			return Err(http::Response::builder()
				.status(http::StatusCode::BAD_REQUEST)
				.bytes(bytes)
				.unwrap()
				.boxed_body());
		};
		let Some(sandbox) = self
			.sandboxes
			.get(&sandbox_id)
			.map(|sandbox| sandbox.value().clone())
		else {
			let error = tg::error!(sandbox = %sandbox_id, "failed to get the sandbox");
			tracing::error!(error = %error.trace(), "failed to update the request context");
			let bytes = match error.to_data_or_id() {
				tg::Either::Left(data) => serde_json::to_string(&data).unwrap(),
				tg::Either::Right(id) => id.to_string(),
			};
			return Err(http::Response::builder()
				.status(http::StatusCode::INTERNAL_SERVER_ERROR)
				.bytes(bytes)
				.unwrap()
				.boxed_body());
		};
		let process = match sandbox.try_get_process(&process_id).await {
			Ok(Some(process)) => process,
			Ok(None) => {
				let error = tg::error!(
					process = %process_id,
					sandbox = %sandbox_id,
					"failed to find the sandbox process"
				);
				tracing::debug!(error = %error.trace(), "failed to update the request context");
				let bytes = match error.to_data_or_id() {
					tg::Either::Left(data) => serde_json::to_string(&data).unwrap(),
					tg::Either::Right(id) => id.to_string(),
				};
				return Err(http::Response::builder()
					.status(http::StatusCode::BAD_REQUEST)
					.bytes(bytes)
					.unwrap()
					.boxed_body());
			},
			Err(source) => {
				let error = tg::error!(
					!source,
					process = %process_id,
					sandbox = %sandbox_id,
					"failed to get the sandbox process"
				);
				tracing::error!(error = %error.trace(), "failed to update the request context");
				let bytes = match error.to_data_or_id() {
					tg::Either::Left(data) => serde_json::to_string(&data).unwrap(),
					tg::Either::Right(id) => id.to_string(),
				};
				return Err(http::Response::builder()
					.status(http::StatusCode::INTERNAL_SERVER_ERROR)
					.bytes(bytes)
					.unwrap()
					.boxed_body());
			},
		};
		context.process = Some(Arc::new(crate::context::Process {
			debug: process.debug,
			id: process.id,
			location: process.location,
			retry: process.retry,
		}));
		Ok(())
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
