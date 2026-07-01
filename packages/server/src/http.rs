use {
	crate::{Context, Server},
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

pub(crate) enum Listener {
	Tcp(tokio::net::TcpListener),
	Unix {
		listener: tokio::net::UnixListener,
		#[expect(dead_code)]
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

type Service =
	tower::util::BoxCloneSyncService<http::Request<BoxBody>, http::Response<BoxBody>, Infallible>;

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
					Stream::Tcp(
						tokio::net::TcpStream::connect((host, port))
							.await
							.map_err(|error| {
								tg::error!(!error, "failed to connect to the socket")
							})?,
					)
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

	pub(crate) async fn serve(
		&self,
		listener: Listener,
		config: crate::config::HttpListener,
		sandbox: bool,
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

		let service = self.service(sandbox, stopper.clone());

		loop {
			// Accept a new connection.
			let accept = async {
				let stream = match &listener {
					Listener::Tcp(listener) => Stream::Tcp(listener.accept().await?.0),
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

	fn service(&self, sandbox: bool, stopper: Stopper) -> Service {
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
			let server = self.clone();
			move |request| {
				let server = server.clone();
				let stopper = stopper.clone();
				async move {
					let context = Context {
						id: None,
						principal: tg::Principal::Anonymous,
						sandbox,
						stopper: Some(stopper),
					};
					let response = server.handle_request(request, context).await;
					Ok::<_, Infallible>(response)
				}
			}
		}))
	}

	pub(crate) async fn serve_stream<S>(&self, stream: S, sandbox: bool, stopper: Stopper)
	where
		S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
	{
		let service = self.service(sandbox, stopper.clone());
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

	#[tracing::instrument(fields(id, method, path), level = "trace", name = "request", skip_all)]
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
		span.record("path", request.uri().path());

		let method = request.method().clone();
		let path = request.uri().path().to_owned();

		// Authenticate.
		let token = request.token(None).map(str::to_owned);
		let result = self.authenticate(context.sandbox, token.as_deref()).await;
		context.principal = match result {
			Ok(authentication) => authentication,
			Err(error) => {
				let bytes = match error.to_data_or_id() {
					tg::Either::Left(data) => serde_json::to_string(&data).unwrap(),
					tg::Either::Right(id) => id.to_string(),
				};
				let response = http::Response::builder()
					.status(http::StatusCode::INTERNAL_SERVER_ERROR)
					.bytes(bytes)
					.unwrap()
					.boxed_body();
				return response;
			},
		};

		let session = self.session(&context);

		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		let response = match (method, path_components.as_slice()) {
			(http::Method::POST, ["cache"]) => session.cache_request(request).boxed(),
			(http::Method::POST, ["check"]) => session.check_request(request).boxed(),
			(http::Method::POST, ["checkin"]) => session.checkin_request(request).boxed(),
			(http::Method::POST, ["checkout"]) => session.checkout_request(request).boxed(),
			(http::Method::POST, ["clean"]) => session.clean_request(request).boxed(),
			(http::Method::POST, ["document"]) => session.document_request(request).boxed(),
			(http::Method::POST, ["format"]) => session.format_request(request).boxed(),
			(http::Method::GET, ["health"]) => session.health_request(request).boxed(),
			(http::Method::POST, ["index"]) => session.index_request(request).boxed(),
			(http::Method::GET, ["list"]) => session.list_request(request).boxed(),
			(http::Method::POST, ["lsp"]) => session.lsp_request(request).boxed(),
			(http::Method::POST, ["pull"]) => session.pull_request(request).boxed(),
			(http::Method::POST, ["push"]) => session.push_request(request).boxed(),
			(http::Method::GET, ["read"]) => session.try_read_stream_request(request).boxed(),
			(http::Method::POST, ["sync"]) => session.sync_request(request).boxed(),
			(http::Method::POST, ["write"]) => session.write_request(request).boxed(),
			(http::Method::GET, ["_", path @ ..]) => session.try_get_request(request, path).boxed(),

			// Grants.
			(http::Method::GET, ["grants"]) => session.list_grants_request(request).boxed(),
			(http::Method::POST, ["grants"]) => session.create_grant_request(request).boxed(),
			(http::Method::DELETE, ["grants"]) => session.delete_grant_request(request).boxed(),

			// Groups.
			(http::Method::POST, ["groups"]) => session.create_group_request(request).boxed(),
			(http::Method::GET, ["groups", group]) => {
				session.try_get_group_request(request, group).boxed()
			},
			(http::Method::DELETE, ["groups", group]) => {
				session.try_delete_group_request(request, group).boxed()
			},
			(http::Method::GET, ["groups", group, "members"]) => {
				session.list_group_members_request(request, group).boxed()
			},
			(http::Method::POST, ["groups", group, "members"]) => {
				session.add_group_member_request(request, group).boxed()
			},
			(http::Method::DELETE, ["groups", group, "members", member]) => session
				.remove_group_member_request(request, group, member)
				.boxed(),

			// Logins.
			(http::Method::POST, ["logins"]) => session.create_login_request(request).boxed(),
			(http::Method::POST, ["login", "wait"]) => session.wait_login_request(request).boxed(),
			(http::Method::GET, ["oauth", "github", "authorize"]) => {
				session.oauth_github_authorize_request(request).boxed()
			},
			(http::Method::GET, ["oauth", "github", "callback"]) => {
				session.oauth_github_callback_request(request).boxed()
			},

			// Modules.
			(http::Method::POST, ["modules", "load"]) => {
				session.load_module_request(request).boxed()
			},
			(http::Method::POST, ["modules", "resolve"]) => {
				session.resolve_module_request(request).boxed()
			},

			// Objects.
			(http::Method::GET, ["objects", object, "metadata"]) => session
				.try_get_object_metadata_request(request, object)
				.boxed(),
			(http::Method::GET, ["objects", object]) => {
				session.try_get_object_request(request, object).boxed()
			},
			(http::Method::PUT, ["objects", object]) => {
				session.put_object_request(request, object).boxed()
			},
			(http::Method::POST, ["objects", "batch"]) => {
				session.post_object_batch_request(request).boxed()
			},
			(http::Method::POST, ["objects", object, "touch"]) => {
				session.try_touch_object_request(request, object).boxed()
			},

			// Organizations.
			(http::Method::POST, ["organizations"]) => {
				session.create_organization_request(request).boxed()
			},
			(http::Method::GET, ["organizations", organization]) => session
				.try_get_organization_request(request, organization)
				.boxed(),
			(http::Method::DELETE, ["organizations", organization]) => session
				.try_delete_organization_request(request, organization)
				.boxed(),
			(http::Method::GET, ["organizations", organization, "members"]) => session
				.list_organization_members_request(request, organization)
				.boxed(),
			(http::Method::POST, ["organizations", organization, "members"]) => session
				.add_organization_member_request(request, organization)
				.boxed(),
			(http::Method::DELETE, ["organizations", organization, "members", member]) => session
				.remove_organization_member_request(request, organization, member)
				.boxed(),

			// Processes.
			(http::Method::GET, ["processes"]) => session.list_processes_request(request).boxed(),
			(http::Method::POST, ["processes", "spawn"]) => {
				session.try_spawn_process_request(request).boxed()
			},
			(http::Method::GET, ["processes", process, "metadata"]) => session
				.try_get_process_metadata_request(request, process)
				.boxed(),
			(http::Method::GET, ["processes", process]) => {
				session.try_get_process_request(request, process).boxed()
			},
			(http::Method::PUT, ["processes", process]) => {
				session.put_process_request(request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "cancel"]) => {
				session.try_cancel_process_request(request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "control"]) => session
				.try_get_process_control_stream_request(request, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "signal"]) => {
				session.try_signal_process_request(request, process).boxed()
			},
			(http::Method::GET, ["processes", process, "status"]) => session
				.try_get_process_status_stream_request(request, process)
				.boxed(),
			(http::Method::GET, ["processes", process, "children"]) => session
				.try_get_process_children_stream_request(request, process)
				.boxed(),
			(http::Method::PUT, ["processes", process, "tty", "size"]) => session
				.try_set_process_tty_size_request(request, process)
				.boxed(),
			(http::Method::GET, ["processes", process, "stdio"]) => session
				.try_read_process_stdio_request(request, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "stdio"]) => session
				.try_write_process_stdio_request(request, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "touch"]) => {
				session.try_touch_process_request(request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "finish"]) => {
				session.try_finish_process_request(request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "wait"]) => session
				.try_wait_process_future_request(request, process)
				.boxed(),

			// Remotes.
			(http::Method::GET, ["remotes"]) => session.list_remotes_request(request).boxed(),
			(http::Method::GET, ["remotes", name]) => {
				session.try_get_remote_request(request, name).boxed()
			},
			(http::Method::PUT, ["remotes", name]) => {
				session.put_remote_request(request, name).boxed()
			},
			(http::Method::DELETE, ["remotes", name]) => {
				session.delete_remote_request(request, name).boxed()
			},

			// Sandboxes.
			(http::Method::POST, ["sandboxes"]) => session.create_sandbox_request(request).boxed(),
			(http::Method::GET, ["sandboxes"]) => session.list_sandboxes_request(request).boxed(),
			(http::Method::POST, ["sandboxes", "dequeue"]) => {
				session.try_dequeue_sandbox_request(request).boxed()
			},
			(http::Method::GET, ["sandboxes", sandbox]) => {
				session.try_get_sandbox_request(request, sandbox).boxed()
			},
			(http::Method::POST, ["sandboxes", sandbox, "processes", "dequeue"]) => session
				.try_dequeue_sandbox_process_request(request, sandbox)
				.boxed(),
			(http::Method::POST, ["sandboxes", sandbox, "destroy"]) => session
				.try_destroy_sandbox_request(request, sandbox)
				.boxed(),
			(http::Method::POST, ["sandboxes", sandbox, "heartbeat"]) => session
				.try_heartbeat_sandbox_request(request, sandbox)
				.boxed(),
			(http::Method::GET, ["sandboxes", sandbox, "status"]) => session
				.try_get_sandbox_status_stream_request(request, sandbox)
				.boxed(),

			// Tags.
			(http::Method::POST, ["tags", "batch"]) => {
				session.post_tag_batch_request(request).boxed()
			},
			(http::Method::GET, ["tags", path @ ..]) => {
				session.try_get_tag_request(request, path).boxed()
			},
			(http::Method::PUT, ["tags"]) => session.put_tag_request(request).boxed(),
			(http::Method::DELETE, ["tags"]) => session.delete_tags_request(request).boxed(),

			// Users.
			(http::Method::GET, ["users", user]) => {
				session.try_get_user_request(request, user).boxed()
			},
			(http::Method::GET, ["user"]) => session.get_current_user_request(request).boxed(),

			// Watches.
			(http::Method::GET, ["watches"]) => session.list_watches_request(request).boxed(),
			(http::Method::DELETE, ["watches"]) => {
				session.try_delete_watch_request(request).boxed()
			},
			(http::Method::POST, ["watches", "touch"]) => {
				session.touch_watch_request(request).boxed()
			},

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
