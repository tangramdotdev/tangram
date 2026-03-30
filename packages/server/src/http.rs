use {
	crate::{Context, Server},
	futures::{FutureExt as _, future},
	std::{convert::Infallible, path::Path, pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_http::{
		body::Boxed as BoxBody, body::Ext as _, request::Ext as _, response::Ext as _,
		response::builder::Ext as _,
	},
	tangram_uri::Uri,
	tokio::net::{TcpListener, UnixListener},
	tower::ServiceExt as _,
	tower_http::ServiceBuilderExt as _,
};

impl Server {
	pub(crate) async fn listen(
		url: &Uri,
	) -> tg::Result<tokio_util::either::Either<tokio::net::UnixListener, tokio::net::TcpListener>>
	{
		let listener = match url.scheme() {
			Some("http+unix") => {
				let path = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let path = Path::new(path);
				let listener = UnixListener::bind(path).map_err(
					|source| tg::error!(!source, path = %path.display(), "failed to bind"),
				)?;
				tokio_util::either::Either::Left(listener)
			},
			Some("http" | "https") => {
				let host = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!(%url, "invalid url"))?;
				let listener = TcpListener::bind(format!("{host}:{port}"))
					.await
					.map_err(|source| tg::error!(!source, "failed to bind"))?;
				tokio_util::either::Either::Right(listener)
			},
			_ => {
				return Err(tg::error!("invalid url"));
			},
		};
		Ok(listener)
	}

	pub(crate) async fn serve(
		&self,
		listener: tokio_util::either::Either<tokio::net::UnixListener, tokio::net::TcpListener>,
		context: Context,
		stopper: Stopper,
	) {
		#[cfg(feature = "tls")]
		let tls = if self
			.http
			.as_ref()
			.is_some_and(|http| matches!(http.url.scheme(), Some("https")))
		{
			let Some(config) = self.config.http.as_ref().and_then(|http| http.tls.as_ref()) else {
				tracing::error!("missing tls configuration");
				return;
			};
			match Self::create_tls_acceptor(config).await {
				Ok(tls) => Some(tls),
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to create the TLS acceptor");
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

		// Create the service builder.
		let builder = tower::ServiceBuilder::new()
			.layer(tangram_http::layer::metrics::MetricsLayer::new())
			.layer(tangram_http::layer::tracing::TracingLayer::new())
			.layer(tower_http::timeout::TimeoutLayer::with_status_code(
				http::StatusCode::REQUEST_TIMEOUT,
				Duration::from_mins(1),
			))
			.add_extension(stopper.clone())
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

		let service = builder.service_fn({
			let handle = self.clone();
			move |request| {
				let handle = handle.clone();
				let context = context.clone();
				async move {
					let response = handle.handle_request(request, context).await;
					Ok::<_, Infallible>(response)
				}
			}
		});

		loop {
			// Accept a new connection.
			let accept = async {
				let stream = match &listener {
					tokio_util::either::Either::Left(listener) => {
						tokio_util::either::Either::Left(listener.accept().await?.0)
					},
					tokio_util::either::Either::Right(listener) => {
						tokio_util::either::Either::Right(listener.accept().await?.0)
					},
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
					#[cfg(feature = "tls")]
					let stream = match stream {
						tokio_util::either::Either::Left(stream) => {
							tokio_util::either::Either::Left(stream)
						},
						tokio_util::either::Either::Right(stream) => {
							if let Some(tls) = tls {
								match tls.accept(stream).await {
									Ok(stream) => tokio_util::either::Either::Right(
										tokio_util::either::Either::Right(stream),
									),
									Err(error) => {
										tracing::error!(
											?error,
											"failed to perform the TLS handshake"
										);
										drop(guard);
										return;
									},
								}
							} else {
								tokio_util::either::Either::Right(tokio_util::either::Either::Left(
									stream,
								))
							}
						},
					};

					let idle = tangram_http::idle::Idle::new(Duration::from_secs(30));
					let executor = hyper_util::rt::TokioExecutor::new();
					let mut builder = hyper_util::server::conn::auto::Builder::new(executor);
					builder
						.http2()
						.max_concurrent_streams(None)
						.max_pending_accept_reset_streams(None)
						.max_local_error_reset_streams(None);
					let service = service
						.map_request(|request: http::Request<hyper::body::Incoming>| {
							request.boxed_body()
						})
						.map_response({
							let idle = idle.clone();
							move |response: http::Response<BoxBody>| {
								response.map(move |body| {
									BoxBody::new(
										tangram_http::idle::Body::new(idle.token(), body).map_err(
											|error| {
												tracing::error!(?error, "response body error");
												error
											},
										),
									)
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
					drop(guard);
				}
			});
		}

		// Wait for all tasks to finish.
		task_tracker.close();
		task_tracker.wait().await;
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
		mut request: http::Request<BoxBody>,
		mut context: Context,
	) -> http::Response<BoxBody> {
		let id = tg::Id::new_uuidv7(tg::id::Kind::Request);
		request.extensions_mut().insert(id.clone());

		let span = tracing::Span::current();
		span.record("id", id.to_string());
		span.record("method", request.method().as_str());
		span.record("path", request.uri().path());

		let method = request.method().clone();
		let path = request.uri().path().to_owned();

		context.token = request.token(None).map(ToOwned::to_owned);
		context.untrusted = true;

		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		let response = match (method, path_components.as_slice()) {
			(http::Method::POST, ["cache"]) => self.handle_cache_request(request, &context).boxed(),
			(http::Method::POST, ["check"]) => self.handle_check_request(request, &context).boxed(),
			(http::Method::POST, ["checkin"]) => {
				self.handle_checkin_request(request, &context).boxed()
			},
			(http::Method::POST, ["checkout"]) => {
				self.handle_checkout_request(request, &context).boxed()
			},
			(http::Method::POST, ["clean"]) => {
				self.handle_server_clean_request(request, &context).boxed()
			},
			(http::Method::POST, ["document"]) => {
				self.handle_document_request(request, &context).boxed()
			},
			(http::Method::POST, ["format"]) => {
				self.handle_format_request(request, &context).boxed()
			},
			(http::Method::GET, ["health"]) => {
				self.handle_server_health_request(request, &context).boxed()
			},
			(http::Method::POST, ["index"]) => self.handle_index_request(request, &context).boxed(),
			(http::Method::POST, ["lsp"]) => self.handle_lsp_request(request, &context).boxed(),
			(http::Method::POST, ["pull"]) => self.handle_pull_request(request, &context).boxed(),
			(http::Method::POST, ["push"]) => self.handle_push_request(request, &context).boxed(),
			(http::Method::GET, ["read"]) => self.handle_read_request(request, &context).boxed(),
			(http::Method::POST, ["sync"]) => self.handle_sync_request(request, &context).boxed(),
			(http::Method::POST, ["write"]) => self.handle_write_request(request, &context).boxed(),
			(http::Method::GET, ["_", path @ ..]) => {
				self.handle_get_request(request, &context, path).boxed()
			},

			// Modules.
			(http::Method::POST, ["modules", "load"]) => {
				self.handle_load_module_request(request, &context).boxed()
			},
			(http::Method::POST, ["modules", "resolve"]) => self
				.handle_resolve_module_request(request, &context)
				.boxed(),

			// Objects.
			(http::Method::GET, ["objects", object, "metadata"]) => self
				.handle_get_object_metadata_request(request, &context, object)
				.boxed(),
			(http::Method::GET, ["objects", object]) => self
				.handle_get_object_request(request, &context, object)
				.boxed(),
			(http::Method::PUT, ["objects", object]) => self
				.handle_put_object_request(request, &context, object)
				.boxed(),
			(http::Method::POST, ["objects", "batch"]) => self
				.handle_post_object_batch_request(request, &context)
				.boxed(),
			(http::Method::POST, ["objects", object, "touch"]) => self
				.handle_touch_object_request(request, &context, object)
				.boxed(),

			// Processes.
			(http::Method::GET, ["processes"]) => self
				.handle_list_processes_request(request, &context)
				.boxed(),
			(http::Method::POST, ["processes", "spawn"]) => {
				self.handle_spawn_process_request(request, &context).boxed()
			},
			(http::Method::GET, ["processes", process, "metadata"]) => self
				.handle_get_process_metadata_request(request, &context, process)
				.boxed(),
			(http::Method::GET, ["processes", process]) => self
				.handle_get_process_request(request, &context, process)
				.boxed(),
			(http::Method::PUT, ["processes", process]) => self
				.handle_put_process_request(request, &context, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "cancel"]) => self
				.handle_cancel_process_request(request, &context, process)
				.boxed(),
			(http::Method::POST, ["processes", "dequeue"]) => self
				.handle_dequeue_process_request(request, &context)
				.boxed(),
			(http::Method::POST, ["processes", process, "signal"]) => self
				.handle_post_process_signal_request(request, &context, process)
				.boxed(),
			(http::Method::GET, ["processes", process, "signal"]) => self
				.handle_get_process_signal_request(request, &context, process)
				.boxed(),
			(http::Method::GET, ["processes", process, "status"]) => self
				.handle_get_process_status_request(request, &context, process)
				.boxed(),
			(http::Method::GET, ["processes", process, "children"]) => self
				.handle_get_process_children_request(request, &context, process)
				.boxed(),
			(http::Method::GET, ["processes", process, "tty", "size"]) => self
				.handle_get_process_tty_size_request(request, &context, process)
				.boxed(),
			(http::Method::PUT, ["processes", process, "tty", "size"]) => self
				.handle_set_process_tty_size_request(request, &context, process)
				.boxed(),
			(http::Method::GET, ["processes", process, "stdio"]) => self
				.handle_post_process_stdio_read_request(request, &context, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "stdio"]) => self
				.handle_post_process_stdio_write_request(request, &context, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "heartbeat"]) => self
				.handle_heartbeat_process_request(request, &context, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "touch"]) => self
				.handle_touch_process_request(request, &context, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "finish"]) => self
				.handle_finish_process_request(request, &context, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "wait"]) => self
				.handle_post_process_wait_request(request, &context, process)
				.boxed(),

			// Remotes.
			(http::Method::GET, ["remotes"]) => {
				self.handle_list_remotes_request(request, &context).boxed()
			},
			(http::Method::GET, ["remotes", name]) => self
				.handle_get_remote_request(request, &context, name)
				.boxed(),
			(http::Method::PUT, ["remotes", name]) => self
				.handle_put_remote_request(request, &context, name)
				.boxed(),
			(http::Method::DELETE, ["remotes", name]) => self
				.handle_delete_remote_request(request, &context, name)
				.boxed(),

			// Watches.
			(http::Method::GET, ["watches"]) => {
				self.handle_list_watches_request(request, &context).boxed()
			},
			(http::Method::DELETE, ["watches"]) => {
				self.handle_delete_watch_request(request, &context).boxed()
			},
			(http::Method::POST, ["watches", "touch"]) => {
				self.handle_touch_watch_request(request, &context).boxed()
			},

			// Tags.
			(http::Method::GET, ["tags"]) => {
				self.handle_list_tags_request(request, &context).boxed()
			},
			(http::Method::POST, ["tags", "batch"]) => self
				.handle_post_tag_batch_request(request, &context)
				.boxed(),
			(http::Method::PUT, ["tags", tag @ ..]) => {
				self.handle_put_tag_request(request, &context, tag).boxed()
			},
			(http::Method::DELETE, ["tags", tag @ ..]) => self
				.handle_delete_tag_request(request, &context, tag)
				.boxed(),

			// Users.
			(http::Method::GET, ["user"]) => {
				self.handle_get_user_request(request, &context).boxed()
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
			let bytes = error
				.state()
				.object()
				.and_then(|object| serde_json::to_string(&object.unwrap_error_ref().to_data()).ok())
				.unwrap_or_default();
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
}
