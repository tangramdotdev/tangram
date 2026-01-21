use {
	crate::{Context, Server},
	futures::{FutureExt as _, future},
	http_body_util::BodyExt as _,
	std::{convert::Infallible, path::Path, pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::Stop,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
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
				let path = url.host().ok_or_else(|| tg::error!("invalid url"))?;
				let path = Path::new(path);
				let listener = UnixListener::bind(path).map_err(
					|source| tg::error!(!source, path = %path.display(), "failed to bind"),
				)?;
				tokio_util::either::Either::Left(listener)
			},
			Some("http") => {
				let host = url.host().ok_or_else(|| tg::error!("invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!("invalid url"))?;
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
		stop: Stop,
	) {
		// Create the task tracker.
		let task_tracker = tokio_util::task::TaskTracker::new();

		// Create the service.
		let service = tower::ServiceBuilder::new()
			.layer(tangram_http::layer::tracing::TracingLayer::new())
			.layer(tower_http::timeout::TimeoutLayer::with_status_code(
				http::StatusCode::REQUEST_TIMEOUT,
				Duration::from_secs(60),
			))
			.add_extension(stop.clone())
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
			)
			.service_fn({
				let handle = self.clone();
				move |request| {
					let handle = handle.clone();
					let context = context.clone();
					async move {
						let response = Self::handle_request(&handle, request, context).await;
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
			let stream = match future::select(pin!(accept), pin!(stop.wait())).await {
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
			task_tracker.spawn({
				let service = service.clone();
				let stop = stop.clone();
				async move {
					let idle = tangram_http::idle::Idle::new(Duration::from_secs(30));
					let executor = hyper_util::rt::TokioExecutor::new();
					let mut builder = hyper_util::server::conn::auto::Builder::new(executor);
					builder.http2().max_concurrent_streams(None);
					let service = service
						.map_request(|request: http::Request<hyper::body::Incoming>| {
							request.map(Body::new)
						})
						.map_response({
							let idle = idle.clone();
							move |response: http::Response<Body>| {
								response.map(move |body| {
									Body::new(tangram_http::idle::Body::new(idle.token(), body))
								})
							}
						});
					let service = hyper_util::service::TowerToHyperService::new(service);
					let stream = hyper_util::rt::TokioIo::new(stream);
					let connection = builder.serve_connection_with_upgrades(stream, service);
					let result = match future::select(
						pin!(connection),
						future::select(pin!(idle.wait()), pin!(stop.wait())),
					)
					.await
					{
						future::Either::Left((result, _)) => result,
						future::Either::Right((_, mut connection)) => {
							connection.as_mut().graceful_shutdown();
							connection.await
						},
					};
					result.ok();
				}
			});
		}

		// Wait for all tasks to finish.
		task_tracker.close();
		task_tracker.wait().await;
	}

	async fn handle_request(
		server: &Server,
		mut request: http::Request<Body>,
		mut context: Context,
	) -> http::Response<Body> {
		let id = tg::Id::new_uuidv7(tg::id::Kind::Request);
		request.extensions_mut().insert(id.clone());

		// Update the context.
		context.token = request.token(None).map(ToOwned::to_owned);
		context.untrusted = true;

		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		let response = match (method, path_components.as_slice()) {
			(http::Method::POST, ["cache"]) => {
				server.handle_cache_request(request, &context).boxed()
			},
			(http::Method::POST, ["check"]) => {
				server.handle_check_request(request, &context).boxed()
			},
			(http::Method::POST, ["checkin"]) => {
				server.handle_checkin_request(request, &context).boxed()
			},
			(http::Method::POST, ["checkout"]) => {
				server.handle_checkout_request(request, &context).boxed()
			},
			(http::Method::POST, ["clean"]) => server
				.handle_server_clean_request(request, &context)
				.boxed(),
			(http::Method::POST, ["document"]) => {
				server.handle_document_request(request, &context).boxed()
			},
			(http::Method::POST, ["format"]) => {
				server.handle_format_request(request, &context).boxed()
			},
			(http::Method::GET, ["health"]) => server
				.handle_server_health_request(request, &context)
				.boxed(),
			(http::Method::POST, ["index"]) => {
				server.handle_index_request(request, &context).boxed()
			},
			(http::Method::POST, ["lsp"]) => server.handle_lsp_request(request, &context).boxed(),
			(http::Method::POST, ["pull"]) => server.handle_pull_request(request, &context).boxed(),
			(http::Method::POST, ["push"]) => server.handle_push_request(request, &context).boxed(),
			(http::Method::GET, ["read"]) => server.handle_read_request(request, &context).boxed(),
			(http::Method::POST, ["sync"]) => server.handle_sync_request(request, &context).boxed(),
			(http::Method::POST, ["write"]) => {
				server.handle_write_request(request, &context).boxed()
			},
			(http::Method::GET, ["_", path @ ..]) => {
				server.handle_get_request(request, &context, path).boxed()
			},

			// Modules.
			(http::Method::POST, ["modules", "load"]) => {
				server.handle_load_module_request(request, &context).boxed()
			},
			(http::Method::POST, ["modules", "resolve"]) => server
				.handle_resolve_module_request(request, &context)
				.boxed(),

			// Objects.
			(http::Method::GET, ["objects", object, "metadata"]) => server
				.handle_get_object_metadata_request(request, &context, object)
				.boxed(),
			(http::Method::GET, ["objects", object]) => server
				.handle_get_object_request(request, &context, object)
				.boxed(),
			(http::Method::PUT, ["objects", object]) => server
				.handle_put_object_request(request, &context, object)
				.boxed(),
			(http::Method::POST, ["objects", "batch"]) => server
				.handle_post_object_batch_request(request, &context)
				.boxed(),
			(http::Method::POST, ["objects", object, "touch"]) => server
				.handle_touch_object_request(request, &context, object)
				.boxed(),

			// Processes.
			(http::Method::GET, ["processes"]) => server
				.handle_list_processes_request(request, &context)
				.boxed(),
			(http::Method::POST, ["processes", "spawn"]) => server
				.handle_spawn_process_request(request, &context)
				.boxed(),
			(http::Method::GET, ["processes", process, "metadata"]) => server
				.handle_get_process_metadata_request(request, &context, process)
				.boxed(),
			(http::Method::GET, ["processes", process]) => server
				.handle_get_process_request(request, &context, process)
				.boxed(),
			(http::Method::PUT, ["processes", process]) => server
				.handle_put_process_request(request, &context, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "cancel"]) => server
				.handle_cancel_process_request(request, &context, process)
				.boxed(),
			(http::Method::POST, ["processes", "dequeue"]) => server
				.handle_dequeue_process_request(request, &context)
				.boxed(),
			(http::Method::POST, ["processes", process, "start"]) => server
				.handle_start_process_request(request, &context, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "signal"]) => server
				.handle_post_process_signal_request(request, &context, process)
				.boxed(),
			(http::Method::GET, ["processes", process, "signal"]) => server
				.handle_get_process_signal_request(request, &context, process)
				.boxed(),
			(http::Method::GET, ["processes", process, "status"]) => server
				.handle_get_process_status_request(request, &context, process)
				.boxed(),
			(http::Method::GET, ["processes", process, "children"]) => server
				.handle_get_process_children_request(request, &context, process)
				.boxed(),
			(http::Method::GET, ["processes", process, "log"]) => server
				.handle_get_process_log_request(request, &context, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "log"]) => server
				.handle_post_process_log_request(request, &context, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "finish"]) => server
				.handle_finish_process_request(request, &context, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "touch"]) => server
				.handle_touch_process_request(request, &context, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "heartbeat"]) => server
				.handle_heartbeat_process_request(request, &context, process)
				.boxed(),
			(http::Method::POST, ["processes", process, "wait"]) => server
				.handle_post_process_wait_request(request, &context, process)
				.boxed(),

			// Pipes.
			(http::Method::POST, ["pipes"]) => {
				server.handle_create_pipe_request(request, &context).boxed()
			},
			(http::Method::DELETE, ["pipes", pipe]) => server
				.handle_delete_pipe_request(request, &context, pipe)
				.boxed(),
			(http::Method::POST, ["pipes", pipe, "close"]) => server
				.handle_close_pipe_request(request, &context, pipe)
				.boxed(),
			(http::Method::GET, ["pipes", pipe, "read"]) => server
				.handle_read_pipe_request(request, &context, pipe)
				.boxed(),
			(http::Method::POST, ["pipes", pipe, "write"]) => server
				.handle_write_pipe_request(request, &context, pipe)
				.boxed(),

			// Ptys.
			(http::Method::POST, ["ptys"]) => {
				server.handle_create_pty_request(request, &context).boxed()
			},
			(http::Method::DELETE, ["ptys", pty]) => server
				.handle_delete_pty_request(request, &context, pty)
				.boxed(),
			(http::Method::POST, ["ptys", pty, "close"]) => server
				.handle_close_pty_request(request, &context, pty)
				.boxed(),
			(http::Method::GET, ["ptys", pty, "size"]) => server
				.handle_get_pty_size_request(request, &context, pty)
				.boxed(),
			(http::Method::GET, ["ptys", pty, "read"]) => server
				.handle_read_pty_request(request, &context, pty)
				.boxed(),
			(http::Method::POST, ["ptys", pty, "write"]) => server
				.handle_write_pty_request(request, &context, pty)
				.boxed(),

			// Remotes.
			(http::Method::GET, ["remotes"]) => server
				.handle_list_remotes_request(request, &context)
				.boxed(),
			(http::Method::GET, ["remotes", name]) => server
				.handle_get_remote_request(request, &context, name)
				.boxed(),
			(http::Method::PUT, ["remotes", name]) => server
				.handle_put_remote_request(request, &context, name)
				.boxed(),
			(http::Method::DELETE, ["remotes", name]) => server
				.handle_delete_remote_request(request, &context, name)
				.boxed(),

			// Watches.
			(http::Method::GET, ["watches"]) => server
				.handle_list_watches_request(request, &context)
				.boxed(),
			(http::Method::DELETE, ["watches"]) => server
				.handle_delete_watch_request(request, &context)
				.boxed(),

			// Tags.
			(http::Method::GET, ["tags"]) => {
				server.handle_list_tags_request(request, &context).boxed()
			},
			(http::Method::GET, ["tags", pattern @ ..]) => server
				.handle_get_tag_request(request, &context, pattern)
				.boxed(),
			(http::Method::POST, ["tags", "batch"]) => server
				.handle_post_tag_batch_request(request, &context)
				.boxed(),
			(http::Method::PUT, ["tags", tag @ ..]) => server
				.handle_put_tag_request(request, &context, tag)
				.boxed(),
			(http::Method::DELETE, ["tags", tag @ ..]) => server
				.handle_delete_tag_request(request, &context, tag)
				.boxed(),

			// Users.
			(http::Method::GET, ["user"]) => {
				server.handle_get_user_request(request, &context).boxed()
			},

			(_, _) => future::ok(
				http::Response::builder()
					.status(http::StatusCode::NOT_FOUND)
					.bytes("not found")
					.unwrap(),
			)
			.boxed(),
		}
		.await;

		// Handle an error.
		let mut response = response.unwrap_or_else(|error| {
			tracing::error!(?error);
			let bytes = error
				.state()
				.object()
				.and_then(|arc| {
					let object = arc.unwrap_error_ref();
					serde_json::to_string(&object.to_data()).ok()
				})
				.unwrap_or_default();
			http::Response::builder()
				.status(http::StatusCode::INTERNAL_SERVER_ERROR)
				.bytes(bytes)
				.unwrap()
		});

		// Add the request ID to the response.
		let key = http::HeaderName::from_static("x-tg-request-id");
		let value = http::HeaderValue::from_str(&id.to_string()).unwrap();
		response.headers_mut().insert(key, value);

		response.map(|body| {
			Body::new(body.map_err(|error| {
				tracing::error!(?error, "response body error");
				error
			}))
		})
	}
}
