use {
	crate::Server,
	futures::{FutureExt as _, future},
	http_body_util::BodyExt as _,
	hyper_util::rt::{TokioExecutor, TokioIo},
	std::{convert::Infallible, path::Path, pin::pin, time::Duration},
	tangram_client as tg,
	tangram_futures::task::Stop,
	tangram_http::{Body, response::builder::Ext as _},
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
				let path = urlencoding::decode(path)
					.map_err(|source| tg::error!(!source, "invalid url"))?;
				let path = Path::new(path.as_ref());
				let listener = UnixListener::bind(path).map_err(
					|source| tg::error!(!source, %path = path.display(), "failed to bind"),
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

	pub(crate) async fn serve<H>(
		handle: H,
		listener: tokio_util::either::Either<tokio::net::UnixListener, tokio::net::TcpListener>,
		stop: Stop,
	) where
		H: tg::Handle,
	{
		// Create the task tracker.
		let task_tracker = tokio_util::task::TaskTracker::new();

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
				Ok::<_, std::io::Error>(TokioIo::new(stream))
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

			// Create the service.
			let idle = tangram_http::idle::Idle::new(Duration::from_secs(30));
			let service = tower::ServiceBuilder::new()
				.layer(tangram_http::layer::tracing::TracingLayer::new())
				.layer(tower_http::timeout::TimeoutLayer::new(Duration::from_secs(
					60,
				)))
				.add_extension(stop.clone())
				.map_response_body({
					let idle = idle.clone();
					move |body: Body| Body::new(tangram_http::idle::Body::new(idle.token(), body))
				})
				.layer(tangram_http::layer::compression::RequestDecompressionLayer)
				.layer(
					tangram_http::layer::compression::ResponseCompressionLayer::new(
						|accept_encoding, parts, _| {
							let has_content_length =
								parts.headers.get(http::header::CONTENT_LENGTH).is_some();
							let is_sync = parts
								.headers
								.get(http::header::CONTENT_TYPE)
								.is_some_and(|content_type| {
									matches!(content_type.to_str(), Ok(tg::sync::CONTENT_TYPE))
								});
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
					let handle = handle.clone();
					move |request| {
						let handle = handle.clone();
						async move {
							let response = Self::handle_request(&handle, request).await;
							Ok::<_, Infallible>(response)
						}
					}
				});

			// Spawn a task to serve the connection.
			task_tracker.spawn({
				let idle = idle.clone();
				let stop = stop.clone();
				async move {
					let mut builder =
						hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
					builder.http2().max_concurrent_streams(None);
					let service =
						service.map_request(|request: http::Request<hyper::body::Incoming>| {
							request.map(Body::new)
						});
					let service = hyper_util::service::TowerToHyperService::new(service);
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

		// Wait for all tasks to complete.
		task_tracker.close();
		task_tracker.wait().await;
	}

	async fn handle_request<H>(handle: &H, mut request: http::Request<Body>) -> http::Response<Body>
	where
		H: tg::Handle,
	{
		let id = tg::Id::new_uuidv7(tg::id::Kind::Request);
		request.extensions_mut().insert(id.clone());

		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		let response = match (method, path_components.as_slice()) {
			(http::Method::POST, ["cache"]) => Self::handle_cache_request(handle, request).boxed(),
			#[cfg(feature = "v8")]
			(http::Method::POST, ["check"]) => Self::handle_check_request(handle, request).boxed(),
			(http::Method::POST, ["checkin"]) => {
				Self::handle_checkin_request(handle, request).boxed()
			},
			(http::Method::POST, ["checkout"]) => {
				Self::handle_checkout_request(handle, request).boxed()
			},
			(http::Method::POST, ["clean"]) => {
				Self::handle_server_clean_request(handle, request).boxed()
			},
			#[cfg(feature = "v8")]
			(http::Method::POST, ["document"]) => Self::handle_document_request(handle, request).boxed(),
			#[cfg(feature = "v8")]
			(http::Method::POST, ["format"]) => Self::handle_format_request(handle, request).boxed(),
			(http::Method::GET, ["health"]) => {
				Self::handle_server_health_request(handle, request).boxed()
			},
			(http::Method::POST, ["index"]) => Self::handle_index_request(handle, request).boxed(),
			#[cfg(feature = "v8")]
			(http::Method::POST, ["lsp"]) => Self::handle_lsp_request(handle, request).boxed(),
			(http::Method::POST, ["pull"]) => Self::handle_pull_request(handle, request).boxed(),
			(http::Method::POST, ["push"]) => Self::handle_push_request(handle, request).boxed(),
			(http::Method::POST, ["sync"]) => Self::handle_sync_request(handle, request).boxed(),
			(http::Method::POST, ["blobs"]) => Self::handle_blob_request(handle, request).boxed(),
			(http::Method::GET, ["blobs", blob, "read"]) => {
				Self::handle_read_request(handle, request, blob).boxed()
			},
			(http::Method::GET, ["_", path @ ..]) => {
				Self::handle_get_request(handle, request, path).boxed()
			},

			// Objects.
			(http::Method::GET, ["objects", object, "metadata"]) => {
				Self::handle_get_object_metadata_request(handle, request, object).boxed()
			},
			(http::Method::GET, ["objects", object]) => {
				Self::handle_get_object_request(handle, request, object).boxed()
			},
			(http::Method::PUT, ["objects", object]) => {
				Self::handle_put_object_request(handle, request, object).boxed()
			},
			(http::Method::POST, ["objects", object, "touch"]) => {
				Self::handle_touch_object_request(handle, request, object).boxed()
			},

			// Processes.
			(http::Method::GET, ["processes"]) => {
				Self::handle_list_processes_request(handle, request).boxed()
			},
			(http::Method::POST, ["processes", "spawn"]) => {
				Self::handle_spawn_process_request(handle, request).boxed()
			},
			(http::Method::GET, ["processes", process, "metadata"]) => {
				Self::handle_get_process_metadata_request(handle, request, process).boxed()
			},
			(http::Method::GET, ["processes", process]) => {
				Self::handle_get_process_request(handle, request, process).boxed()
			},
			(http::Method::PUT, ["processes", process]) => {
				Self::handle_put_process_request(handle, request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "cancel"]) => {
				Self::handle_cancel_process_request(handle, request, process).boxed()
			},
			(http::Method::POST, ["processes", "dequeue"]) => {
				Self::handle_dequeue_process_request(handle, request).boxed()
			},
			(http::Method::POST, ["processes", process, "start"]) => {
				Self::handle_start_process_request(handle, request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "signal"]) => {
				Self::handle_post_process_signal_request(handle, request, process).boxed()
			},
			(http::Method::GET, ["processes", process, "signal"]) => {
				Self::handle_get_process_signal_request(handle, request, process).boxed()
			},
			(http::Method::GET, ["processes", process, "status"]) => {
				Self::handle_get_process_status_request(handle, request, process).boxed()
			},
			(http::Method::GET, ["processes", process, "children"]) => {
				Self::handle_get_process_children_request(handle, request, process).boxed()
			},
			(http::Method::GET, ["processes", process, "log"]) => {
				Self::handle_get_process_log_request(handle, request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "log"]) => {
				Self::handle_post_process_log_request(handle, request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "finish"]) => {
				Self::handle_finish_process_request(handle, request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "touch"]) => {
				Self::handle_touch_process_request(handle, request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "heartbeat"]) => {
				Self::handle_heartbeat_process_request(handle, request, process).boxed()
			},
			(http::Method::POST, ["processes", process, "wait"]) => {
				Self::handle_post_process_wait_request(handle, request, process).boxed()
			},

			// Pipes.
			(http::Method::POST, ["pipes"]) => {
				Self::handle_create_pipe_request(handle, request).boxed()
			},
			(http::Method::POST, ["pipes", pipe, "close"]) => {
				Self::handle_close_pipe_request(handle, request, pipe).boxed()
			},
			(http::Method::GET, ["pipes", pipe, "read"]) => {
				Self::handle_read_pipe_request(handle, request, pipe).boxed()
			},
			(http::Method::POST, ["pipes", pipe, "write"]) => {
				Self::handle_write_pipe_request(handle, request, pipe).boxed()
			},

			// Ptys.
			(http::Method::POST, ["ptys"]) => {
				Self::handle_create_pty_request(handle, request).boxed()
			},
			(http::Method::POST, ["ptys", pty, "close"]) => {
				Self::handle_close_pty_request(handle, request, pty).boxed()
			},
			(http::Method::GET, ["ptys", pty, "size"]) => {
				Self::handle_get_pty_size_request(handle, request, pty).boxed()
			},
			(http::Method::GET, ["ptys", pty, "read"]) => {
				Self::handle_read_pty_request(handle, request, pty).boxed()
			},
			(http::Method::POST, ["ptys", pty, "write"]) => {
				Self::handle_write_pty_request(handle, request, pty).boxed()
			},

			// Remotes.
			(http::Method::GET, ["remotes"]) => {
				Self::handle_list_remotes_request(handle, request).boxed()
			},
			(http::Method::GET, ["remotes", name]) => {
				Self::handle_get_remote_request(handle, request, name).boxed()
			},
			(http::Method::PUT, ["remotes", name]) => {
				Self::handle_put_remote_request(handle, request, name).boxed()
			},
			(http::Method::DELETE, ["remotes", name]) => {
				Self::handle_delete_remote_request(handle, request, name).boxed()
			},

			// Tags.
			(http::Method::GET, ["tags"]) => {
				Self::handle_list_tags_request(handle, request).boxed()
			},
			(http::Method::GET, ["tags", pattern @ ..]) => {
				Self::handle_get_tag_request(handle, request, pattern).boxed()
			},
			(http::Method::PUT, ["tags", tag @ ..]) => {
				Self::handle_put_tag_request(handle, request, tag).boxed()
			},
			(http::Method::DELETE, ["tags", tag @ ..]) => {
				Self::handle_delete_tag_request(handle, request, tag).boxed()
			},

			// Users.
			(http::Method::GET, ["user"]) => Self::handle_get_user_request(handle, request).boxed(),

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
			let bytes = serde_json::to_string(&error.to_data())
				.ok()
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
