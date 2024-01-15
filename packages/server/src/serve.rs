use crate::Server;
use bytes::{BufMut, Bytes, BytesMut};
use futures::{
	future::{self},
	FutureExt, TryStreamExt,
};
use http_body_util::{BodyExt, StreamBody};
use hyper_util::rt::{TokioExecutor, TokioIo};
use itertools::Itertools;
use num::ToPrimitive;
use std::{collections::BTreeMap, convert::Infallible};
use tangram_client as tg;
use tangram_error::{error, Error, Result, WrapErr};
use tokio::net::{TcpListener, UnixListener};
use tokio_util::either::Either;

type Incoming = hyper::body::Incoming;

type Outgoing = http_body_util::combinators::UnsyncBoxBody<
	Bytes,
	Box<dyn std::error::Error + Send + Sync + 'static>,
>;

impl Server {
	pub async fn serve(
		self,
		addr: tg::Addr,
		mut stop: tokio::sync::watch::Receiver<bool>,
	) -> Result<()> {
		// Create the tasks.
		let mut tasks = tokio::task::JoinSet::new();

		// Create the listener.
		let listener = match &addr {
			tg::Addr::Inet(inet) => Either::Left(
				TcpListener::bind(inet.to_string())
					.await
					.wrap_err("Failed to create the TCP listener.")?,
			),
			tg::Addr::Unix(path) => Either::Right(
				UnixListener::bind(path).wrap_err("Failed to create the UNIX listener.")?,
			),
		};

		tracing::info!("🚀 Serving on {addr:?}.");

		loop {
			// Accept a new connection.
			let accept = async {
				let stream = match &listener {
					Either::Left(listener) => Either::Left(
						listener
							.accept()
							.await
							.wrap_err("Failed to accept a new TCP connection.")?
							.0,
					),
					Either::Right(listener) => Either::Right(
						listener
							.accept()
							.await
							.wrap_err("Failed to accept a new UNIX connection.")?
							.0,
					),
				};
				Ok::<_, Error>(TokioIo::new(stream))
			};
			let stream = tokio::select! {
				stream = accept => stream?,
				() = stop.wait_for(|stop| *stop).map(|_| ()) => {
					break
				},
			};

			// Create the service.
			let service = hyper::service::service_fn({
				let server = self.clone();
				let stop = stop.clone();
				move |mut request| {
					let server = server.clone();
					let stop = stop.clone();
					async move {
						request.extensions_mut().insert(stop);
						let response = server.handle_request(request).await;
						Ok::<_, Infallible>(response)
					}
				}
			});

			// Spawn a task to serve the connection.
			tasks.spawn({
				let mut stop = stop.clone();
				async move {
					let builder =
						hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
					let connection = builder.serve_connection_with_upgrades(stream, service);
					tokio::pin!(connection);
					let result = tokio::select! {
						result = connection.as_mut() => {
							Some(result)
						},
						() = stop.wait_for(|stop| *stop).map(|_| ()) => {
							connection.as_mut().graceful_shutdown();
							None
						}
					};
					let result = match result {
						Some(result) => result,
						None => connection.await,
					};
					if let Err(error) = result {
						tracing::error!(?error, "Failed to serve the connection.");
					}
				}
			});
		}

		// Join all tasks.
		while let Some(result) = tasks.join_next().await {
			result.unwrap();
		}

		Ok(())
	}

	async fn try_get_user_from_request(
		&self,
		request: &http::Request<Incoming>,
	) -> Result<Option<tg::user::User>> {
		// Get the token.
		let Some(token) = get_token(request, None) else {
			return Ok(None);
		};

		// Get the user.
		let user = self.get_user_for_token(&token).await?;

		Ok(user)
	}

	#[allow(clippy::too_many_lines)]
	async fn handle_request(
		&self,
		mut request: http::Request<Incoming>,
	) -> http::Response<Outgoing> {
		let id = tg::Id::new_uuidv7(tg::id::Kind::Request);
		request.extensions_mut().insert(id.clone());

		tracing::info!(?id, method = ?request.method(), path = ?request.uri().path(), "Received request.");

		let method = request.method().clone();
		let path_components = request.uri().path().split('/').skip(1).collect_vec();
		let response = match (method, path_components.as_slice()) {
			// Artifacts
			(http::Method::POST, ["artifacts", "checkin"]) => self
				.handle_check_in_artifact_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["artifacts", "checkout"]) => self
				.handle_check_out_artifact_request(request)
				.map(Some)
				.boxed(),

			// Builds
			(http::Method::GET, ["builds"]) => {
				self.handle_list_builds_request(request).map(Some).boxed()
			},
			(http::Method::HEAD, ["builds", _]) => self
				.handle_get_build_exists_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["builds", _]) => {
				self.handle_get_build_request(request).map(Some).boxed()
			},
			(http::Method::PUT, ["builds", _]) => {
				self.handle_put_build_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["builds"]) => self
				.handle_get_or_create_build_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["builds", "dequeue"]) => {
				self.handle_dequeue_build_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["builds", _, "status"]) => self
				.handle_get_build_status_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["builds", _, "status"]) => self
				.handle_set_build_status_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["builds", _, "children"]) => self
				.handle_get_build_children_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["builds", _, "children"]) => self
				.handle_add_build_child_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["builds", _, "log"]) => {
				self.handle_get_build_log_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["builds", _, "log"]) => {
				self.handle_add_build_log_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["builds", _, "outcome"]) => self
				.handle_get_build_outcome_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["builds", _, "outcome"]) => self
				.handle_set_build_outcome_request(request)
				.map(Some)
				.boxed(),

			// Objects
			(http::Method::HEAD, ["objects", _]) => self
				.handle_get_object_exists_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["objects", _]) => {
				self.handle_get_object_request(request).map(Some).boxed()
			},
			(http::Method::PUT, ["objects", _]) => {
				self.handle_put_object_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["objects", _, "push"]) => {
				self.handle_push_object_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["objects", _, "pull"]) => {
				self.handle_pull_object_request(request).map(Some).boxed()
			},

			// Packages
			(http::Method::GET, ["packages", "search"]) => self
				.handle_search_packages_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["packages", _]) => {
				self.handle_get_package_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["packages", _, "versions"]) => self
				.handle_get_package_versions_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["packages", _, "metadata"]) => self
				.handle_get_package_metadata_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["packages", _, "dependencies"]) => self
				.handle_get_package_dependencies_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["packages"]) => self
				.handle_publish_package_request(request)
				.map(Some)
				.boxed(),

			// Server
			(http::Method::GET, ["health"]) => {
				self.handle_health_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["clean"]) => self.handle_clean_request(request).map(Some).boxed(),
			(http::Method::POST, ["stop"]) => self.handle_stop_request(request).map(Some).boxed(),

			// Users
			(http::Method::POST, ["logins"]) => {
				self.handle_create_login_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["logins", _]) => {
				self.handle_get_login_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["user"]) => self
				.handle_get_user_for_token_request(request)
				.map(Some)
				.boxed(),

			(_, _) => future::ready(None).boxed(),
		}
		.await;

		let mut response = match response {
			None => http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
				.body(full("not found"))
				.unwrap(),
			Some(Err(error)) => {
				let body = serde_json::to_string(&error)
					.unwrap_or_else(|_| "internal server error".to_owned());
				http::Response::builder()
					.status(http::StatusCode::INTERNAL_SERVER_ERROR)
					.body(full(body))
					.unwrap()
			},
			Some(Ok(response)) => response,
		};

		let key = http::HeaderName::from_static("x-tangram-request-id");
		let value = http::HeaderValue::from_str(&id.to_string()).unwrap();
		response.headers_mut().insert(key, value);

		tracing::info!(?id, status = ?response.status(), "Sending response.");

		response
	}

	async fn handle_check_in_artifact_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;

		// Check in the artifact.
		let output = self.check_in_artifact(arg).await?;

		// Create the response.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	async fn handle_check_out_artifact_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;

		// Check out the artifact.
		self.check_out_artifact(arg).await?;

		Ok(ok())
	}

	async fn handle_list_builds_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Read the search params.
		let Some(query) = request.uri().query() else {
			return Ok(bad_request());
		};
		let arg = serde_urlencoded::from_str(query)
			.wrap_err("Failed to deserialize the search params.")?;

		let output = self.try_list_builds(arg).await?;

		// Create the response.
		let body = serde_json::to_string(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
			.unwrap();

		Ok(response)
	}

	async fn handle_get_build_exists_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Get whether the build exists.
		let exists = self.get_build_exists(&id).await?;

		// Create the response.
		let status = if exists {
			http::StatusCode::OK
		} else {
			http::StatusCode::NOT_FOUND
		};
		let response = http::Response::builder()
			.status(status)
			.body(empty())
			.unwrap();

		Ok(response)
	}

	async fn handle_get_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", build_id] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let build_id = build_id.parse().wrap_err("Failed to parse the ID.")?;

		// Get the build.
		let Some(state) = self.try_get_build(&build_id).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let body = serde_json::to_string(&state).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
			.unwrap();

		Ok(response)
	}

	async fn handle_put_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", build_id] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let build_id = build_id.parse().wrap_err("Failed to parse the ID.")?;

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let state = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;

		// Put the build.
		let output = self.try_put_build(user.as_ref(), &build_id, &state).await?;

		// Create the response.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
			.unwrap();

		Ok(response)
	}

	async fn handle_get_or_create_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;

		// Get or create the build.
		let output = self.get_or_create_build(user.as_ref(), arg).await?;

		// Create the response.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	async fn handle_dequeue_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;

		let output = self.try_dequeue_build(user.as_ref(), arg).await?;

		// Create the response.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	async fn handle_get_build_status_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "status"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Attempt to get the build status.
		let Some(status) = self.try_get_build_status(&id).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let body = serde_json::to_vec(&status).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	async fn handle_set_build_status_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "status"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let build_id: tg::build::Id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let status = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;

		self.set_build_status(user.as_ref(), &build_id, status)
			.await?;

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap();

		Ok(response)
	}

	async fn handle_get_build_children_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "children"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Attempt to get the children.
		let stop = request.extensions().get().cloned();
		let Some(children) = self.try_get_build_children(&id, stop).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let children = children
			.map_ok(|id| {
				let mut bytes = BytesMut::new();
				let string = id.to_string();
				bytes.put_u64(string.len().to_u64().unwrap());
				bytes.put(string.as_bytes());
				hyper::body::Frame::data(bytes.into())
			})
			.map_err(Into::into);
		let body = Outgoing::new(StreamBody::new(children));
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(body)
			.unwrap();

		Ok(response)
	}

	async fn handle_add_build_child_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "children"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let build_id: tg::build::Id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let child_id =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;

		self.add_build_child(user.as_ref(), &build_id, &child_id)
			.await?;

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap();

		Ok(response)
	}

	async fn handle_get_build_log_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "log"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Get the search params.
		let arg = request
			.uri()
			.query()
			.map(serde_urlencoded::from_str)
			.transpose()
			.wrap_err("Failed to deserialize the search params.")?
			.unwrap_or_default();

		// Get the log.
		let Some(log) = self.try_get_build_log(&id, arg).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let body = Outgoing::new(StreamBody::new(
			log.map_ok(|entry| {
				let mut bytes = BytesMut::new();
				bytes.put_u64(entry.pos.to_u64().unwrap());
				bytes.put_u64(entry.bytes.len().to_u64().unwrap());
				bytes.put(entry.bytes);
				hyper::body::Frame::data(bytes.into())
			})
			.map_err(Into::into),
		));

		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(body)
			.unwrap();

		Ok(response)
	}

	async fn handle_add_build_log_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "log"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let build_id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();

		self.add_build_log(user.as_ref(), &build_id, bytes).await?;

		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap();

		Ok(response)
	}

	async fn handle_get_build_outcome_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "outcome"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Attempt to get the outcome.
		let stop = request.extensions().get().cloned();
		let Some(outcome) = self.try_get_build_outcome(&id, stop).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let outcome = outcome.data(self).await?;
		let body = serde_json::to_vec(&outcome).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
			.unwrap();

		Ok(response)
	}

	async fn handle_set_build_outcome_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", build_id, "outcome"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let build_id = build_id.parse().wrap_err("Failed to parse the ID.")?;

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let outcome = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize.")?;

		// Set the outcome.
		self.set_build_outcome(user.as_ref(), &build_id, outcome)
			.await?;

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap();

		Ok(response)
	}

	async fn handle_get_object_exists_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Get whether the object exists.
		let exists = self.get_object_exists(&id).await?;

		// Create the response.
		let status = if exists {
			http::StatusCode::OK
		} else {
			http::StatusCode::NOT_FOUND
		};
		let response = http::Response::builder()
			.status(status)
			.body(empty())
			.unwrap();

		Ok(response)
	}

	async fn handle_get_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Get the object.
		let Some(output) = self.try_get_object(&id).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(output.bytes))
			.unwrap();

		Ok(response)
	}

	async fn handle_put_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();

		// Put the object.
		let output = self.try_put_object(&id, &bytes).await?;

		// Create the response.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
			.unwrap();

		Ok(response)
	}

	async fn handle_push_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id, "push"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Push the object.
		self.push_object(&id).await?;

		Ok(ok())
	}

	async fn handle_pull_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id, "pull"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Pull the object.
		self.pull_object(&id).await?;

		Ok(ok())
	}

	async fn handle_search_packages_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Read the search params.
		let Some(query) = request.uri().query() else {
			return Ok(bad_request());
		};
		let arg = serde_urlencoded::from_str(query)
			.wrap_err("Failed to deserialize the search params.")?;

		// Perform the search.
		let output = self.search_packages(arg).await?;

		// Create the response.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	async fn handle_get_package_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let dependency =
			urlencoding::decode(dependency).wrap_err("Failed to decode the dependency.")?;
		let dependency = dependency
			.parse()
			.wrap_err("Failed to parse the dependency.")?;

		// Get the search params.
		let arg = request
			.uri()
			.query()
			.map(serde_urlencoded::from_str)
			.transpose()
			.wrap_err("Failed to deserialize the search params.")?
			.unwrap_or_default();

		// Get the package.
		let Some(output) = self.try_get_package(&dependency, arg).await? else {
			return Ok(not_found());
		};

		// Create the body.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;

		// Create the response.
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	async fn handle_get_package_versions_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency, "versions"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let dependency =
			urlencoding::decode(dependency).wrap_err("Failed to decode the dependency.")?;
		let dependency = dependency
			.parse()
			.wrap_err("Failed to parse the dependency.")?;

		// Get the package.
		let Some(output) = self.try_get_package_versions(&dependency).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	async fn handle_get_package_metadata_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency, "metadata"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let dependency =
			urlencoding::decode(dependency).wrap_err("Failed to decode the dependency.")?;
		let dependency = dependency
			.parse()
			.wrap_err("Failed to parse the dependency.")?;

		// Get the package metadata.
		let Some(metadata) = self.try_get_package_metadata(&dependency).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let body = serde_json::to_vec(&metadata).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	async fn handle_get_package_dependencies_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", dependency, "dependencies"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let dependency =
			urlencoding::decode(dependency).wrap_err("Failed to decode the dependency.")?;
		let dependency = dependency
			.parse()
			.wrap_err("Failed to parse the dependency.")?;

		// Get the package dependencies.
		let Some(dependencies) = self.try_get_package_dependencies(&dependency).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let body =
			serde_json::to_vec(&dependencies).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	async fn handle_publish_package_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let package_id = serde_json::from_slice(&bytes).wrap_err("Invalid request.")?;

		// Publish the package.
		self.publish_package(user.as_ref(), &package_id).await?;

		Ok(ok())
	}

	async fn handle_health_request(
		&self,
		_request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		let health = self.health().await?;
		let body = serde_json::to_vec(&health).unwrap();
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
			.unwrap();
		Ok(response)
	}

	async fn handle_clean_request(
		&self,
		_request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		self.clean().await?;
		Ok(http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap())
	}

	#[allow(clippy::unnecessary_wraps, clippy::unused_async)]
	async fn handle_stop_request(
		&self,
		_request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		self.stop();
		Ok(ok())
	}

	async fn handle_create_login_request(
		&self,
		_request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Create the login.
		let login = self.create_login().await?;

		// Create the response.
		let body = serde_json::to_string(&login).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder()
			.status(200)
			.body(full(body))
			.unwrap();

		Ok(response)
	}

	async fn handle_get_login_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["logins", id] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Get the login.
		let login = self.get_login(&id).await?;

		// Create the response.
		let response =
			serde_json::to_string(&login).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder()
			.status(200)
			.body(full(response))
			.unwrap();

		Ok(response)
	}

	async fn handle_get_user_for_token_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the token from the request.
		let Some(token) = get_token(&request, None) else {
			return Ok(unauthorized());
		};

		// Authenticate the user.
		let Some(user) = self.get_user_for_token(token.as_str()).await? else {
			return Ok(unauthorized());
		};

		// Create the response.
		let body = serde_json::to_string(&user).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
			.unwrap();

		Ok(response)
	}
}

#[must_use]
fn empty() -> Outgoing {
	http_body_util::Empty::new()
		.map_err(Into::into)
		.boxed_unsync()
}

#[must_use]
fn full(chunk: impl Into<Bytes>) -> Outgoing {
	http_body_util::Full::new(chunk.into())
		.map_err(Into::into)
		.boxed_unsync()
}

/// Get a bearer token or cookie from an HTTP request.
fn get_token(request: &http::Request<Incoming>, name: Option<&str>) -> Option<String> {
	if let Some(authorization) = request.headers().get(http::header::AUTHORIZATION) {
		let Ok(authorization) = authorization.to_str() else {
			return None;
		};
		let mut components = authorization.split(' ');
		let token = match (components.next(), components.next()) {
			(Some("Bearer"), Some(token)) => token.to_owned(),
			_ => return None,
		};
		Some(token)
	} else if let Some(cookies) = request.headers().get(http::header::COOKIE) {
		if let Some(name) = name {
			let Ok(cookies) = cookies.to_str() else {
				return None;
			};
			let cookies: BTreeMap<&str, &str> = match parse_cookies(cookies).collect() {
				Ok(cookies) => cookies,
				Err(_) => return None,
			};
			let token = match cookies.get(name) {
				Some(&token) => token.to_owned(),
				None => return None,
			};
			Some(token)
		} else {
			None
		}
	} else {
		None
	}
}

/// Parse an HTTP cookie string.
fn parse_cookies(cookies: &str) -> impl Iterator<Item = Result<(&str, &str)>> {
	cookies.split("; ").map(|cookie| {
		let mut components = cookie.split('=');
		let key = components
			.next()
			.wrap_err("Expected a key in the cookie string.")?;
		let value = components
			.next()
			.wrap_err("Expected a value in the cookie string.")?;
		Ok((key, value))
	})
}

/// 200
#[must_use]
fn ok() -> http::Response<Outgoing> {
	http::Response::builder()
		.status(http::StatusCode::OK)
		.body(empty())
		.unwrap()
}

/// 400
#[must_use]
fn bad_request() -> http::Response<Outgoing> {
	http::Response::builder()
		.status(http::StatusCode::BAD_REQUEST)
		.body(full("bad request"))
		.unwrap()
}

/// 401
#[must_use]
fn unauthorized() -> http::Response<Outgoing> {
	http::Response::builder()
		.status(http::StatusCode::UNAUTHORIZED)
		.body(full("unauthorized"))
		.unwrap()
}

/// 404
#[must_use]
fn not_found() -> http::Response<Outgoing> {
	http::Response::builder()
		.status(http::StatusCode::NOT_FOUND)
		.body(full("not found"))
		.unwrap()
}
