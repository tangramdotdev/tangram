use crate::Server;
use bytes::Bytes;
use futures::{
	future::{self},
	stream, FutureExt, StreamExt, TryStreamExt,
};
use http_body_util::{BodyExt, StreamBody};
use hyper_util::rt::{TokioExecutor, TokioIo};
use itertools::Itertools;
use std::{collections::BTreeMap, convert::Infallible};
use tangram_client as tg;
use tangram_error::{return_error, Error, Result, WrapErr};
use tokio::net::{TcpListener, UnixListener};
use tokio_util::either::Either;

type Incoming = hyper::body::Incoming;

type Outgoing = http_body_util::combinators::UnsyncBoxBody<
	::bytes::Bytes,
	Box<dyn std::error::Error + Send + Sync + 'static>,
>;

impl Server {
	pub async fn serve(
		self,
		addr: tg::client::Addr,
		mut stop: tokio::sync::watch::Receiver<bool>,
	) -> Result<()> {
		// Create the tasks.
		let mut tasks = tokio::task::JoinSet::new();

		// Create the listener.
		let listener = match &addr {
			tg::client::Addr::Inet(inet) => Either::Left(
				TcpListener::bind(inet.to_string())
					.await
					.wrap_err("Failed to create the TCP listener.")?,
			),
			tg::client::Addr::Unix(path) => Either::Right(
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
					request.extensions_mut().insert(stop.clone());
					let server = server.clone();
					async move { Ok::<_, Infallible>(server.handle_request(request).await) }
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
	async fn handle_request(&self, request: http::Request<Incoming>) -> http::Response<Outgoing> {
		tracing::info!(method = ?request.method(), path = ?request.uri().path(), "Received request.");

		let method = request.method().clone();
		let path_components = request.uri().path().split('/').skip(1).collect_vec();
		let response = match (method, path_components.as_slice()) {
			// Server
			(http::Method::GET, ["v1", "health"]) => {
				self.handle_get_health_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["v1", "stop"]) => {
				self.handle_post_stop_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["v1", "clean"]) => {
				self.handle_post_clean_request(request).map(Some).boxed()
			},

			// Builds
			(http::Method::GET, ["v1", "targets", _, "build"]) => self
				.handle_get_assignment_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["v1", "targets", _, "build"]) => self
				.handle_get_or_create_build_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["v1", "builds", "queue"]) => self
				.handle_get_queue_item_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["v1", "builds", _, "status"]) => self
				.handle_get_build_status_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["v1", "builds", _, "status"]) => self
				.handle_post_build_status_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["v1", "builds", _, "target"]) => self
				.handle_get_build_target_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["v1", "builds", _, "children"]) => self
				.handle_get_build_children_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["v1", "builds", _, "children"]) => self
				.handle_post_build_child_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["v1", "builds", _, "log"]) => {
				self.handle_get_build_log_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["v1", "builds", _, "log"]) => self
				.handle_post_build_log_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["v1", "builds", _, "outcome"]) => self
				.handle_get_build_outcome_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["v1", "builds", _, "finish"]) => self
				.handle_post_build_finish_request(request)
				.map(Some)
				.boxed(),

			// Objects
			(http::Method::HEAD, ["v1", "objects", _]) => {
				self.handle_head_object_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["v1", "objects", _]) => {
				self.handle_get_object_request(request).map(Some).boxed()
			},
			(http::Method::PUT, ["v1", "objects", _]) => {
				self.handle_put_object_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["v1", "objects", _, "push"]) => {
				self.handle_push_object_request(request).map(Some).boxed()
			},
			(http::Method::POST, ["v1", "objects", _, "pull"]) => {
				self.handle_pull_object_request(request).map(Some).boxed()
			},

			// Artifacts
			(http::Method::POST, ["v1", "artifacts", "checkin"]) => self
				.handle_check_in_artifact_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["v1", "artifacts", "checkout"]) => self
				.handle_check_out_artifact_request(request)
				.map(Some)
				.boxed(),

			// Packages
			(http::Method::GET, ["v1", "packages", "search"]) => self
				.handle_search_packages_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["v1", "packages", _]) => {
				self.handle_get_package_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["v1", "packages", _, "versions"]) => self
				.handle_get_package_versions_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["v1", "packages", _, "metadata"]) => self
				.handle_get_package_metadata_request(request)
				.map(Some)
				.boxed(),
			(http::Method::GET, ["v1", "packages", _, "dependencies"]) => self
				.handle_get_package_dependencies_request(request)
				.map(Some)
				.boxed(),
			(http::Method::POST, ["v1", "packages"]) => self
				.handle_publish_package_request(request)
				.map(Some)
				.boxed(),

			// Users
			(http::Method::POST, ["v1", "logins"]) => {
				self.handle_create_login_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["v1", "logins", _]) => {
				self.handle_get_login_request(request).map(Some).boxed()
			},
			(http::Method::GET, ["v1", "user"]) => self
				.handle_get_user_for_token_request(request)
				.map(Some)
				.boxed(),

			(_, _) => future::ready(None).boxed(),
		}
		.await;

		let response = match response {
			None => http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
				.body(full("not found"))
				.unwrap(),
			Some(Err(error)) => {
				let trace = error.trace();
				tracing::error!(%trace);
				http::Response::builder()
					.status(http::StatusCode::INTERNAL_SERVER_ERROR)
					.body(full("internal server error"))
					.unwrap()
			},
			Some(Ok(response)) => response,
		};

		tracing::info!(status = ?response.status(), "Sending response.");

		response
	}

	async fn handle_get_health_request(
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

	async fn handle_post_stop_request(
		&self,
		_request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		self.stop().await?;
		Ok(ok())
	}

	async fn handle_post_clean_request(
		&self,
		_request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		self.clean().await?;
		Ok(http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap())
	}

	async fn handle_get_queue_item_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Get the search params.
		let hosts = if let Some(query) = request.uri().query() {
			let search_params: tg::client::GetBuildQueueItemSearchParams =
				serde_urlencoded::from_str(query).wrap_err("Failed to parse the search params.")?;
			search_params
				.hosts
				.map(|hosts| hosts.split(',').map(str::parse).try_collect())
				.transpose()?
		} else {
			None
		};

		let build_id = self.try_get_queue_item(user.as_ref(), hosts).await?;

		// Create the response.
		let body = serde_json::to_vec(&build_id).wrap_err("Failed to serialize the ID.")?;
		let response = http::Response::builder().body(full(body)).unwrap();
		Ok(response)
	}

	async fn handle_get_assignment_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let [_, "targets", id, "build"] = path_components.as_slice() else {
			return_error!("Unexpected path.");
		};
		let id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Attempt to get the assignment.
		let Some(build_id) = self.try_get_assignment(&id).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let body = serde_json::to_vec(&build_id).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();
		Ok(response)
	}

	async fn handle_get_or_create_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let [_, "targets", id, "build"] = path_components.as_slice() else {
			return_error!("Unexpected path.");
		};
		let id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Get the search params.
		let Some(query) = request.uri().query() else {
			return Ok(bad_request());
		};
		let search_params: tg::client::GetOrCreateBuildForTargetSearchParams =
			serde_urlencoded::from_str(query).wrap_err("Failed to parse the search params.")?;
		let parent = search_params.parent;
		let depth = search_params.depth;
		let retry = search_params.retry;

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Get or create the build.
		let build_id = self
			.get_or_create_build(user.as_ref(), &id, parent, depth, retry)
			.await?;

		// Create the response.
		let body = serde_json::to_vec(&build_id).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();
		Ok(response)
	}

	async fn handle_get_build_status_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let [_, "builds", id, "status"] = path_components.as_slice() else {
			return_error!("Unexpected path.");
		};
		let id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Attempt to get the build status.
		let Some(build_id) = self.try_get_build_status(&id).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let body = serde_json::to_vec(&build_id).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();
		Ok(response)
	}

	async fn handle_post_build_status_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let [_, "builds", id, "status"] = path_components.as_slice() else {
			return_error!("Unexpected path.");
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

	async fn handle_get_build_target_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let [_, "builds", id, "target"] = path_components.as_slice() else {
			return_error!("Unexpected path.");
		};
		let id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Attempt to get the build target.
		let Some(build_id) = self.try_get_build_target(&id).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let body = serde_json::to_vec(&build_id).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();
		Ok(response)
	}

	async fn handle_get_build_children_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let [_, "builds", id, "children"] = path_components.as_slice() else {
			return_error!("Unexpected path.");
		};
		let id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Attempt to get the children.
		let stop = request.extensions().get().cloned();
		let Some(children) = self.try_get_build_children(&id, stop).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let children = children
			.enumerate()
			.map(|(i, result)| match result {
				Ok(id) => Ok((i, id)),
				Err(error) => Err(error),
			})
			.map_ok(|(i, id)| {
				let mut string = String::new();
				if i != 0 {
					string.push(',');
				}
				string.push_str(&serde_json::to_string(&id).unwrap());
				let bytes = Bytes::from(string);
				hyper::body::Frame::data(bytes)
			})
			.map_err(Into::into);
		let open = stream::once(future::ok(hyper::body::Frame::data(Bytes::from("["))));
		let close = stream::once(future::ok(hyper::body::Frame::data(Bytes::from("]"))));
		let children = open.chain(children).chain(close);
		let body = Outgoing::new(StreamBody::new(children));
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(body)
			.unwrap();
		Ok(response)
	}

	async fn handle_post_build_child_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let [_, "builds", id, "children"] = path_components.as_slice() else {
			return_error!("Unexpected path.");
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
		let [_, "builds", id, "log"] = path_components.as_slice() else {
			return_error!("Unexpected path.");
		};
		let id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Get the log.
		let Some(log) = self.try_get_build_log(&id).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let body = Outgoing::new(StreamBody::new(
			log.map_ok(hyper::body::Frame::data).map_err(Into::into),
		));
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(body)
			.unwrap();
		Ok(response)
	}

	async fn handle_post_build_log_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let [_, "builds", id, "log"] = path_components.as_slice() else {
			return_error!("Unexpected path.");
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
		let [_, "builds", id, "outcome"] = path_components.as_slice() else {
			return_error!("Unexpected path.");
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

	async fn handle_post_build_finish_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let [_, "builds", build_id, "finish"] = path_components.as_slice() else {
			return_error!("Unexpected path.");
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

		// Finish the build.
		self.finish_build(user.as_ref(), &build_id, outcome).await?;

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap();
		Ok(response)
	}

	async fn handle_head_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["v1", "objects", id] = path_components.as_slice() else {
			return_error!("Unexpected path.")
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
		let ["v1", "objects", id] = path_components.as_slice() else {
			return_error!("Unexpected path.")
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Get the object.
		let Some(bytes) = self.try_get_object(&id).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(bytes))
			.unwrap();

		Ok(response)
	}

	async fn handle_put_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["v1", "objects", id] = path_components.as_slice() else {
			return_error!("Unexpected path.")
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
		let missing = self.try_put_object(&id, &bytes).await?;

		// Create the response.
		let body =
			serde_json::to_vec(&missing).wrap_err("Failed to serialize the missing children.")?;
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
			.unwrap();
		Ok(response)
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
		let body: tg::client::CheckinArtifactBody =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;

		// Check in the artifact.
		let id = self.check_in_artifact(&body.path).await?;

		// Create the response.
		let body = serde_json::to_vec(&id).wrap_err("Failed to serialize the response.")?;
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
		let body: tg::client::CheckoutArtifactBody =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;

		// Check out the artifact.
		self.check_out_artifact(&body.artifact, &body.path).await?;

		Ok(ok())
	}

	async fn handle_push_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["v1", "objects", id, "push"] = path_components.as_slice() else {
			return_error!("Unexpected path.")
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
		let ["v1", "objects", id, "pull"] = path_components.as_slice() else {
			return_error!("Unexpected path.")
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
		let search_params: tg::client::SearchPackagesSearchParams =
			serde_urlencoded::from_str(query).wrap_err("Failed to parse the search params.")?;

		// Perform the search.
		let packages = self.search_packages(&search_params.query).await?;

		// Create the response.
		let body = serde_json::to_vec(&packages).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	async fn handle_get_package_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let [_, "packages", dependency] = path_components.as_slice() else {
			return_error!("Unexpected path.");
		};
		let dependency =
			urlencoding::decode(dependency).wrap_err("Failed to decode the dependency.")?;
		let dependency = dependency
			.parse()
			.wrap_err("Failed to parse the dependency.")?;

		// Get the search params.
		let search_params: tg::client::GetPackageSearchParams = request
			.uri()
			.query()
			.map(|query| {
				serde_urlencoded::from_str(query)
					.wrap_err("Failed to deserialize the search params.")
			})
			.transpose()?
			.unwrap_or_default();

		// Get the package.
		let body = if search_params.lock {
			self.try_get_package_and_lock(&dependency)
				.await?
				.map(tg::client::GetPackageBody::PackageAndLock)
		} else {
			self.try_get_package(&dependency)
				.await?
				.map(tg::client::GetPackageBody::Package)
		};

		let Some(body) = body else {
			return Ok(not_found());
		};

		// Create the body.
		let body = serde_json::to_vec(&body).wrap_err("Failed to serialize the ID.")?;

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
		let [_, "packages", dependency, "versions"] = path_components.as_slice() else {
			return_error!("Unexpected path.");
		};
		let dependency =
			urlencoding::decode(dependency).wrap_err("Failed to decode the dependency.")?;
		let dependency = dependency
			.parse()
			.wrap_err("Failed to parse the dependency.")?;

		// Get the package.
		let source_artifact_hash = self.try_get_package_versions(&dependency).await?;

		// Create the response.
		let response = if let Some(source_artifact_hash) = source_artifact_hash {
			let body = serde_json::to_vec(&source_artifact_hash)
				.wrap_err("Failed to serialize the source artifact hash.")?;
			http::Response::builder().body(full(body)).unwrap()
		} else {
			not_found()
		};

		Ok(response)
	}

	async fn handle_get_package_metadata_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let [_, "packages", dependency, "metadata"] = path_components.as_slice() else {
			return_error!("Unexpected path.");
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

		// Create the body.
		let body = serde_json::to_vec(&metadata).wrap_err("Failed to serialize the metadata.")?;

		// Create the response.
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	async fn handle_get_package_dependencies_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let [_, "packages", dependency, "dependencies"] = path_components.as_slice() else {
			return_error!("Unexpected path.");
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

		// Create the body.
		let body =
			serde_json::to_vec(&dependencies).wrap_err("Failed to serialize the package.")?;

		// Create the response.
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
		let ["v1", "logins", id] = path_components.as_slice() else {
			return_error!("Unexpected path.");
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
		let body = serde_json::to_string(&user).wrap_err("Failed to serialize the user.")?;
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
fn full(chunk: impl Into<::bytes::Bytes>) -> Outgoing {
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
