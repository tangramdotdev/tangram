use super::Server;
use crate::{database::Json, params, BuildContext};
use futures::{stream, StreamExt, TryStreamExt};
use http_body_util::BodyExt;
use itertools::Itertools;
use std::sync::Arc;
use tangram_client as tg;
use tangram_error::{error, Error, Result, WrapErr};
use tangram_util::http::{bad_request, empty, full, not_found, Incoming, Outgoing};

mod children;
mod log;
mod outcome;
mod queue;
mod status;

impl Server {
	pub async fn try_list_builds(&self, arg: tg::build::ListArg) -> Result<tg::build::ListOutput> {
		let db = self.inner.database.get().await?;
		let order = match arg.sort {
			tg::build::ListSort::Timestamp => "state->>'timestamp' desc",
		};
		let statement = &format!(
			"
				select state
				from builds
				where state->>'target' = ?1
				order by {order}
				limit ?2
				offset ?3;
			"
		);
		let params = params![arg.target.to_string(), arg.limit, 0];
		let mut statement = db
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		let rows = statement
			.query(params)
			.wrap_err("Failed to execute the query.")?;
		let values = rows
			.and_then(|row| row.get::<_, Json<tg::build::State>>(0).map(|json| json.0))
			.try_collect()
			.wrap_err("Failed to deserialize the rows.")?;
		let output = tg::build::ListOutput { values };
		Ok(output)
	}

	pub async fn get_build_exists(&self, id: &tg::build::Id) -> Result<bool> {
		if self.get_build_exists_local(id).await? {
			return Ok(true);
		}
		if self.get_build_exists_remote(id).await? {
			return Ok(true);
		}
		Ok(false)
	}

	pub(crate) async fn get_build_exists_local(&self, id: &tg::build::Id) -> Result<bool> {
		let db = self.inner.database.get().await?;
		let statement = "
			select count(*) != 0
			from builds
			where id = ?1;
		";
		let params = params![id.to_string()];
		let mut statement = db
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		let mut rows = statement
			.query(params)
			.wrap_err("Failed to execute the query.")?;
		let row = rows
			.next()
			.wrap_err("Failed to retrieve the row.")?
			.wrap_err("Expected a row.")?;
		let exists = row
			.get::<_, bool>(0)
			.wrap_err("Failed to deserialize the column.")?;
		Ok(exists)
	}

	async fn get_build_exists_remote(&self, id: &tg::build::Id) -> Result<bool> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(false);
		};
		let exists = remote.get_build_exists(id).await?;
		Ok(exists)
	}

	pub async fn try_get_build(&self, id: &tg::build::Id) -> Result<Option<tg::build::GetOutput>> {
		if let Some(output) = self.try_get_build_local(id).await? {
			Ok(Some(output))
		} else if let Some(output) = self.try_get_build_remote(id).await? {
			Ok(Some(output))
		} else {
			Ok(None)
		}
	}

	async fn try_get_build_local(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::GetOutput>> {
		let db = self.inner.database.get().await?;
		let statement = "
			select state
			from builds
			where id = ?1;
		";
		let params = params![id.to_string()];
		let mut statement = db
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		let mut rows = statement
			.query(params)
			.wrap_err("Failed to execute the query.")?;
		let Some(row) = rows.next().wrap_err("Failed to retrieve the row.")? else {
			return Ok(None);
		};
		let state = row
			.get::<_, Json<tg::build::State>>(0)
			.wrap_err("Failed to deserialize the column.")?
			.0;
		let output = tg::build::GetOutput { state };
		Ok(Some(output))
	}

	async fn try_get_build_remote(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::GetOutput>> {
		// Get the remote.
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(None);
		};

		// Get the build from the remote server.
		let Some(output) = remote.try_get_build(id).await? else {
			return Ok(None);
		};

		// Add the build to the database.
		{
			let db = self.inner.database.get().await?;
			let statement = "
				insert into builds (id, state)
				values (?1, ?2)
				on conflict (id) do update set state = ?2;
			";
			let params = params![id.to_string(), Json(output.state.clone())];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		Ok(Some(output))
	}

	pub async fn try_put_build(
		&self,
		_user: Option<&tg::User>,
		id: &tg::build::Id,
		state: &tg::build::State,
	) -> Result<tg::build::PutOutput> {
		// Verify the build is finished.
		if state.status != tg::build::Status::Finished {
			return Err(error!("The build is not finished."));
		}

		// Get the missing builds.
		let builds = stream::iter(state.children.clone())
			.map(Ok)
			.try_filter_map(|id| async move {
				let exists = self.get_build_exists(&id).await?;
				Ok::<_, Error>(if exists { None } else { Some(id) })
			})
			.try_collect::<Vec<_>>()
			.await?;

		// Get the missing objects.
		let objects = stream::iter(state.objects())
			.map(Ok)
			.try_filter_map(|id| async move {
				let exists = self.get_object_exists(&id).await?;
				Ok::<_, Error>(if exists { None } else { Some(id) })
			})
			.try_collect::<Vec<_>>()
			.await?;

		// Insert the build if there are no missing builds or objects.
		if builds.is_empty() && objects.is_empty() {
			let db = self.inner.database.get().await?;
			let statement = "
				insert into builds (id, state)
				values (?1, ?2)
				on conflict do update set state = ?2;
			";
			let params = params![id.to_string(), Json(state.clone())];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		let output = tg::build::PutOutput {
			missing: tg::build::Missing { builds, objects },
		};

		Ok(output)
	}

	#[allow(clippy::too_many_lines)]
	pub async fn get_or_create_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::GetOrCreateArg,
	) -> Result<tg::build::GetOrCreateOutput> {
		// Get a local build if one exists that satisfies the retry constraint.
		let build = 'a: {
			// Find a build.
			let list_arg = tg::build::ListArg {
				limit: 1,
				sort: tg::build::ListSort::Timestamp,
				target: arg.target.clone(),
			};
			let Some(build) = self
				.try_list_builds(list_arg)
				.await?
				.values
				.first()
				.cloned()
				.map(|state| tg::Build::with_id(state.id))
			else {
				break 'a None;
			};

			// Verify the build satisfies the retry constraint.
			let outcome_arg = tg::build::outcome::GetArg {
				timeout: Some(std::time::Duration::ZERO),
			};
			let outcome = build.get_outcome(self, outcome_arg).await?;
			if let Some(outcome) = outcome {
				if outcome.retry() <= arg.options.retry {
					break 'a None;
				}
			}

			Some(build)
		};

		// Get a remote build if one exists that satisfies the retry constraint.
		let build = 'a: {
			if let Some(build) = build {
				break 'a Some(build);
			}

			// Get the remote.
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a None;
			};

			// Find a build.
			let list_arg = tg::build::ListArg {
				limit: 1,
				sort: tg::build::ListSort::Timestamp,
				target: arg.target.clone(),
			};
			let Some(build) = remote
				.try_list_builds(list_arg)
				.await?
				.values
				.first()
				.cloned()
				.map(|state| tg::Build::with_id(state.id))
			else {
				break 'a None;
			};

			// Verify the build satisfies the retry constraint.
			let outcome_arg = tg::build::outcome::GetArg {
				timeout: Some(std::time::Duration::ZERO),
			};
			let outcome = build.get_outcome(self, outcome_arg).await?;
			if let Some(outcome) = outcome {
				if outcome.retry() <= arg.options.retry {
					break 'a None;
				}
			}

			Some(build)
		};

		// If a local or remote build was found that satisfies the retry constraint, then return it.
		if let Some(build) = build {
			// Update the queue with the depth if it is greater.
			let updated = {
				let db = self.inner.database.get().await?;
				let statement = "
					update build_queue
					set
						options = json_set(options, '$.depth', (select json(max(depth, ?1)))),
						depth = (select max(depth, ?1))
					where build = ?2;
				";
				let params = params![arg.options.depth, build.id().to_string()];
				let mut statement = db
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let n = statement
					.execute(params)
					.wrap_err("Failed to execute the query.")?;
				n > 0
			};
			if updated {
				self.inner.local_queue_task_wake_sender.send_replace(());
			}

			// Add the build as a child of the parent.
			if let Some(parent) = arg.options.parent.as_ref() {
				self.add_build_child(user, parent, build.id()).await?;
			}

			let output = tg::build::GetOrCreateOutput {
				id: build.id().clone(),
			};

			return Ok(output);
		}

		// If the build has a parent and it is not local, or if the build has no parent and the remote flag is set, then attempt to get or create a remote build.
		'a: {
			// Determine if the build should be remote.
			let remote = if let Some(parent) = arg.options.parent.as_ref() {
				self.get_build_exists_local(parent).await?
			} else {
				arg.options.remote
			};
			if !remote {
				break 'a;
			}

			// Get the remote.
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};

			// Push the target.
			let object = tg::object::Handle::with_id(arg.target.clone().into());
			let Ok(()) = object.push(self, remote.as_ref()).await else {
				break 'a;
			};

			// Get or create the build on the remote.
			let options = tg::build::Options {
				remote: false,
				..arg.options.clone()
			};
			let arg = tg::build::GetOrCreateArg {
				target: arg.target.clone(),
				options,
			};
			let Ok(output) = remote.get_or_create_build(user, arg).await else {
				break 'a;
			};

			return Ok(output);
		}

		// Otherwise, create a new build.
		let build_id = tg::build::Id::new();

		// Get the host.
		let target = tg::Target::with_id(arg.target.clone());
		let host = target.host(self).await?;

		// Create the build context.
		let (children, _) = tokio::sync::watch::channel(());
		let (log, _) = tokio::sync::watch::channel(());
		let (status, _) = tokio::sync::watch::channel(());
		let (stop, _) = tokio::sync::watch::channel(false);
		let context = Arc::new(BuildContext {
			build: build_id.clone(),
			children: Some(children),
			depth: arg.options.depth,
			log: Some(log),
			status: Some(status),
			stop,
			task: std::sync::Mutex::new(None),
		});
		self.inner
			.build_context
			.write()
			.unwrap()
			.insert(build_id.clone(), context);

		// Add the build to the database.
		{
			let state = tg::build::State {
				children: Vec::new(),
				id: build_id.clone(),
				log: None,
				outcome: None,
				status: tg::build::Status::Queued,
				target: arg.target.clone(),
				timestamp: time::OffsetDateTime::now_utc(),
			};
			let db = self.inner.database.get().await?;
			let statement = "
				insert into builds (id, state)
				values (?1, ?2);
			";
			let params = params![build_id.to_string(), Json(state)];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		// Add the build to the queue.
		{
			let db = self.inner.database.get().await?;
			let statement = "
				insert into build_queue (build, options, host, depth)
				values (?1, ?2, ?3, ?4);
			";
			let params = params![
				build_id.to_string(),
				Json(arg.options.clone()),
				host.to_string(),
				arg.options.depth,
			];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		// Add the build to the parent.
		if let Some(parent) = arg.options.parent.as_ref() {
			self.add_build_child(user, parent, &build_id).await?;
		}

		// Send a message to the build queue task that the item has been added.
		self.inner.local_queue_task_wake_sender.send_replace(());

		let output = tg::build::GetOrCreateOutput { id: build_id };

		Ok(output)
	}
}

impl Server {
	pub async fn handle_list_builds_request(
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

	pub async fn handle_get_build_exists_request(
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

	pub async fn handle_get_build_request(
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

	pub async fn handle_put_build_request(
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

	pub async fn handle_get_or_create_build_request(
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
}
