use super::log;
use crate::{
	database::{Database, PostgresJson, SqliteJson},
	postgres_params, sqlite_params, Http, Server,
};
use async_recursion::async_recursion;
use futures::{future, stream::FuturesUnordered, TryFutureExt, TryStreamExt};
use http_body_util::BodyExt;
use itertools::Itertools;
use num::ToPrimitive;
use tangram_client as tg;
use tangram_error::{error, Result, Wrap, WrapErr};
use tangram_util::{
	http::{empty, full, not_found, Incoming, Outgoing},
	iter::IterExt,
};

impl Server {
	pub async fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<Option<tg::build::Outcome>>> {
		if let Some(outcome) = self
			.try_get_build_outcome_local(id, arg.clone(), stop.clone())
			.await?
		{
			Ok(Some(outcome))
		} else if let Some(outcome) = self
			.try_get_build_outcome_remote(id, arg.clone(), stop.clone())
			.await?
		{
			Ok(Some(outcome))
		} else {
			Ok(None)
		}
	}

	async fn try_get_build_outcome_local(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<Option<tg::build::Outcome>>> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}

		// Wait for the build to finish.
		let arg = tg::build::status::GetArg {
			timeout: arg.timeout,
		};
		let finished = self
			.try_get_build_status_local(id, arg, stop)
			.await?
			.wrap_err("Expected the build to exist.")?
			.try_filter_map(|status| {
				future::ready(Ok(if status == tg::build::Status::Finished {
					Some(())
				} else {
					None
				}))
			})
			.try_next()
			.map_ok(|option| option.is_some())
			.await?;
		if !finished {
			return Ok(Some(None));
		}

		// Get the outcome.
		let outcome = match &self.inner.database {
			Database::Sqlite(database) => {
				let connection = database.get().await?;
				let statement = "
					select outcome
					from builds
					where id = ?1;
				";
				let params = sqlite_params![id.to_string()];
				let mut statement = connection
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let mut rows = statement
					.query(params)
					.wrap_err("Failed to execute the statement.")?;
				let row = rows
					.next()
					.wrap_err("Failed to get the row.")?
					.wrap_err("Expected a row.")?;
				row.get::<_, SqliteJson<Option<tg::build::Outcome>>>(0)
					.wrap_err("Failed to deserialize the column.")?
					.0
					.wrap_err("Expected the outcome to be set.")?
			},

			Database::Postgres(database) => {
				let connection = database.get().await?;
				let statement = "
					select outcome
					from builds
					where id = $1;
				";
				let params = postgres_params![id.to_string()];
				let statement = connection
					.prepare_cached(statement)
					.await
					.wrap_err("Failed to prepare the query.")?;
				let rows = connection
					.query(&statement, params)
					.await
					.wrap_err("Failed to execute the statement.")?;
				let row = rows.into_iter().next().wrap_err("Expected a row.")?;
				row.try_get::<_, PostgresJson<Option<tg::build::Outcome>>>(0)
					.wrap_err("Failed to deserialize the column.")?
					.0
					.wrap_err("Expected the outcome to be set.")?
			},
		};

		Ok(Some(Some(outcome)))
	}

	async fn try_get_build_outcome_remote(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<Option<tg::build::Outcome>>> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(None);
		};
		let Some(outcome) = remote.try_get_build_outcome(id, arg, stop).await? else {
			return Ok(None);
		};
		Ok(Some(outcome))
	}

	#[async_recursion]
	pub async fn set_build_outcome(
		&self,
		user: Option<&'async_recursion tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<()> {
		if self
			.try_set_build_outcome_local(user, id, outcome.clone())
			.await?
		{
			return Ok(());
		}
		if self
			.try_set_build_outcome_remote(user, id, outcome.clone())
			.await?
		{
			return Ok(());
		}
		Err(error!("Failed to get the build."))
	}

	async fn try_set_build_outcome_local(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<bool> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(false);
		}

		// If the build is finished, then return.
		let arg = tg::build::status::GetArg {
			timeout: Some(std::time::Duration::ZERO),
		};
		let status = self
			.try_get_build_status_local(id, arg, None)
			.await?
			.wrap_err("Expected the build to exist.")?
			.try_next()
			.await?
			.wrap_err("Failed to get the status.")?;
		if status == tg::build::Status::Finished {
			return Ok(true);
		}

		// Get the children.
		let children: Vec<tg::build::Id> = {
			match &self.inner.database {
				Database::Sqlite(database) => {
					let connection = database.get().await?;
					let statement = "
						select child
						from build_children
						where build = ?1
						order by position;
					";
					let params = sqlite_params![id.to_string()];
					let mut statement = connection
						.prepare_cached(statement)
						.wrap_err("Failed to prepare the query.")?;
					let rows = statement
						.query(params)
						.wrap_err("Failed to execute the statement.")?;
					let rows = rows
						.and_then(|row| row.get::<_, String>(0))
						.map_err(|error| error.wrap("Failed to deserialize the rows."));
					rows.and_then(|id| id.parse()).try_collect()?
				},

				Database::Postgres(database) => {
					let connection = database.get().await?;
					let statement = "
						select child
						from build_children
						where build = $1
						order by position;
					";
					let params = postgres_params![id.to_string()];
					let statement = connection
						.prepare_cached(statement)
						.await
						.wrap_err("Failed to prepare the query.")?;
					let rows = connection
						.query(&statement, params)
						.await
						.wrap_err("Failed to execute the statement.")?;
					let rows = rows
						.into_iter()
						.map(|row| row.try_get::<_, String>(0))
						.map_err(|error| error.wrap("Failed to deserialize the rows."));
					rows.and_then(|id| id.parse()).try_collect()?
				},
			}
		};

		// If the build was terminated or canceled, then terminate or cancel the children.
		if matches!(outcome, tg::build::Outcome::Canceled) {
			children
				.iter()
				.map(|child| {
					let outcome = outcome.clone();
					async move { self.set_build_outcome(user, child, outcome.clone()).await }
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect()
				.await?;
		}

		// Get the children outcomes.
		let children_outcomes = children
			.iter()
			.map(|child_id| async move {
				self.try_get_build_outcome(child_id, tg::build::outcome::GetArg::default(), None)
					.await?
					.wrap_err("Failed to get the build.")?
					.wrap_err("Failed to get the outcome.")
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;

		// If any of the children were canceled, then this build should be canceled.
		let outcome = if children_outcomes
			.iter()
			.any(|outcome| outcome.try_unwrap_terminated_ref().is_ok())
		{
			tg::build::Outcome::Terminated
		} else if children_outcomes
			.iter()
			.any(|outcome| outcome.try_unwrap_canceled_ref().is_ok())
		{
			tg::build::Outcome::Canceled
		} else {
			outcome
		};
		let outcome = outcome.data(self).await?;

		// Create a blob from the log.
		let log = tg::Blob::with_reader(self, log::Reader::new(self, id).await?).await?;
		let log = log.id(self).await?;

		// Add the log to the build objects.
		match &self.inner.database {
			Database::Sqlite(database) => {
				let connection = database.get().await?;
				let statement = "
					insert into build_objects (build, object)
					values (?1, ?2)
					on conflict (build, object) do nothing;
				";
				let build = id.to_string();
				let object = log.to_string();
				let params = sqlite_params![build, object];
				let mut statement = connection
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				statement
					.execute(params)
					.wrap_err("Failed to execute the statement.")?;
			},

			Database::Postgres(database) => {
				let connection = database.get().await?;
				let statement = "
					insert into build_objects (build, object)
					values ($1, $2)
					on conflict (build, object) do nothing;
				";
				let build = id.to_string();
				let object = log.to_string();
				let params = postgres_params![build, object];
				let statement = connection
					.prepare_cached(statement)
					.await
					.wrap_err("Failed to prepare the query.")?;
				connection
					.execute(&statement, params)
					.await
					.wrap_err("Failed to execute the statement.")?;
			},
		}

		// Add the outcome's children to the build objects.
		let objects = outcome
			.try_unwrap_succeeded_ref()
			.map(tg::value::Data::children)
			.into_iter()
			.flatten();
		match &self.inner.database {
			Database::Sqlite(database) => {
				let connection = database.get().await?;
				for object in objects {
					let statement = "
						insert into build_objects (build, object)
						values (?1, ?2)
						on conflict (build, object) do nothing;
					";
					let build = id.to_string();
					let object = object.to_string();
					let params = sqlite_params![build, object];
					let mut statement = connection
						.prepare_cached(statement)
						.wrap_err("Failed to prepare the query.")?;
					statement
						.execute(params)
						.wrap_err("Failed to execute the statement.")?;
				}
			},

			Database::Postgres(database) => {
				let connection = database.get().await?;
				for object in objects {
					let statement = "
						insert into build_objects (build, object)
						values ($1, $2)
						on conflict (build, object) do nothing;
					";
					let build = id.to_string();
					let object = object.to_string();
					let params = postgres_params![build, object];
					let statement = connection
						.prepare_cached(statement)
						.await
						.wrap_err("Failed to prepare the query.")?;
					connection
						.execute(&statement, params)
						.await
						.wrap_err("Failed to execute the statement.")?;
				}
			},
		}

		// Compute the descendants.
		let descendants: u64 = 0;

		// Compute the weight.
		let weight: u64 = 0;

		// Update the build.
		match &self.inner.database {
			Database::Sqlite(database) => {
				let status = tg::build::Status::Finished;
				let connection = database.get().await?;
				let statement = "
					update builds
					set
						children = ?1,
						descendants = ?2,
						log = ?3,
						outcome = ?4,
						status = ?5,
						weight = ?6
					where id = ?7;
				";
				let children = SqliteJson(children);
				let descendants = descendants.to_i64().unwrap();
				let log = log.to_string();
				let outcome = SqliteJson(outcome);
				let status = status.to_string();
				let weight = weight.to_i64().unwrap();
				let id = id.to_string();
				let params =
					sqlite_params![children, descendants, log, outcome, status, weight, id];
				let mut statement = connection
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				statement
					.execute(params)
					.wrap_err("Failed to execute the statement.")?;
			},

			Database::Postgres(database) => {
				let status = tg::build::Status::Finished;
				let connection = database.get().await?;
				let statement = "
					update builds
					set
						children = $1,
						descendants = $2,
						log = $3,
						outcome = $4,
						status = $5,
						weight = $6
					where id = $7;
				";
				let children = PostgresJson(children);
				let descendants = descendants.to_i64().unwrap();
				let log = log.to_string();
				let outcome = PostgresJson(outcome);
				let status = status.to_string();
				let weight = weight.to_i64().unwrap();
				let id = id.to_string();
				let params =
					postgres_params![children, descendants, log, outcome, status, weight, id];
				let statement = connection
					.prepare_cached(statement)
					.await
					.wrap_err("Failed to prepare the query.")?;
				connection
					.execute(&statement, params)
					.await
					.wrap_err("Failed to execute the statement.")?;
			},
		}

		// Publish the message.
		self.inner.messenger.publish_to_build_status(id).await?;

		Ok(true)
	}

	async fn try_set_build_outcome_remote(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<bool> {
		// Get the remote handle.
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(false);
		};

		// Push the output if the build succeeded.
		if let tg::build::Outcome::Succeeded(value) = &outcome {
			value.push(self, remote.as_ref()).await?;
		}

		// Set the outcome.
		remote.set_build_outcome(user, id, outcome).await?;

		Ok(true)
	}
}

impl Http {
	pub async fn handle_get_build_outcome_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "outcome"] = path_components.as_slice() else {
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

		let stop = request.extensions().get().cloned();
		let Some(outcome) = self.inner.tg.try_get_build_outcome(&id, arg, stop).await? else {
			return Ok(not_found());
		};

		// Create the body.
		let outcome = if let Some(outcome) = outcome {
			Some(outcome.data(self.inner.tg.as_ref()).await?)
		} else {
			None
		};
		let body = serde_json::to_vec(&outcome).wrap_err("Failed to serialize the response.")?;

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
			.unwrap();

		Ok(response)
	}

	pub async fn handle_set_build_outcome_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "outcome"] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let outcome = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;

		// Set the outcome.
		self.inner
			.tg
			.set_build_outcome(user.as_ref(), &id, outcome)
			.await?;

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap();

		Ok(response)
	}
}
