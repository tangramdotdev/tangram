use super::log;
use crate::{
	database::{Database, PostgresJson, SqliteJson},
	postgres_params, sqlite_params, Http, Server,
};
use async_recursion::async_recursion;
use futures::{future, stream::FuturesUnordered, TryFutureExt, TryStreamExt};
use http_body_util::BodyExt;
use itertools::Itertools;
use tangram_client as tg;
use tangram_error::{error, Error, Result, Wrap, WrapErr};
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
		let outcome = {
			match &self.inner.database {
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
			}
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

	#[allow(clippy::too_many_lines)]
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

	#[allow(clippy::too_many_lines)]
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

		// If the build was canceled, then cancel the children.
		if matches!(outcome, tg::build::Outcome::Canceled) {
			children
				.iter()
				.map(|child| async move {
					self.set_build_outcome(user, child, tg::build::Outcome::Canceled)
						.await?;
					Ok::<_, Error>(())
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect()
				.await?;
		}

		// If any of the children were canceled, then this build should be canceled.
		let outcome = if children
			.iter()
			.map(|child_id| async move {
				self.try_get_build_outcome(child_id, tg::build::outcome::GetArg::default(), None)
					.await?
					.wrap_err("Failed to get the build.")?
					.wrap_err("Failed to get the outcome.")
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?
			.into_iter()
			.any(|outcome| outcome.try_unwrap_canceled_ref().is_ok())
		{
			tg::build::Outcome::Canceled
		} else {
			outcome
		};

		// Create a blob from the log.
		let log = tg::Blob::with_reader(self, log::Reader::new(self, id).await?).await?;
		let log = log.id(self).await?;

		// Update the state.
		match &self.inner.database {
			Database::Sqlite(database) => {
				let connection = database.get().await?;
				let status = tg::build::Status::Finished;
				let outcome = outcome.data(self).await?;
				let statement = "
					update builds
					set
						children = ?1,
						log = ?2,
						outcome = ?3,
						status = ?4
					where id = ?5;
				";
				let params = sqlite_params![
					SqliteJson(children),
					log.to_string(),
					SqliteJson(outcome),
					status.to_string(),
					id.to_string()
				];
				let mut statement = connection
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				statement
					.execute(params)
					.wrap_err("Failed to execute the statement.")?;
			},

			Database::Postgres(database) => {
				let connection = database.get().await?;
				let status = tg::build::Status::Finished;
				let outcome = outcome.data(self).await?;
				let statement = "
					update builds
					set
						children = $1,
						log = $2,
						outcome = $3,
						status = $4
					where id = $5;
				";
				let params = postgres_params![
					PostgresJson(children),
					log.to_string(),
					PostgresJson(outcome),
					status.to_string(),
					id.to_string()
				];
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

		// Send the event.
		if let Some(status) = self
			.inner
			.build_state
			.read()
			.unwrap()
			.get(id)
			.unwrap()
			.status
			.as_ref()
		{
			status.send_replace(());
		}

		// Remove the build context.
		self.inner.build_state.write().unwrap().remove(id);

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

		// Remove the build context.
		self.inner.build_state.write().unwrap().remove(id);

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