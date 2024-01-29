use crate::{database::Json, params, Server};
use async_recursion::async_recursion;
use futures::{
	future,
	stream::{self, FuturesUnordered},
	FutureExt, StreamExt, TryStreamExt,
};
use http_body_util::BodyExt;
use tangram_client as tg;
use tangram_error::{error, Error, Result, WrapErr};
use tangram_util::http::{empty, full, not_found, Incoming, Outgoing};
use tokio_stream::wrappers::WatchStream;

impl Server {
	pub async fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<Option<tg::build::Outcome>>> {
		if let Some(outcome) = self
			.try_get_build_outcome_local(id, arg.clone(), stop)
			.await?
		{
			Ok(Some(outcome))
		} else if let Some(outcome) = self.try_get_build_outcome_remote(id, arg.clone()).await? {
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

		// Create the event stream.
		let context = self.inner.build_context.read().unwrap().get(id).cloned();
		let status = context
			.as_ref()
			.map_or_else(
				|| stream::empty().boxed(),
				|context| {
					WatchStream::from_changes(context.status.as_ref().unwrap().subscribe()).boxed()
				},
			)
			.chain(stream::pending());
		let timeout = arg.timeout.map_or_else(
			|| future::pending().left_future(),
			|timeout| tokio::time::sleep(timeout).right_future(),
		);
		let stop = stop.map_or_else(
			|| future::pending().left_future(),
			|mut stop| async move { stop.wait_for(|stop| *stop).map(|_| ()).await }.right_future(),
		);
		let events = stream::once(future::ready(()))
			.chain(status.take_until(timeout).take_until(stop))
			.boxed();

		// Create the stream.
		let server = self.clone();
		let id = id.clone();
		let mut stream = stream::try_unfold(
			(server, id, events),
			move |(server, id, mut stream)| async move {
				let Some(()) = stream.next().await else {
					return Ok(None);
				};
				let outcome = server.try_get_build_outcome_local_inner(&id).await?;
				Ok::<_, Error>(Some((outcome, (server, id, stream))))
			},
		)
		.try_filter_map(|outcome| future::ready(Ok(outcome)))
		.boxed();

		// Get the outcome.
		let outcome = stream.try_next().await?;

		Ok(Some(outcome))
	}

	async fn try_get_build_outcome_local_inner(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::Outcome>> {
		let db = self.inner.database.get().await?;
		let statement = "
			select state->'outcome' as outcome
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
			.wrap_err("Failed to get the row.")?
			.wrap_err("Expected a row.")?;
		let outcome = row
			.get::<_, Json<Option<tg::build::Outcome>>>(0)
			.wrap_err("Failed to deserialize the column.")?
			.0;
		Ok(outcome)
	}

	async fn try_get_build_outcome_remote(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
	) -> Result<Option<Option<tg::build::Outcome>>> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(None);
		};
		let Some(outcome) = remote.try_get_build_outcome(id, arg).await? else {
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
		let children = {
			let db = self.inner.database.get().await?;
			let statement = "
				select json_extract(state, '$.children')
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
			let Some(row) = rows.next().wrap_err("Failed to get the row.")? else {
				return Ok(false);
			};
			row.get::<_, Json<Vec<tg::build::Id>>>(0)
				.wrap_err("Failed to deserialize the column.")?
				.0
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

		// Update the state.
		{
			let status = tg::build::Status::Finished;
			let outcome = outcome.data(self).await?;
			let db = self.inner.database.get().await?;
			let statement = "
				update builds
				set state = json_set(
					state,
					'$.status', ?1,
					'$.outcome', json(?2)
				)
				where id = ?3;
			";
			let params = params![status.to_string(), Json(outcome), id.to_string()];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		// Notify subscribers that the status has been updated.
		if let Some(status) = self
			.inner
			.build_context
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
		self.inner.build_context.write().unwrap().remove(id);

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
		self.inner.build_context.write().unwrap().remove(id);

		Ok(true)
	}
}

impl Server {
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

		// Attempt to get the outcome.
		let stop = request.extensions().get().cloned();
		let Some(outcome) = self.try_get_build_outcome(&id, arg, stop).await? else {
			return Ok(not_found());
		};

		// Create the body.
		let outcome = if let Some(outcome) = outcome {
			Some(outcome.data(self).await?)
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
		self.set_build_outcome(user.as_ref(), &id, outcome).await?;

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap();

		Ok(response)
	}
}
