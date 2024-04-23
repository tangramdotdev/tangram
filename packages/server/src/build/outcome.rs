use super::log;
use crate::{
	util::http::{empty, full, not_found, Incoming, Outgoing},
	Http, Server,
};
use futures::{future, stream::FuturesUnordered, TryFutureExt as _, TryStreamExt as _};
use http_body_util::BodyExt as _;
use indoc::formatdoc;
use std::pin::pin;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<Option<Option<tg::build::Outcome>>> {
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
	) -> tg::Result<Option<Option<tg::build::Outcome>>> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}

		// Wait for the build to finish.
		let arg = tg::build::status::GetArg {
			timeout: arg.timeout,
		};
		let status = self
			.try_get_build_status_local(id, arg, stop)
			.await?
			.ok_or_else(|| tg::error!("expected the build to exist"))?;
		let finished = status.try_filter_map(|status| {
			future::ready(Ok(if status == tg::build::Status::Finished {
				Some(())
			} else {
				None
			}))
		});
		let finished = pin!(finished)
			.try_next()
			.map_ok(|option| option.is_some())
			.await?;
		if !finished {
			return Ok(Some(None));
		}

		// Get the outcome.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select outcome
				from builds
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let outcome = connection
			.query_optional_value_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		Ok(Some(outcome))
	}

	async fn try_get_build_outcome_remote(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<Option<Option<tg::build::Outcome>>> {
		let Some(remote) = self.remotes.first() else {
			return Ok(None);
		};
		let Some(outcome) = remote.try_get_build_outcome(id, arg, stop).await? else {
			return Ok(None);
		};
		Ok(Some(outcome))
	}

	pub async fn set_build_outcome(
		&self,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> tg::Result<()> {
		if self
			.try_set_build_outcome_local(id, outcome.clone())
			.await?
		{
			return Ok(());
		}
		if self
			.try_set_build_outcome_remote(id, outcome.clone())
			.await?
		{
			return Ok(());
		}
		Err(tg::error!("failed to get the build"))
	}

	async fn try_set_build_outcome_local(
		&self,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> tg::Result<bool> {
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
			.ok_or_else(|| tg::error!("expected the build to exist"))?;
		let status = pin!(status)
			.try_next()
			.await?
			.ok_or_else(|| tg::error!("failed to get the status"))?;
		if status == tg::build::Status::Finished {
			return Ok(true);
		}

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the children.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select child
				from build_children
				where build = {p}1
				order by position;
			"
		);
		let params = db::params![id];
		let children = connection
			.query_all_value_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// If the build was canceled, then stop the build and cancel the children.
		children
			.iter()
			.map(|child| async move {
				self.set_build_outcome(child, tg::build::Outcome::Canceled)
					.await
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await
			.ok();

		// If any of the children were canceled, then this build should be canceled.
		let outcomes = children
			.iter()
			.map(|child_id| async move {
				let arg = tg::build::outcome::GetArg {
					timeout: Some(std::time::Duration::ZERO),
				};
				self.try_get_build_outcome(child_id, arg, None)
					.await?
					.ok_or_else(|| tg::error!(%child_id, "failed to get the build"))?
					.ok_or_else(|| tg::error!(%child_id, "expected the build to be finished"))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		let outcome = if outcomes
			.iter()
			.any(|outcome| outcome.try_unwrap_canceled_ref().is_ok())
		{
			tg::build::Outcome::Canceled
		} else {
			outcome
		};

		// Get the outcome data.
		let outcome = outcome.data(self).await?;

		// Create a blob from the log.
		let log = tg::Blob::with_reader(self, log::Reader::new(self, id).await?, None).await?;
		let log = log.id(self, None).await?;

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Remove the build log.
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from build_logs
				where build = {p}1;
			"
		);
		let params = db::params![id];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Add the log to the build objects.
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into build_objects (build, object)
				values ({p}1, {p}2)
				on conflict (build, object) do nothing;
			"
		);
		let params = db::params![id, log];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Add the outcome's children to the build objects.
		let objects = outcome
			.try_unwrap_succeeded_ref()
			.map(tg::value::Data::children)
			.into_iter()
			.flatten();
		for object in objects {
			let p = connection.p();
			let statement = formatdoc!(
				"
					insert into build_objects (build, object)
					values ({p}1, {p}2)
					on conflict (build, object) do nothing;
				"
			);
			let params = db::params![id, object];
			connection
				.execute(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Compute the count.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select 1 + (
					select count(*)
					from build_children
					where build = {p}1
				) as count;
			"
		);
		let params = db::params![id];
		let count = connection
			.query_one_value_into::<Option<u64>>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Compute the weight.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select (
					select sum(weight)
					from objects
					where id in (
						select object
						from build_objects
						where build = {p}1
					)
				) + (
					select sum(weight)
					from builds
					where id in (
						select child
						from build_children
						where build = {p}1
					)
				) as weight;
			"
		);
		let params = db::params![id];
		let weight = connection
			.query_one_value_into::<Option<u64>>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Update the build.
		let p = connection.p();
		let statement = formatdoc!(
			"
				update builds
				set
					count = {p}1,
					log = {p}2,
					outcome = {p}3,
					status = {p}4,
					weight = {p}5,
					finished_at = {p}6
				where id = {p}7;
			"
		);
		let status = tg::build::Status::Finished;
		let finished_at = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![count, log, outcome, status, weight, finished_at, id];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// Publish the message.
		self.messenger.publish_to_build_status(id).await?;

		Ok(true)
	}

	async fn try_set_build_outcome_remote(
		&self,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> tg::Result<bool> {
		// Get the remote.
		let Some(remote) = self.remotes.first() else {
			return Ok(false);
		};

		// Push the output.
		if let tg::build::Outcome::Succeeded(value) = &outcome {
			value.push(self, remote, None).await?;
		}

		// Set the outcome.
		remote.set_build_outcome(id, outcome).await?;

		Ok(true)
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_get_build_outcome_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "outcome"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the ID"))?;

		// Get the search params.
		let arg = request
			.uri()
			.query()
			.map(serde_urlencoded::from_str)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize the search params"))?
			.unwrap_or_default();

		let stop = request.extensions().get().cloned().unwrap();
		let Some(outcome) = self
			.handle
			.try_get_build_outcome(&id, arg, Some(stop))
			.await?
		else {
			return Ok(not_found());
		};

		// Create the body.
		let outcome = if let Some(outcome) = outcome {
			Some(outcome.data(&self.handle).await?)
		} else {
			None
		};
		let body = serde_json::to_vec(&outcome)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(body)
			.unwrap();

		Ok(response)
	}

	pub async fn handle_set_build_outcome_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "outcome"] = path_components.as_slice() else {
			return Err(tg::error!("unexpected path"));
		};
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the ID"))?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the body"))?
			.to_bytes();
		let outcome = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;

		// Set the outcome.
		self.handle.set_build_outcome(&id, outcome).await?;

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap();

		Ok(response)
	}
}
