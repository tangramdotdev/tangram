use super::log;
use crate::Server;
use bytes::Bytes;
use futures::{stream::FuturesUnordered, TryStreamExt as _};
use indoc::formatdoc;
use std::pin::pin;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tangram_messenger::Messenger as _;
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn finish_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::finish::Arg,
	) -> tg::Result<()> {
		if self.try_set_build_outcome_local(id, arg.clone()).await? {
			return Ok(());
		}
		if self.try_set_build_outcome_remote(id, arg.clone()).await? {
			return Ok(());
		}
		Err(tg::error!("failed to get the build"))
	}

	async fn try_set_build_outcome_local(
		&self,
		id: &tg::build::Id,
		arg: tg::build::finish::Arg,
	) -> tg::Result<bool> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(false);
		}

		// If the build is finished, then return.
		let status_arg = tg::build::status::Arg {
			timeout: Some(std::time::Duration::ZERO),
		};
		let status = self
			.try_get_build_status_local(id, status_arg)
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

		// Cancel unfinished children.
		children
			.iter()
			.map(|child| async move {
				let arg = tg::build::finish::Arg {
					outcome: tg::build::outcome::Data::Canceled,
				};
				self.finish_build(child, arg).await?;
				Ok::<_, tg::Error>(())
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await
			.ok();

		// If any of the children were canceled, then this build should be canceled.
		let outcomes = children
			.iter()
			.map(|child_id| async move {
				let arg = tg::build::outcome::Arg {
					timeout: Some(std::time::Duration::ZERO),
				};
				self.try_get_build_outcome_future(child_id, arg)
					.await?
					.ok_or_else(|| tg::error!(%child_id, "failed to get the build"))?
					.await?
					.ok_or_else(|| tg::error!(%child_id, "expected the build to be finished"))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		let outcome = if outcomes
			.iter()
			.any(|outcome| outcome.try_unwrap_canceled_ref().is_ok())
		{
			tg::build::outcome::Data::Canceled
		} else {
			arg.outcome
		};

		// Create a blob from the log.
		let reader = log::Reader::new(self, id).await?;
		let log = tg::Blob::with_reader(self, reader).await?;
		let log = log.id(self, None).await?;

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Remove the log file if it exists.
		let path = self.logs_path().join(id.to_string());
		tokio::fs::remove_file(path).await.ok();

		// Remove the log from the database.
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

		// Add the log object to the build objects.
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

		// Update the build.
		let p = connection.p();
		let statement = formatdoc!(
			"
				update builds
				set
					log = {p}1,
					outcome = {p}2,
					status = {p}3,
					finished_at = {p}4
				where id = {p}5;
			"
		);
		let status = tg::build::Status::Finished;
		let finished_at = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![log, outcome, status, finished_at, id];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// Publish a message that the build's status has changed.
		self.messenger
			.publish(format!("builds.{id}.status"), Bytes::new())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish"))?;

		Ok(true)
	}

	async fn try_set_build_outcome_remote(
		&self,
		id: &tg::build::Id,
		arg: tg::build::finish::Arg,
	) -> tg::Result<bool> {
		// Get the remote.
		let Some(remote) = self.remotes.first() else {
			return Ok(false);
		};

		// Push the output.
		if let tg::build::outcome::Data::Succeeded(value) = &arg.outcome {
			tg::Value::try_from(value.clone())?
				.push(self, remote, None)
				.await?;
		}

		// Set the outcome.
		remote.finish_build(id, arg).await?;

		Ok(true)
	}
}

impl Server {
	pub(crate) async fn handle_finish_build_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		handle.finish_build(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
