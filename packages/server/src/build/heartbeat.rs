use crate::Server;
use futures::{stream::FuturesUnordered, StreamExt as _};
use hyper::body::Incoming;
use num::ToPrimitive;
use std::time::Duration;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Outgoing};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn heartbeat_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::heartbeat::Arg,
	) -> tg::Result<tg::build::heartbeat::Output> {
		// If the remote arg is set, then forward the request.
		let remote = arg.remote.as_ref();
		if let Some(remote) = remote {
			let remote = self
				.remotes
				.get(remote)
				.ok_or_else(|| tg::error!("the remote does not exist"))?
				.clone();
			let arg = tg::build::heartbeat::Arg { remote: None };
			let output = remote.heartbeat_build(id, arg).await?;
			return Ok(output);
		}

		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Err(tg::error!("failed to find the build"));
		}

		// Get a database connection.
		let connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Update the heartbeat.
		let p = connection.p();
		let statement = format!(
			"
				update builds
				set heartbeat_at = {p}1
				where id = {p}2
				returning status;
			"
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![now, id];
		let status = connection
			.query_one_value_into(statement, params)
			.await
			.inspect_err(|error| tracing::error!(%error, "failed to perform heartbeat query"))
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Create the output.
		let stop = !matches!(status, tg::build::Status::Started);
		let output = tg::build::heartbeat::Output { stop };

		Ok(output)
	}

	pub async fn build_heartbeat_monitor_task(
		&self,
		options: &crate::config::BuildHeartbeatMonitor,
	) {
		loop {
			let result = self
				.build_monitor_heartbeat_task_inner(options.timeout, options.limit)
				.await
				.inspect_err(|error| tracing::error!(%error, "failed to cancel builds"));
			if matches!(result, Err(_) | Ok(0)) {
				tokio::time::sleep(options.interval).await;
			}
		}
	}

	pub(crate) async fn build_monitor_heartbeat_task_inner(
		&self,
		timeout: Duration,
		limit: usize,
	) -> tg::Result<u64> {
		// Get a database connection.
		let connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get all started builds whose heartbeat_at exceeds the timeout.
		let p = connection.p();
		let statement = format!(
			"
				select id
				from builds
				where
					status = 'started' and
					heartbeat_at <= {p}1
				limit {p}2;
			"
		);
		let time = (time::OffsetDateTime::now_utc() - timeout)
			.format(&Rfc3339)
			.unwrap();
		let params = db::params![time, limit];
		let builds = connection
			.query_all_value_into::<tg::build::Id>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);

		// Cancel the builds.
		builds
			.iter()
			.map(|id| async move {
				let build = tg::Build::with_id(id.clone());
				let arg = tg::build::finish::Arg {
					outcome: tg::build::outcome::Data::Canceled,
					remote: None,
				};
				build
					.finish(self, arg)
					.await
					.inspect_err(|error| tracing::error!(%error, %id, "failed to cancel the build"))
					.ok()
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		Ok(builds.len().to_u64().unwrap())
	}
}

impl Server {
	pub(crate) async fn handle_heartbeat_build_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let output = handle.heartbeat_build(&id, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
