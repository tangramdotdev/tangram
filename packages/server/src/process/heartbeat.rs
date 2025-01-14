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
	pub async fn heartbeat_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> tg::Result<tg::process::heartbeat::Output> {
		// If the remote arg is set, then forward the request.
		let remote = arg.remote.as_ref();
		if let Some(remote) = remote {
			let remote = self.get_remote_client(remote.clone()).await?;
			let arg = tg::process::heartbeat::Arg { remote: None };
			let output = remote.heartbeat_process(id, arg).await?;
			return Ok(output);
		}

		// Verify the build is local.
		if !self.get_process_exists_local(id).await? {
			return Err(tg::error!("failed to find the build"));
		}

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Update the heartbeat.
		let p = connection.p();
		let statement = format!(
			"
				update processes
				set heartbeat_at = {p}1
				where id = {p}2
				returning status;
			"
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![now, id];
		let status = connection
			.query_one_value_into::<tg::process::Status>(statement.into(), params)
			.await
			.inspect_err(|error| tracing::error!(%error, "failed to perform heartbeat query"))
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Create the output.
		let output = tg::process::heartbeat::Output { status };

		Ok(output)
	}

	pub async fn process_heartbeat_monitor_task(
		&self,
		options: &crate::config::ProcessHeartbeatMonitor,
	) {
		loop {
			let result = self
				.process_monitor_heartbeat_task_inner(options.timeout, options.limit)
				.await
				.inspect_err(|error| tracing::error!(%error, "failed to cancel processes"));
			if matches!(result, Err(_) | Ok(0)) {
				tokio::time::sleep(options.interval).await;
			}
		}
	}

	pub(crate) async fn process_monitor_heartbeat_task_inner(
		&self,
		timeout: Duration,
		limit: usize,
	) -> tg::Result<u64> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get all started processes whose heartbeat_at exceeds the timeout.
		let p = connection.p();
		let statement = format!(
			"
				select id
				from processes
				where
					(status = 'started' or status = 'finishing') and
					heartbeat_at <= {p}1
				limit {p}2;
			"
		);
		let time = (time::OffsetDateTime::now_utc() - timeout)
			.format(&Rfc3339)
			.unwrap();
		let params = db::params![time, limit];
		let processes = connection
			.query_all_value_into::<tg::process::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Cancel the processes.
		processes
			.iter()
			.map(|process| {
				let server = self.clone();
				async move {
					let arg = tg::process::finish::Arg {
						error: Some(tg::error!("the build's heartbeat expired")),
						output: None,
						remote: None,
						status: tg::process::Status::Canceled,
						token: todo!(),
					};
					todo!()
				}
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		Ok(processes.len().to_u64().unwrap())
	}
}

impl Server {
	pub(crate) async fn handle_heartbeat_process_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?; 
		let output = handle.heartbeat_process(&id, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
