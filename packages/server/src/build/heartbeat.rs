use std::sync::Arc;

use futures::{future, stream, StreamExt};
use hyper::body::Incoming;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::Outgoing;
use time::format_description::well_known::Rfc3339;
use tokio_stream::wrappers::IntervalStream;
use crate::Server;

impl Server {
	/// Try to start a task that sends build heartbeats.
	pub async fn try_start_heartbeat_for_build(&self, build: &tg::Build) -> tg::Result<()> {
		// Get the heartbeat interval.
		let interval = self.options.build.as_ref().unwrap().heartbeat_interval;

		// Subscribe to the build's status changes.
		let status = build
			.status(self, tg::build::status::Arg::default())
			.await
			.map_err(
				|source| tg::error!(!source, %build = build.id(), "failed to create build status stream"),
			)?
			.take_while(|status| {
				// Take while the status is started or dequeued.
				let status = status.clone();
				async move {
					matches!(
						status,
						Ok(tg::build::Status::Started | tg::build::Status::Dequeued)
					)
				}
			})
			.map(|_| ());

		// Create the interval for the heartbeat.
		let interval = IntervalStream::new(tokio::time::interval(interval)).map(|_| ());

		// Create the events stream.
		let mut events = stream::once(future::ready(()))
			.chain(stream::select(status, interval))
			.boxed();

		// Spawn a task to give a build heartbeat.
		tokio::spawn({
			let build = build.clone();
			let server = Arc::downgrade(&self.0);
			async move {
				// Every tick or change on build status, send a heartbeat.
				while let Some(()) = events.next().await {
					// Try to get a strong reference to the server.
					let Some(server) = server.upgrade() else {
						break;
					};
					let server = Server(server);

					// Send a heartbeat.
					match server.heartbeat_build(build.id()).await {
						// If the server replies that the build should be stopped, stop the build and stop sending heartbeats.
						Ok(tg::build::heartbeat::Output { stop }) if stop => {
							build
								.cancel(&server)
								.await
								.inspect_err(
									|error| tracing::error!(%error, "failed to stop build after failed heartbeat"),
								)
								.ok();
							break;
						},
						Err(e) => {
							let id = build.id();
							tracing::error!(%e, %id, "failed to send build heartbeat");
							break;
						},
						_ => (),
					}
				}
			}
		});

		Ok(())
	}

	/// Send a heartbeat for a build.
	pub async fn heartbeat_build(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<tg::build::heartbeat::Output> {
		if let Some(remote) = self.remotes.first() {
			remote.heartbeat_build(id).await
		} else {
			self.try_heartbeat_build_local(id).await
		}
	}

	async fn try_heartbeat_build_local(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<tg::build::heartbeat::Output> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Update the heartbeat_at time.
		#[derive(serde::Deserialize)]
		struct Row {
			status: tg::build::Status,
		}
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
		let Row { status } = connection
			.query_one_into(statement, params)
			.await
			.inspect_err(|error| tracing::error!(%error, "failed to perform heartbeat query"))
			.map_err(|source| tg::error!(!source, "failed to perform query"))?;

		let stop = !matches!(status, tg::build::Status::Started);

		// Drop the database connection.
		drop(connection);

		Ok(tg::build::heartbeat::Output { stop })
	}
}

impl Server {
	pub(crate) async fn handle_heartbeat_build_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let output = handle.heartbeat_build(&id).await?;
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(Outgoing::json(output))
			.unwrap();
		Ok(response)
	}
}
