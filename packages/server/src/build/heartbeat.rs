use crate::Server;
use hyper::body::Incoming;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{outgoing::response::Ext as _, Outgoing};
use time::format_description::well_known::Rfc3339;

impl Server {
	/// Send a heartbeat for a build.
	pub async fn heartbeat_build(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<tg::build::heartbeat::Output> {
		if let Some(output) = self.try_heartbeat_build_local(id).await? {
			Ok(output)
		} else if let Some(output) = self.try_heartbeat_build_remote(id).await? {
			Ok(output)
		} else {
			Err(tg::error!("failed to get the build"))
		}
	}

	async fn try_heartbeat_build_local(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<Option<tg::build::heartbeat::Output>> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
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
			.query_optional_value_into(statement, params)
			.await
			.inspect_err(|error| tracing::error!(%error, "failed to perform heartbeat query"))
			.map_err(|source| tg::error!(!source, "failed to perform query"))?;
		let Some(status) = status else {
			return Ok(None);
		};

		// Drop the database connection.
		drop(connection);

		// Create the output.
		let stop = !matches!(status, tg::build::Status::Started);
		let output = tg::build::heartbeat::Output { stop };

		Ok(Some(output))
	}

	async fn try_heartbeat_build_remote(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<Option<tg::build::heartbeat::Output>> {
		let Some(remote) = self.remotes.first() else {
			return Ok(None);
		};
		let Ok(output) = remote.heartbeat_build(id).await else {
			return Ok(None);
		};
		Ok(Some(output))
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
			.json(output)
			.unwrap();
		Ok(response)
	}
}
