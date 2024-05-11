use crate::Server;
use bytes::Bytes;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{
	outgoing::{ResponseBuilderExt as _, ResponseExt as _},
	Incoming, Outgoing,
};
use tangram_messenger::Messenger as _;
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn try_start_build(&self, id: &tg::build::Id) -> tg::Result<Option<bool>> {
		if let Some(output) = self.try_start_build_local(id).await? {
			return Ok(Some(output));
		}
		if let Some(output) = self.try_start_build_remote(id).await? {
			return Ok(Some(output));
		}
		Ok(None)
	}

	async fn try_start_build_local(&self, id: &tg::build::Id) -> tg::Result<Option<bool>> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Start the build.
		let p = connection.p();
		let statement = format!(
			"
				update builds
				set status = 'started', started_at = {p}1, heartbeat_at = {p}1
				where id = {p}2 and (status = 'created' or status = 'dequeued')
				returning id;
			"
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![now, id];
		let output = connection
			.query_optional(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.is_some();

		// Drop the database connection.
		drop(connection);

		// Publish the message.
		if output {
			self.messenger
				.publish(format!("builds.{id}.status"), Bytes::new())
				.await
				.map_err(|source| tg::error!(!source, "failed to publish"))?;
		}

		Ok(Some(output))
	}

	async fn try_start_build_remote(&self, id: &tg::build::Id) -> tg::Result<Option<bool>> {
		let Some(remote) = self.remotes.first() else {
			return Ok(None);
		};
		let Some(output) = remote.try_start_build(id).await? else {
			return Ok(None);
		};
		Ok(Some(output))
	}
}

impl Server {
	pub(crate) async fn handle_start_build_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let Some(output) = handle.try_start_build(&id).await? else {
			return Ok(http::Response::not_found());
		};
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.json(output)
			.unwrap();
		Ok(response)
	}
}
