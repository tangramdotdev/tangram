use crate::Server;
use bytes::Bytes;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tangram_messenger::Messenger as _;
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn try_start_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::start::Arg,
	) -> tg::Result<Option<bool>> {
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
			tokio::spawn({
				let server = self.clone();
				let id = id.clone();
				async move {
					server
						.messenger
						.publish(format!("builds.{id}.status"), Bytes::new())
						.await
						.inspect_err(|error| tracing::error!(%error, "failed to publish"))
						.ok();
				}
			});
		}

		Ok(Some(output))
	}
}

impl Server {
	pub(crate) async fn handle_start_build_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let Some(output) = handle.try_start_build(&id, arg).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
