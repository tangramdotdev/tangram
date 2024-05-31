use crate::Server;
use hyper::body::Incoming;
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
		let remote = arg.remote.as_ref();
		match remote {
			None => {
				let output = self.heartbeat_build_local(id, arg).await?;
				Ok(output)
			},
			Some(remote) => {
				let remote = self
					.remotes
					.get(remote)
					.ok_or_else(|| tg::error!("the remote does not exist"))?
					.clone();
				let output = remote.heartbeat_build(id, arg).await?;
				Ok(output)
			},
		}
	}

	pub async fn heartbeat_build_local(
		&self,
		id: &tg::build::Id,
		_arg: tg::build::heartbeat::Arg,
	) -> tg::Result<tg::build::heartbeat::Output> {
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
			return Err(tg::error!("failed to find the build"));
		};

		// Drop the database connection.
		drop(connection);

		// Create the output.
		let stop = !matches!(status, tg::build::Status::Started);
		let output = tg::build::heartbeat::Output { stop };

		Ok(output)
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
