use {
	crate::{Server, database},
	indoc::indoc,
	tangram_client as tg,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn heartbeat_process(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::heartbeat::Arg,
	) -> tg::Result<tg::process::heartbeat::Output> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote.clone()).await?;
			let arg = tg::process::heartbeat::Arg { remote: None };
			let output = remote.heartbeat_process(id, arg).await?;
			return Ok(output);
		}

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Update the heartbeat.
		let statement = match &connection {
			database::Connection::Sqlite(_) => indoc!(
				"
					update processes
					set heartbeat_at = case
						when status = 'started' then ?1
						else null
					end
					where id = ?2
					returning status;
				"
			),
			#[cfg(feature = "postgres")]
			database::Connection::Postgres(_) => indoc!(
				"
					with params as (select $1::int8 as now, $2::text as process)
					update processes
					set heartbeat_at = case
						when status = 'started' then params.now
						else null
					end
					from params
					where id = params.process
					returning status;
				"
			),
		};
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![now, id.to_string()];
		let Some(status) = connection
			.query_optional_value_into::<db::value::Serde<tg::process::Status>>(
				statement.into(),
				params,
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|status| status.0)
		else {
			return Err(tg::error!("failed to find the process"));
		};

		// Drop the database connection.
		drop(connection);

		// Create the output.
		let output = tg::process::heartbeat::Output { status };

		Ok(output)
	}

	pub(crate) async fn handle_heartbeat_process_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let output = handle.heartbeat_process(&id, arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
