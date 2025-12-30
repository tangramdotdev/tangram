use {
	crate::{Context, Server, database},
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub(crate) async fn heartbeat_process_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> tg::Result<tg::process::heartbeat::Output> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::process::heartbeat::Arg {
				local: None,
				remotes: None,
			};
			let output = client.heartbeat_process(id, arg).await?;
			return Ok(output);
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Update the heartbeat.
		let statement = match &connection {
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
			#[cfg(feature = "sqlite")]
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

	pub(crate) async fn handle_heartbeat_process_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;
		let output = self
			.heartbeat_process_with_context(context, &id, arg)
			.await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
