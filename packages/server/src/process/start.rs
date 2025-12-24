use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
};

impl Server {
	pub(crate) async fn start_process_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::start::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::process::start::Arg {
				local: None,
				remotes: None,
			};
			client.start_process(id, arg).await.map_err(
				|source| tg::error!(!source, %id, "failed to start the process on the remote"),
			)?;
			return Ok(());
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

		// Start the process.
		let p = connection.p();
		let statement = format!(
			"
				update processes
				set
					heartbeat_at = {p}1,
					started_at = {p}1,
					status = 'started'
				where id = {p}2 and (status = 'created' or status = 'enqueued' or status = 'dequeued');
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![now, id.to_string()];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		if n == 0 {
			return Err(tg::error!(
				"the process does not exist or cannot be started"
			));
		}

		// Drop the database connection.
		drop(connection);

		// Publish the status message.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				server
					.messenger
					.publish(format!("processes.{id}.status"), ())
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to publish"))
					.ok();
			}
		});

		Ok(())
	}

	pub(crate) async fn handle_start_process_request(
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
		self.start_process_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to start the process"))?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
