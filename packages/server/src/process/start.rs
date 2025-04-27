use crate::Server;
use bytes::Bytes;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::Messenger as _;
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn start_process(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::start::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote.clone()).await?;
			let arg = tg::process::start::Arg { remote: None };
			remote.start_process(id, arg).await?;
			return Ok(());
		}

		// Verify the process is local.
		if !self.get_process_exists_local(id).await? {
			return Err(tg::error!("failed to find the process"));
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
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![now, id];
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
					.publish(format!("processes.{id}.status"), Bytes::new())
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to publish"))
					.ok();
			}
		});

		Ok(())
	}

	pub(crate) async fn handle_start_process_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		handle.start_process(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
