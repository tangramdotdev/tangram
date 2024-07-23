use crate::Server;
use bytes::Bytes;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tangram_messenger::Messenger as _;
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn start_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::start::Arg,
	) -> tg::Result<()> {
		// Attempt to get the remote for the build.
		let remote = if arg.remote.is_none() {
			self.try_get_remote_for_build(id).await?
		} else {
			arg.remote.clone()
		};
		// If a remote was set, try and start it.
		if let Some(remote) = remote {
			let client = self
				.remotes
				.get(&remote)
				.ok_or_else(|| tg::error!(%remote, "the remote does not exist"))?
				.clone();
			let arg = tg::build::start::Arg { remote: None };
			return client.start_build(id, arg).await;
		}

		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Err(tg::error!("failed to find the build"));
		}

		// Get a database connection.
		let connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Start the build.
		let p = connection.p();
		let statement = format!(
			"
				update builds
				set status = 'started', started_at = {p}1, heartbeat_at = {p}1
				where id = {p}2 and (status = 'created' or status = 'dequeued')
				returning 1;
			"
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![now, id];
		let output = connection
			.query_optional(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// If the build was not started, then return an error.
		if output.is_none() {
			return Err(tg::error!("failed to start the build"));
		}

		// Publish the message.
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

		Ok(())
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
		handle.start_build(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
