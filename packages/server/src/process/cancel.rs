use crate::Server;
use bytes::Bytes;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::Messenger as _;

impl Server {
	pub async fn cancel_process(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::cancel::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			remote.cancel_process(id, arg).await?;
			return Ok(());
		}

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		// Delete the process token.
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from process_tokens
				where process = {p}1 and token = {p}2;
			"
		);
		let params = db::params![id.to_bytes(), arg.token];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Publish the watchdog message.
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish("watchdog".into(), Bytes::new())
					.await
					.inspect_err(|error| {
						tracing::error!(?error, "failed to publish cancellation message");
					})
					.ok();
			}
		});

		Ok(())
	}

	pub(crate) async fn handle_cancel_process_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Parse the arg.
		let arg = request
			.query_params()
			.transpose()?
			.ok_or_else(|| tg::error!("query parameters required"))?;

		// Cancel the process.
		handle.cancel_process(&id, arg).await?;

		// Create the response.
		let response = http::Response::builder().empty().unwrap();

		Ok(response)
	}
}
