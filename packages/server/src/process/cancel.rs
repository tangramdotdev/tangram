use {
	crate::{Context, Server},
	bytes::Bytes,
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
};

impl Server {
	pub async fn cancel_process_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<()> {
		// Forward to remote if requested.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self.get_remote_client(remote).await?;
			let arg = tg::process::cancel::Arg {
				local: None,
				remotes: None,
				token: arg.token,
			};
			client.cancel_process(id, arg).await?;
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
		let params = db::params![id.to_string(), arg.token];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Update token count.
		let statement = formatdoc!(
			"
				update processes
				set token_count = token_count - 1
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
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
						tracing::error!(?error, "failed to publish the watchdog message");
					})
					.ok();
			}
		});

		Ok(())
	}

	pub(crate) async fn handle_cancel_process_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Parse the ID.
		let id = id.parse()?;

		// Parse the arg.
		let arg = request
			.query_params()
			.transpose()?
			.ok_or_else(|| tg::error!("query parameters required"))?;

		// Cancel the process.
		self.cancel_process_with_context(context, &id, arg).await?;

		// Create the response.
		let response = http::Response::builder().empty().unwrap();

		Ok(response)
	}
}
