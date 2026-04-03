use {
	crate::{Context, Server},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
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
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::process::cancel::Arg {
				local: None,
				remotes: None,
				token: arg.token,
			};
			client.cancel_process(id, arg).await.map_err(
				|source| tg::error!(!source, %id, "failed to cancel the process on the remote"),
			)?;
			return Ok(());
		}

		// Get a database connection.
		let mut connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Delete the process token.
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from process_tokens
				where process = {p}1 and token = {p}2;
			"
		);
		let params = db::params![id.to_string(), arg.token];
		let deleted = transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// If no token was removed, then validate whether the token was stale or invalid.
		if deleted == 0 {
			let statement = formatdoc!(
				"
					select status
					from processes
					where id = {p}1;
				"
			);
			let params = db::params![id.to_string()];
			let status = transaction
				.query_optional_value_into::<db::value::Serde<tg::process::Status>>(
					statement.into(),
					params,
				)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
				.map(|status| status.0);
			if status.is_none() {
				transaction
					.rollback()
					.await
					.map_err(|source| tg::error!(!source, "failed to roll back the transaction"))?;
				return Err(tg::error!("failed to find the process"));
			}
			if status.is_some_and(|status| !status.is_finished()) {
				transaction
					.rollback()
					.await
					.map_err(|source| tg::error!(!source, "failed to roll back the transaction"))?;
				return Err(tg::error!("the process token was not found"));
			}
		}

		// Update the token count.
		let statement = formatdoc!(
			"
				update processes
				set token_count = (
					select count(*)
					from process_tokens
					where process = {p}1
				)
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		drop(connection);

		// Publish the watchdog message if a token was removed.
		if deleted > 0 {
			self.spawn_publish_watchdog_message_task();
		}

		Ok(())
	}

	pub(crate) async fn handle_cancel_process_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the ID.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Parse the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("query parameters required"))?;

		// Cancel the process.
		self.cancel_process_with_context(context, &id, arg).await?;

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().body(BoxBody::empty()).unwrap();

		Ok(response)
	}
}
