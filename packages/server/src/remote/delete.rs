use {
	crate::Session,
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn try_delete_remote(&self, name: &str) -> tg::Result<Option<()>> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		self.authorize_remote_management()?;
		let owner = self.remote_owner()?;

		let remote = self.try_get_remote_config(name).await?;
		let connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from remotes
				where name = {p}1
					and (
						(\"user\" is null and {p}2 is null)
						or \"user\" = {p}2
					);
			",
		);
		let params = db::params![&name, owner];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

		if n == 0 {
			return Ok(None);
		}

		if let Some(remote) = remote {
			self.server.remote_clients.remove(&remote.url);
		}

		Ok(Some(()))
	}

	pub(crate) async fn try_delete_remote_request(
		&self,
		request: http::Request<BoxBody>,
		name: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Delete the remote.
		let Some(()) = self
			.try_delete_remote(name)
			.await
			.map_err(|error| tg::error!(!error, %name, "failed to delete the remote"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

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

		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
