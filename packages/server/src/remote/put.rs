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
	pub(crate) async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		self.authorize_remote_management()?;
		let user = self.remote_user()?;
		let user = user.as_ref().map(ToString::to_string);

		let previous_remote = self.try_get_remote_config(name).await?;
		let connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			r#"
				update remotes
				set url = {p}3
				where name = {p}1
					and (
						("user" is null and {p}2 is null)
						or "user" = {p}2
					);
			"#,
		);
		let params = db::params![&name, user.clone(), &arg.url.to_string()];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to insert the remote"))?;
		if n == 0 {
			let statement = formatdoc!(
				r#"
					insert into remotes (name, "user", url)
					values ({p}1, {p}2, {p}3);
				"#,
			);
			let params = db::params![&name, user.clone(), &arg.url.to_string()];
			connection
				.execute(statement.into(), params)
				.await
				.map_err(|error| tg::error!(!error, "failed to insert the remote"))?;
		}
		drop(connection);
		let remote = self
			.try_get_remote_config(name)
			.await?
			.ok_or_else(|| tg::error!("failed to find the remote"))?;
		if let Some(previous_remote) = previous_remote {
			self.server.remote_clients.remove(&previous_remote.url);
		}
		self.server.remote_clients.remove(&remote.url);
		Ok(())
	}

	pub(crate) async fn put_remote_token(&self, name: &str, token: String) -> tg::Result<()> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		self.authorize_remote_management()?;
		let user = self.remote_user()?;
		let user = user.as_ref().map(ToString::to_string);

		let previous_remote = self.try_get_remote_config(name).await?;
		let connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			r#"
				update remotes
				set token = {p}2
				where name = {p}1
					and (
						("user" is null and {p}3 is null)
						or "user" = {p}3
					);
			"#,
		);
		let params = db::params![&name, token, user];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to update the remote"))?;
		drop(connection);
		if n == 0 {
			return Err(tg::error!("failed to find the remote"));
		}
		if let Some(previous_remote) = previous_remote {
			self.server.remote_clients.remove(&previous_remote.url);
		}
		Ok(())
	}

	pub(crate) async fn put_remote_request(
		&self,
		request: http::Request<BoxBody>,
		name: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Put the remote.
		self.put_remote(name, arg)
			.await
			.map_err(|error| tg::error!(!error, %name, "failed to put the remote"))?;

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
