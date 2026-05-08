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

		let connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into remotes (name, url)
				values ({p}1, {p}2)
				on conflict (name)
				do update set url = {p}2;
			",
		);
		let params = db::params![&name, &arg.url.to_string()];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to insert the remote"))?;
		drop(connection);
		let remote = self
			.try_get_remote_config(name)
			.await?
			.ok_or_else(|| tg::error!("failed to find the remote"))?;
		let client =
			self.server
				.create_remote_client(&remote.name, remote.url.clone(), remote.token)?;
		self.server.remotes.insert(name.to_owned(), client);
		Ok(())
	}

	pub(crate) async fn put_remote_token(&self, name: &str, token: String) -> tg::Result<()> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				update remotes
				set token = {p}2
				where name = {p}1;
			",
		);
		let params = db::params![&name, token];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to update the remote"))?;
		drop(connection);
		let remote = self
			.try_get_remote_config(name)
			.await?
			.ok_or_else(|| tg::error!("failed to find the remote"))?;
		let client =
			self.server
				.create_remote_client(&remote.name, remote.url.clone(), remote.token)?;
		self.server.remotes.insert(name.to_owned(), client);
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
