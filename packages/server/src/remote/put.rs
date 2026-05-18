use {
	crate::{Session, context::Authentication},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}

		let authentication = self
			.context
			.authentication
			.as_ref()
			.ok_or_else(|| tg::error!("unauthenticated"))?;
		let user = match authentication {
			Authentication::Root => None,
			Authentication::User(user) => Some(&user.id),
			_ => {
				return Err(tg::error!("unauthorized"));
			},
		};

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
				where name = {p}1 and (
					("user" is null and {p}2 is null) or
					"user" = {p}2
				);
			"#,
		);
		let user = user.map(ToString::to_string);
		let params = db::params![&name, user, &arg.url.to_string()];
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
