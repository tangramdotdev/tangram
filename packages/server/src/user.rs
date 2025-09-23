use {
	crate::Server,
	indoc::formatdoc,
	tangram_client as tg,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn get_user(&self, token: &str) -> tg::Result<Option<tg::user::User>> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the user for the token.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select users.id, users.email, tokens.token
				from users
				join tokens on tokens.user = users.id
				where tokens.token = {p}1;
			"
		);
		let params = db::params![token];
		let user = connection
			.query_optional_into::<db::row::Serde<_>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|user| user.0);

		// Drop the database connection.
		drop(connection);

		Ok(user)
	}

	pub(crate) async fn handle_get_user_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Get the token.
		let Some(token) = request.token(None) else {
			let response = http::Response::builder()
				.status(http::StatusCode::UNAUTHORIZED)
				.empty()
				.unwrap();
			return Ok(response);
		};

		// Get the user.
		let Some(output) = handle.get_user(token).await? else {
			let response = http::Response::builder()
				.status(http::StatusCode::UNAUTHORIZED)
				.empty()
				.unwrap();
			return Ok(response);
		};

		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();

		Ok(response)
	}
}
