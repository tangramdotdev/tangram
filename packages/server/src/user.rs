use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{request::Ext as _, response::builder::Ext as _, Body};

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
			.query_optional_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(user)
	}
}

impl Server {
	pub(crate) async fn handle_get_user_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let Some(user) = Self::try_get_user_from_request(handle, &request).await? else {
			let response = http::Response::builder()
				.status(http::StatusCode::UNAUTHORIZED)
				.empty()
				.unwrap();
			return Ok(response);
		};
		let response = http::Response::builder().json(user).unwrap();
		Ok(response)
	}

	async fn try_get_user_from_request<H>(
		handle: &H,
		request: &http::Request<Body>,
	) -> tg::Result<Option<tg::user::User>>
	where
		H: tg::Handle,
	{
		// Get the token.
		let Some(token) = request.token(None) else {
			return Ok(None);
		};

		// Get the user.
		let Some(user) = handle.get_user(token).await? else {
			return Ok(None);
		};

		Ok(Some(user))
	}
}
