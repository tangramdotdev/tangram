use crate::{
	util::http::{full, unauthorized, Incoming, Outgoing},
	Http, Server,
};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub async fn get_user(&self, token: &str) -> tg::Result<Option<tg::user::User>> {
		if let Some(remote) = self.remotes.first() {
			return remote.get_user(token).await;
		}

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
				join tokens on tokens.user_id = users.id
				where tokens.token = {p}1;
			"
		);
		let params = db::params![token];
		let user = connection
			.query_optional_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(user)
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_get_user_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Get the user from the request.
		let Some(user) = self.try_get_user_from_request(&request).await? else {
			return Ok(unauthorized());
		};

		// Create the body.
		let body = serde_json::to_vec(&user)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(body)
			.unwrap();

		Ok(response)
	}
}
