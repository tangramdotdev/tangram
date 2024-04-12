use crate::{
	util::http::{bad_request, full, unauthorized, Incoming, Outgoing},
	Http, Server,
};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use url::Url;

impl Server {
	pub async fn create_login(&self) -> tg::Result<tg::user::Login> {
		if let Some(remote) = self.inner.remotes.first() {
			return remote.create_login().await;
		}

		// Create the ID.
		let id = tg::Id::new_uuidv7(tg::id::Kind::Login);

		// Create the URL.
		let mut url = Url::parse("http://localhost:8476").unwrap();
		url.set_path("/login");
		url.set_query(Some(&format!("id={id}")));

		// Create the login.
		let login = tg::user::Login {
			id: id.clone(),
			url: url.clone(),
			token: None,
		};

		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Add the login to the database.
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into logins (id, url)
				values ({p}1, {p}2);
			"
		);
		let params = db::params![id, url];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(login)
	}

	pub async fn get_login(&self, id: &tg::Id) -> tg::Result<Option<tg::user::Login>> {
		if let Some(remote) = self.inner.remotes.first() {
			return remote.get_login(id).await;
		}

		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the login.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select id, url, token
				from logins
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let login = connection
			.query_optional_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(login)
	}

	pub async fn get_user_for_token(&self, token: &str) -> tg::Result<Option<tg::user::User>> {
		if let Some(remote) = self.inner.remotes.first() {
			return remote.get_user_for_token(token).await;
		}

		// Get a database connection.
		let connection = self
			.inner
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
	pub async fn handle_create_login_request(
		&self,
		_request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Create the login.
		let output = self.inner.tg.create_login().await?;

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().status(200).body(body).unwrap();

		Ok(response)
	}

	pub async fn handle_get_login_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["logins", id] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Get the login.
		let output = self.inner.tg.get_login(&id).await?;

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().status(200).body(body).unwrap();

		Ok(response)
	}

	pub async fn handle_get_user_for_token_request(
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
