use crate::{Http, Server};
use tangram_client as tg;
use tangram_error::{error, Result, WrapErr};
use tangram_util::http::{bad_request, full, get_token, unauthorized, Incoming, Outgoing};

impl Server {
	pub async fn create_login(&self) -> Result<tg::user::Login> {
		self.inner
			.remote
			.as_ref()
			.wrap_err("The server does not have a remote.")?
			.create_login()
			.await
	}

	pub async fn get_login(&self, id: &tg::Id) -> Result<Option<tg::user::Login>> {
		self.inner
			.remote
			.as_ref()
			.wrap_err("The server does not have a remote.")?
			.get_login(id)
			.await
	}

	pub async fn get_user_for_token(&self, token: &str) -> Result<Option<tg::user::User>> {
		self.inner
			.remote
			.as_ref()
			.wrap_err("The server does not have a remote.")?
			.get_user_for_token(token)
			.await
	}
}

impl Http {
	pub async fn handle_create_login_request(
		&self,
		_request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Create the login.
		let login = self.inner.tg.create_login().await?;

		// Create the response.
		let body = serde_json::to_string(&login).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder()
			.status(200)
			.body(full(body))
			.unwrap();

		Ok(response)
	}

	pub async fn handle_get_login_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["logins", id] = path_components.as_slice() else {
			return Err(error!("Unexpected path."));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Get the login.
		let login = self.inner.tg.get_login(&id).await?;

		// Create the response.
		let response =
			serde_json::to_string(&login).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder()
			.status(200)
			.body(full(response))
			.unwrap();

		Ok(response)
	}

	pub async fn handle_get_user_for_token_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the token from the request.
		let Some(token) = get_token(&request, None) else {
			return Ok(unauthorized());
		};

		// Authenticate the user.
		let Some(user) = self.inner.tg.get_user_for_token(token.as_str()).await? else {
			return Ok(unauthorized());
		};

		// Create the response.
		let body = serde_json::to_string(&user).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
			.unwrap();

		Ok(response)
	}
}
