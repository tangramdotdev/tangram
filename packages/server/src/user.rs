use crate::{
	util::http::{bad_request, full, unauthorized, Incoming, Outgoing},
	Http, Server,
};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_error::{error, Result};
use url::Url;

impl Server {
	pub async fn create_login(&self) -> Result<tg::user::Login> {
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
			.map_err(|source| error!(!source, "failed to get a database connection"))?;

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
			.map_err(|source| error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(login)
	}

	pub async fn get_login(&self, id: &tg::Id) -> Result<Option<tg::user::Login>> {
		if let Some(remote) = self.inner.remotes.first() {
			return remote.get_login(id).await;
		}

		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| error!(!source, "failed to get a database connection"))?;

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
			.map_err(|source| error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(login)
	}

	pub async fn get_user_for_token(&self, token: &str) -> Result<Option<tg::user::User>> {
		if let Some(remote) = self.inner.remotes.first() {
			return remote.get_user_for_token(token).await;
		}

		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| error!(!source, "failed to get a database connection"))?;

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
			.map_err(|source| error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(user)
	}

	// pub async fn create_oauth_url(&self, login_id: &tg::Id) -> Result<Url> {
	// 	// Get the GitHub OAuth client.
	// 	let oauth_client = self
	// 		.inner
	// 		.oauth
	// 		.github
	// 		.as_ref()
	// 		.ok_or_else(|| error!("the GitHub OAuth client is not configured"))?;

	// 	// Create the authorize URL.
	// 	let mut redirect_url = oauth_client.redirect_url().unwrap().url().clone();
	// 	redirect_url.set_query(Some(&format!("id={login_id}")));
	// 	let redirect_url = oauth2::RedirectUrl::new(redirect_url.to_string())
	// 		.map_err(|source| error!(!source, "failed to create the redirect url"))?;
	// 	let (oauth_authorize_url, _csrf_token) = oauth_client
	// 		.authorize_url(oauth2::CsrfToken::new_random)
	// 		.set_redirect_uri(Cow::Owned(redirect_url))
	// 		.add_scope(oauth2::Scope::new("user:email".to_owned()))
	// 		.url();

	// 	Ok(oauth_authorize_url)
	// }

	// pub async fn login_with_github_code(
	// 	&self,
	// 	login_id: tg::Id,
	// 	github_code: String,
	// ) -> Result<tg::user::User> {
	// 	// Get the GitHub OAuth client.
	// 	let github_oauth_client = self
	// 		.inner
	// 		.oauth
	// 		.github
	// 		.as_ref()
	// 		.ok_or_else(|| error!("the GitHub OAuth client is not configured"))?;

	// 	// Get a GitHub token.
	// 	let github_token = github_oauth_client
	// 		.exchange_code(oauth2::AuthorizationCode::new(github_code))
	// 		.request_async(oauth2::reqwest::async_http_client)
	// 		.await
	// 		.map_err(|source| error!(!source, "failed to exchange the code"))?;

	// 	// Create the GitHub client.
	// 	let octocrab = octocrab::OctocrabBuilder::new()
	// 		.oauth(octocrab::auth::OAuth {
	// 			access_token: github_token.access_token().secret().clone().into(),
	// 			token_type: match github_token.token_type() {
	// 				oauth2::basic::BasicTokenType::Bearer => "Bearer".to_owned(),
	// 				token_type => return Err(error!(?token_type, "unsupported token type")),
	// 			},
	// 			scope: github_token.scopes().map_or_else(Vec::new, |scopes| {
	// 				scopes.iter().map(|scope| scope.to_string()).collect()
	// 			}),
	// 			expires_in: None,
	// 			refresh_token: None,
	// 			refresh_token_expires_in: None,
	// 		})
	// 		.build()
	// 		.map_err(|source| error!(!source, "failed to create the GitHub client"))?;

	// 	// Get the GitHub user's primary email.
	// 	#[derive(serde::Deserialize)]
	// 	struct Entry {
	// 		email: String,
	// 		primary: bool,
	// 		verified: bool,
	// 	}
	// 	let emails: Vec<Entry> = octocrab
	// 		.get("/user/emails", None::<&()>)
	// 		.await
	// 		.map_err(|source| error!(!source, "failed to perform the request"))?;
	// 	let email = emails
	// 		.into_iter()
	// 		.find(|email| email.verified && email.primary)
	// 		.map(|email| email.email)
	// 		.ok_or_else(|| {
	// 			error!("the GitHub user does not have a verified primary email address")
	// 		})?;

	// 	// Log the user in.
	// 	let (user, token) = self.login(&email).await?;

	// 	// Update the login with the user.
	// 	let connection = self
	// 		.inner
	// 		.database
	// 		.connection()
	// 		.await
	// 		.map_err(|source| error!(!source, "failed to get a database connection"))?;
	// 	let p = connection.p();
	// 	let statement = formatdoc!(
	// 		"
	// 			update logins
	// 			set token = {p}1
	// 			where id = {p}2;
	// 		"
	// 	);
	// 	let params = db::params![token, login_id];
	// 	connection
	// 		.execute(statement, params)
	// 		.await
	// 		.map_err(|source| error!(!source, "failed to execute the statement"))?;

	// 	Ok(user)
	// }

	// pub async fn login(&self, email: &str) -> Result<(tg::user::User, tg::Id)> {
	// 	// Get a database connection.
	// 	let connection = self
	// 		.inner
	// 		.database
	// 		.connection()
	// 		.await
	// 		.map_err(|source| error!(!source, "failed to get a database connection"))?;

	// 	// Retrieve a user with the specified email, or create one if one does not exist.
	// 	let p = connection.p();
	// 	let statement = formatdoc!(
	// 		"
	// 			select id
	// 			from users
	// 			where email = {p}1;
	// 		"
	// 	);
	// 	let params = db::params![email];
	// 	let id = connection
	// 		.query_optional_value_into(statement, params)
	// 		.await
	// 		.map_err(|source| error!(!source, "failed to execute the statement"))?;
	// 	let user_id = if let Some(id) = id {
	// 		id
	// 	} else {
	// 		let id = tg::Id::new_uuidv7(tg::id::Kind::User);
	// 		let p = connection.p();
	// 		let statement = formatdoc!(
	// 			"
	// 				insert into users (id, email)
	// 				values ({p}1, {p}2);
	// 			"
	// 		);
	// 		let params = db::params![id, email];
	// 		connection
	// 			.execute(statement, params)
	// 			.await
	// 			.map_err(|source| error!(!source, "failed to execute the statement"))?;
	// 		id
	// 	};

	// 	// Create a token.
	// 	let id = tg::Id::new_uuidv7(tg::id::Kind::Token);
	// 	let p = connection.p();
	// 	let statement = formatdoc!(
	// 		"
	// 			insert into tokens (id, user_id)
	// 			values ({p}1, {p}2);
	// 		"
	// 	);
	// 	let params = db::params![id, user_id];
	// 	connection
	// 		.execute(statement, params)
	// 		.await
	// 		.map_err(|source| error!(!source, "failed to execute the statement"))?;

	// 	// Drop the database connection.
	// 	drop(connection);

	// 	// Create the user.
	// 	let user = tg::user::User {
	// 		id: user_id,
	// 		email: email.to_owned(),
	// 		token: Some(id.clone()),
	// 	};

	// 	Ok((user, id))
	// }
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_create_login_request(
		&self,
		_request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Create the login.
		let output = self.inner.tg.create_login().await?;

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().status(200).body(body).unwrap();

		Ok(response)
	}

	pub async fn handle_get_login_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["logins", id] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "unexpected path"));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Get the login.
		let output = self.inner.tg.get_login(&id).await?;

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().status(200).body(body).unwrap();

		Ok(response)
	}

	pub async fn handle_get_user_for_token_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the user from the request.
		let Some(user) = self.try_get_user_from_request(&request).await? else {
			return Ok(unauthorized());
		};

		// Create the body.
		let body = serde_json::to_vec(&user)
			.map_err(|source| error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(body)
			.unwrap();

		Ok(response)
	}

	// pub async fn handle_create_oauth_url_request(
	// 	&self,
	// 	request: http::Request<Incoming>,
	// ) -> Result<http::Response<Outgoing>> {
	// 	#[derive(serde::Deserialize)]
	// 	struct SearchParams {
	// 		id: tg::Id,
	// 	}

	// 	// Get the search params.
	// 	let Some(query) = request.uri().query() else {
	// 		return Ok(bad_request());
	// 	};
	// 	let search_params: SearchParams = if let Ok(form) = serde_urlencoded::from_str(query) {
	// 		form
	// 	} else {
	// 		return Ok(bad_request());
	// 	};

	// 	let oauth_url = self.inner.tg.create_oauth_url(&search_params.id).await?;

	// 	// Respond with a redirect to the oauth URL.
	// 	let response = http::Response::builder()
	// 		.status(http::StatusCode::SEE_OTHER)
	// 		.header(http::header::LOCATION, oauth_url.to_string())
	// 		.body(empty())
	// 		.unwrap();
	// 	Ok(response)
	// }

	// pub async fn handle_oauth_callback_request(
	// 	&self,
	// 	request: http::Request<Incoming>,
	// ) -> Result<http::Response<Outgoing>> {
	// 	#[derive(serde::Serialize, serde::Deserialize)]
	// 	struct SearchParams {
	// 		id: tg::Id,
	// 		code: String,
	// 	}

	// 	// Get the path params.
	// 	let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
	// 	let ["oauth", _provider] = path_components.as_slice() else {
	// 		return Ok(not_found());
	// 	};

	// 	// Get the search params.
	// 	let Some(query) = request.uri().query() else {
	// 		return Ok(bad_request());
	// 	};
	// 	let search_params: SearchParams = if let Ok(form) = serde_urlencoded::from_str(query) {
	// 		form
	// 	} else {
	// 		return Ok(bad_request());
	// 	};

	// 	self.inner
	// 		.tg
	// 		.complete_login(&search_params.id, search_params.code)
	// 		.await?;

	// 	// Respond with a simple success page.
	// 	let response = http::Response::builder()
	// 		.status(http::StatusCode::OK)
	// 		.body(full(
	// 			"You were successfully logged in. You can now close this window.",
	// 		))
	// 		.unwrap();
	// 	Ok(response)
	// }
}
