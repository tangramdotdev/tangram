use crate::{database::Database, postgres_params, Http, Server};
use oauth2::TokenResponse;
use std::borrow::Cow;
use tangram_client as tg;
use tangram_error::{error, Result};
use tangram_util::http::{
	bad_request, empty, full, get_token, not_found, unauthorized, Incoming, Outgoing,
};
use url::Url;

impl Server {
	pub async fn create_login(&self) -> Result<tg::user::Login> {
		if let Some(remote) = self.inner.remote.as_ref() {
			return remote.create_login().await;
		}

		// Create the ID.
		let id = tg::Id::new_uuidv7(tg::id::Kind::Login);

		// Create the URL.
		let mut url = self
			.inner
			.options
			.www
			.as_ref()
			.ok_or_else(|| error!("expected the WWW URL to be set"))?
			.clone();
		url.set_path("/login");
		url.set_query(Some(&format!("id={id}")));

		// Create the login.
		let login = tg::user::Login {
			id: id.clone(),
			url: url.clone(),
			token: None,
		};

		// Add the login to the database.
		let Database::Postgres(database) = &self.inner.database else {
			return Err(error!("unimplemented"));
		};
		let connection = database.get().await?;
		let statement = "
			insert into logins (id, url)
			values ($1, $2);
		";
		let params = postgres_params![id.to_string(), url.to_string()];
		let statement = connection
			.prepare_cached(statement)
			.await
			.map_err(|error| error!(source = error, "failed to prepare the statement"))?;
		connection
			.execute(&statement, params)
			.await
			.map_err(|error| error!(source = error, "failed to execute the statement"))?;

		Ok(login)
	}

	pub async fn get_login(&self, id: &tg::Id) -> Result<Option<tg::user::Login>> {
		if let Some(remote) = self.inner.remote.as_ref() {
			return remote.get_login(id).await;
		}

		let Database::Postgres(database) = &self.inner.database else {
			return Err(error!("unimplemented"));
		};
		let connection = database.get().await?;
		let statement = "
			select id, url, token
			from logins
			where id = $1;
		";
		let params = postgres_params![id.to_string()];
		let statement = connection
			.prepare_cached(statement)
			.await
			.map_err(|error| error!(source = error, "failed to prepare the statement"))?;
		let Some(row) = connection
			.query_opt(&statement, params)
			.await
			.map_err(|error| error!(source = error, "failed to execute the statement"))?
		else {
			return Ok(None);
		};
		let id = row.get::<_, String>(0).parse()?;
		let url = row
			.get::<_, String>(1)
			.parse()
			.map_err(|error| error!(source = error, "failed to parse the URL"))?;
		let token = row.get(2);
		let login = tg::user::Login { id, url, token };
		Ok(Some(login))
	}

	pub async fn get_user_for_token(&self, token: &str) -> Result<Option<tg::user::User>> {
		if let Some(remote) = self.inner.remote.as_ref() {
			return remote.get_user_for_token(token).await;
		}

		let Database::Postgres(database) = &self.inner.database else {
			return Err(error!("unimplemented"));
		};
		let connection = database.get().await?;
		let statement = "
			select users.id, users.email, tokens.token
			from users
			join tokens on tokens.user_id = users.id
			where tokens.token = $1;
		";
		let params = postgres_params![token];
		let statement = connection
			.prepare_cached(statement)
			.await
			.map_err(|error| error!(source = error, "failed to prepare the statement"))?;
		let row = connection
			.query_opt(&statement, params)
			.await
			.map_err(|error| error!(source = error, "failed to execute the statement"))?;
		let Some(row) = row else {
			return Ok(None);
		};
		let id = row.get::<_, String>(0).parse()?;
		let email = row.get::<_, String>(1);
		let token = row.get::<_, String>(2).parse()?;
		let user = tg::user::User {
			id,
			email,
			token: Some(token),
		};
		Ok(Some(user))
	}

	pub async fn create_oauth_url(&self, login_id: &tg::Id) -> Result<Url> {
		// Create the redirect URL.
		let mut redirect_url = self
			.inner
			.options
			.url
			.as_ref()
			.ok_or_else(|| error!("expected the URL to be set in options"))?
			.clone();
		redirect_url.set_path("/oauth/github");
		redirect_url.set_query(Some(&format!("id={login_id}")));

		// Get the GitHub OAuth client.
		let oauth_client = self
			.inner
			.oauth
			.github
			.as_ref()
			.ok_or_else(|| error!("the GitHub OAuth client is not configured"))?;

		// Create the authorize URL.
		let oauth_redirect_url = oauth2::RedirectUrl::new(redirect_url.to_string())
			.map_err(|error| error!(source = error, "failed to create the redirect URL"))?;
		let (oauth_authorize_url, _csrf_token) = oauth_client
			.authorize_url(oauth2::CsrfToken::new_random)
			.set_redirect_uri(Cow::Owned(oauth_redirect_url))
			.add_scope(oauth2::Scope::new("user:email".to_owned()))
			.url();

		Ok(oauth_authorize_url)
	}

	pub async fn login_with_github_code(
		&self,
		login_id: tg::Id,
		github_code: String,
	) -> Result<tg::user::User> {
		// Get the GitHub OAuth client.
		let github_oauth_client = self
			.inner
			.oauth
			.github
			.as_ref()
			.ok_or_else(|| error!("the GitHub OAuth client is not configured"))?;

		// Get a GitHub token.
		let github_token = github_oauth_client
			.exchange_code(oauth2::AuthorizationCode::new(github_code))
			.request_async(oauth2::reqwest::async_http_client)
			.await
			.map_err(|error| error!(source = error, "failed to exchange the code"))?;

		// Create the GitHub client.
		let octocrab = octocrab::OctocrabBuilder::new()
			.oauth(octocrab::auth::OAuth {
				access_token: github_token.access_token().secret().clone().into(),
				token_type: match github_token.token_type() {
					oauth2::basic::BasicTokenType::Bearer => "Bearer".to_owned(),
					token_type => return Err(error!(?token_type, "unsupported token type")),
				},
				scope: github_token.scopes().map_or_else(Vec::new, |scopes| {
					scopes.iter().map(|scope| scope.to_string()).collect()
				}),
				expires_in: None,
				refresh_token: None,
				refresh_token_expires_in: None,
			})
			.build()
			.map_err(|error| error!(source = error, "failed to create the GitHub client"))?;

		// Get the GitHub user's primary email.
		#[derive(serde::Deserialize)]
		struct Entry {
			email: String,
			primary: bool,
			verified: bool,
		}
		let emails: Vec<Entry> = octocrab
			.get("/user/emails", None::<&()>)
			.await
			.map_err(|error| error!(source = error, "failed to perform the request"))?;
		let email = emails
			.into_iter()
			.find(|email| email.verified && email.primary)
			.map(|email| email.email)
			.ok_or_else(|| {
				error!("the GitHub user does not have a verified primary email address")
			})?;

		// Log the user in.
		let (user, token) = self.login(&email).await?;

		// Update the login with the user.
		match &self.inner.database {
			Database::Sqlite(_) => return Err(error!("unimplemented")),

			Database::Postgres(database) => {
				let connection = database.get().await?;
				let statement = "
					update logins
					set token = $1
					where id = $2;
				";
				let params = postgres_params![token.to_string(), login_id.to_string()];
				let statement = connection
					.prepare_cached(statement)
					.await
					.map_err(|error| error!(source = error, "failed to prepare the statement"))?;
				connection
					.execute(&statement, params)
					.await
					.map_err(|error| error!(source = error, "failed to execute the statement"))?;
			},
		}

		Ok(user)
	}

	pub async fn login(&self, email: &str) -> Result<(tg::user::User, tg::Id)> {
		let Database::Postgres(database) = &self.inner.database else {
			return Err(error!("unimplemented"));
		};
		let connection = database.get().await?;

		// Retrieve a user with the specified email, or create one if one does not exist.
		let statement = "
			select id
			from users
			where email = $1;
		";
		let params = postgres_params![email];
		let statement = connection
			.prepare_cached(statement)
			.await
			.map_err(|error| error!(source = error, "failed to prepare the statement"))?;
		let row = connection
			.query_opt(&statement, params)
			.await
			.map_err(|error| error!(source = error, "failed to execute the statement"))?;
		let user_id = if let Some(row) = row {
			row.get::<_, String>(0).parse()?
		} else {
			let id = tg::Id::new_uuidv7(tg::id::Kind::User);
			let statement = "
				insert into users (id, email)
				values ($1, $2);
			";
			let params = postgres_params![id.to_string(), email];
			let statement = connection
				.prepare_cached(statement)
				.await
				.map_err(|error| error!(source = error, "failed to prepare the statement"))?;
			connection
				.execute(&statement, params)
				.await
				.map_err(|error| error!(source = error, "failed to execute the statement"))?;
			id
		};

		// Create a token.
		let id = tg::Id::new_uuidv7(tg::id::Kind::Token);
		let statement = "
			insert into tokens (id, user_id)
			values ($1, $2);
		";
		let params = postgres_params![id.to_string(), user_id.to_string()];
		let statement = connection
			.prepare_cached(statement)
			.await
			.map_err(|error| error!(source = error, "failed to prepare the statement"))?;
		connection
			.execute(&statement, params)
			.await
			.map_err(|error| error!(source = error, "failed to execute the statement"))?;

		let user = tg::user::User {
			id: user_id,
			email: email.to_owned(),
			token: Some(id.clone()),
		};

		Ok((user, id))
	}

	#[must_use]
	pub fn user_is_admin(&self, user: Option<&tg::user::User>) -> bool {
		if cfg!(debug_assertions) {
			return true;
		}
		const ADMINS: [&str; 4] = [
			"david@yamnitsky.com",
			"isabella.tromba@gmail.com",
			"ben@deciduously.com",
			"mike@hilgendorf.audio",
		];
		user.map_or(false, |user| ADMINS.contains(&user.email.as_str()))
	}
}

impl Http {
	pub async fn handle_create_login_request(
		&self,
		_request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Create the login.
		let output = self.inner.tg.create_login().await?;

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|error| error!(source = error, "failed to serialize the body"))?;
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
			.map_err(|error| error!(source = error, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().status(200).body(body).unwrap();

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
		let Some(output) = self.inner.tg.get_user_for_token(token.as_str()).await? else {
			return Ok(unauthorized());
		};

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|error| error!(source = error, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(body)
			.unwrap();

		Ok(response)
	}

	pub async fn handle_create_oauth_url_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		#[derive(serde::Deserialize)]
		struct SearchParams {
			id: tg::Id,
		}

		// Get the search params.
		let Some(query) = request.uri().query() else {
			return Ok(bad_request());
		};
		let search_params: SearchParams = if let Ok(form) = serde_urlencoded::from_str(query) {
			form
		} else {
			return Ok(bad_request());
		};

		let oauth_url = self.inner.tg.create_oauth_url(&search_params.id).await?;

		// Respond with a redirect to the oauth URL.
		let response = http::Response::builder()
			.status(http::StatusCode::SEE_OTHER)
			.header(http::header::LOCATION, oauth_url.to_string())
			.body(empty())
			.unwrap();
		Ok(response)
	}

	pub async fn handle_oauth_callback_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		#[derive(serde::Serialize, serde::Deserialize)]
		struct SearchParams {
			id: tg::Id,
			code: String,
		}

		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["oauth", _provider] = path_components.as_slice() else {
			return Ok(not_found());
		};

		// Get the search params.
		let Some(query) = request.uri().query() else {
			return Ok(bad_request());
		};
		let search_params: SearchParams = if let Ok(form) = serde_urlencoded::from_str(query) {
			form
		} else {
			return Ok(bad_request());
		};

		self.inner
			.tg
			.complete_login(&search_params.id, search_params.code)
			.await?;

		// Respond with a simple success page.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(
				"You were successfully logged in. You can now close this window.",
			))
			.unwrap();
		Ok(response)
	}
}
