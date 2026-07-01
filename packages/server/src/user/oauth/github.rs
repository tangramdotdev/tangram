use {
	crate::{Session, config::Github},
	futures::FutureExt as _,
	indoc::formatdoc,
	oauth2::TokenResponse as _,
	std::{borrow::Cow, collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, response::Ext as _, response::builder::Ext as _},
};

#[derive(Clone)]
struct GithubIdentity {
	access_token: String,
	email: Option<String>,
	expires_at: Option<i64>,
	github_user_id: String,
	html_url: String,
	avatar_url: String,
	login: String,
	name: Option<String>,
	refresh_token: Option<String>,
	refresh_token_expires_at: Option<i64>,
	scope: String,
	token_type: String,
	user: tg::user::Id,
}

impl Session {
	pub(crate) async fn oauth_github_authorize_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the GitHub config.
		let github = self.github_config()?;

		// Get the query params.
		let query = parse_query(request.uri().query().unwrap_or(""))?;
		let code = required_form_value(&query, "code")?.to_owned();

		// Create the state.
		let state = crate::user::login::create_token();
		let now = time::OffsetDateTime::now_utc().unix_timestamp();

		// Update the login.
		self.server
			.database
			.run(|transaction| {
				let code = code.clone();
				let state = state.clone();
				async move {
					#[derive(db::row::Deserialize)]
					struct Row {
						expires_at: i64,
						provider: String,
						status: String,
					}
					let p = transaction.p();
					let statement = formatdoc!(
						"
							select expires_at, provider, status
							from logins
							where code = {p}1;
						"
					);
					let row = transaction
						.query_optional_into::<Row>(statement.into(), db::params![code.clone()])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
						.ok_or_else(|| tg::error!("invalid login code"))?;
					if row.provider != "github" {
						return Err(tg::error!("invalid login provider").into());
					}
					if row.status != "started" {
						return Err(tg::error!("the login has not started").into());
					}
					if now > row.expires_at {
						return Err(tg::error!("the login has expired").into());
					}
					let statement = formatdoc!(
						"
							update logins
							set state = {p}1, updated_at = {p}2
							where code = {p}3 and status = 'started';
						"
					);
					transaction
						.execute(statement.into(), db::params![state, now, code])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await?;

		// Create the authorization URL.
		let client = github_client(github)?;
		let (url, _) = client.authorize_url(|| oauth2::CsrfToken::new(state)).url();

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::FOUND)
			.header(http::header::LOCATION, url.to_string())
			.empty()
			.unwrap()
			.boxed_body();

		Ok(response)
	}

	pub(crate) async fn oauth_github_callback_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the GitHub config.
		let github = self.github_config()?.clone();

		// Get the query params.
		let query = parse_query(request.uri().query().unwrap_or(""))?;
		let authorization_code = required_form_value(&query, "code")?.to_owned();
		let state = required_form_value(&query, "state")?.to_owned();

		// Claim the login state.
		let (code, claimed) = self.claim_github_login_state(&state).await?;
		if !claimed {
			if self.login_succeeded(&code).await? {
				return self.login_redirect("succeeded", None);
			}
			return Err(tg::error!("the login has not succeeded"));
		}

		// Complete the login.
		if let Err(error) = self
			.complete_github_login_with_code(github, authorization_code, code.clone())
			.await
		{
			self.finish_login_with_error(&code, error.to_string())
				.await?;
			return self.login_redirect("failed", Some(&error.to_string()));
		}

		// Create the response.
		self.login_redirect("succeeded", None)
	}

	fn github_config(&self) -> tg::Result<&Github> {
		self.server
			.config()
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.providers.github.as_ref())
			.ok_or_else(|| tg::error!("the GitHub authentication provider is not configured"))
	}

	fn login_web_url(&self) -> tg::Result<String> {
		self.server
			.config()
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.web_url.as_deref())
			.map(|url| url.trim_end_matches('/').to_owned())
			.ok_or_else(|| tg::error!("missing login web URL"))
	}

	fn login_redirect(
		&self,
		status: &str,
		message: Option<&str>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Create the location.
		let mut location = format!("{}/login/{status}", self.login_web_url()?);
		if let Some(message) = message {
			let message = tangram_uri::encode_query_value(message);
			location.push_str("?message=");
			location.push_str(&message);
		}

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::FOUND)
			.header(http::header::LOCATION, location)
			.empty()
			.unwrap()
			.boxed_body();

		Ok(response)
	}

	async fn claim_github_login_state(&self, state: &str) -> tg::Result<(String, bool)> {
		// Claim the login state.
		#[derive(db::row::Deserialize)]
		struct Row {
			claimed_at: Option<i64>,
			code: String,
			expires_at: i64,
		}
		let (code, claimed) = self
			.server
			.database
			.run(|transaction| {
				let state = state.to_owned();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
							select claimed_at, expires_at, code
							from logins
							where provider = 'github' and state = {p}1;
						"
					);
					let row = transaction
						.query_optional_into::<Row>(statement.into(), db::params![state.clone()])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
						.ok_or_else(|| tg::error!("invalid login state"))?;
					let now = time::OffsetDateTime::now_utc().unix_timestamp();
					if now > row.expires_at {
						return Err(tg::error!("the login has expired").into());
					}
					if row.claimed_at.is_some() {
						return Ok(ControlFlow::Break((row.code, false)));
					}
					let statement = formatdoc!(
						"
							update logins
							set claimed_at = {p}1, updated_at = {p}1
							where state = {p}2 and provider = 'github' and claimed_at is null;
						"
					);
					let n = transaction
						.execute(statement.into(), db::params![now, state])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					Ok::<_, crate::database::Error>(ControlFlow::Break((row.code, n == 1)))
				}
				.boxed()
			})
			.await?;
		Ok((code, claimed))
	}

	async fn login_succeeded(&self, code: &str) -> tg::Result<bool> {
		// Get the login.
		#[derive(db::row::Deserialize)]
		struct Row {
			error: Option<String>,
			status: String,
			token: Option<String>,
			user: Option<String>,
		}
		let row = self
			.server
			.database
			.run(|transaction| {
				let code = code.to_owned();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						r#"
							select error, status, token, "user"
							from logins
							where code = {p}1;
						"#
					);
					let row = transaction
						.query_optional_into::<Row>(statement.into(), db::params![code])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(row))
				}
				.boxed()
			})
			.await?;

		// Check the login.
		Ok(row.is_some_and(|row| {
			row.status == "finished"
				&& row.error.is_none()
				&& row.token.is_some()
				&& row.user.is_some()
		}))
	}

	async fn complete_github_login_with_code(
		&self,
		github: Github,
		authorization_code: String,
		code: String,
	) -> tg::Result<()> {
		// Exchange the code.
		let client = github_client(&github)?;
		let http_client = oauth2::reqwest::ClientBuilder::new()
			.redirect(oauth2::reqwest::redirect::Policy::none())
			.build()
			.map_err(|error| tg::error!(!error, "failed to build the HTTP client"))?;
		let http_client_ = http_client.clone();
		let token = client
			.exchange_code(oauth2::AuthorizationCode::new(authorization_code))
			.request_async(&move |request: oauth2::HttpRequest| {
				let http_client = http_client_.clone();
				async move {
					let response = http_client
						.execute(request.try_into().map_err(Box::new)?)
						.await
						.map_err(Box::new)?;
					let mut builder = http::Response::builder().status(response.status());
					#[cfg(not(target_arch = "wasm32"))]
					{
						builder = builder.version(response.version());
					}
					for (name, value) in response.headers() {
						builder = builder.header(name, value);
					}
					builder
						.body(response.bytes().await.map_err(Box::new)?.to_vec())
						.map_err(oauth2::HttpClientError::Http)
				}
			})
			.await
			.map_err(|error| match error {
				oauth2::RequestTokenError::ServerResponse(response) => tg::error!(
					error = %response.error(),
					description = ?response.error_description(),
					"failed to exchange the authorization code"
				),
				error => tg::error!(!error, "failed to exchange the authorization code"),
			})?;

		// Get the token data.
		let access_token = token.access_token().secret().to_owned();
		let refresh_token = token.refresh_token().map(|token| token.secret().to_owned());
		let scope = token
			.scopes()
			.map(|scopes| {
				scopes
					.iter()
					.map(|scope| scope.as_str().to_owned())
					.collect::<Vec<_>>()
					.join(",")
			})
			.unwrap_or_default();
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let expires_at = token
			.expires_in()
			.map(|duration| now + i64::try_from(duration.as_secs()).unwrap_or(i64::MAX));

		// Get the GitHub user.
		let github = octocrab::Octocrab::builder()
			.personal_token(access_token.clone())
			.build()
			.map_err(|error| tg::error!(!error, "failed to build the GitHub client"))?;
		let github_user = github
			.current()
			.user()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the GitHub user"))?;

		// Get the GitHub emails.
		let mut emails = github
			.users("current_user")
			.emails()
			.list()
			.await
			.inspect_err(|error| tracing::warn!(%error, "failed to get the GitHub emails"))
			.map(|page| {
				page.items
					.into_iter()
					.filter(|email| email.verified)
					.map(|email| email.email)
					.collect::<Vec<_>>()
			})
			.unwrap_or_default();
		if let Some(email) = &github_user.email {
			emails.push(email.clone());
		}
		emails.sort();
		emails.dedup();

		// Finish the login.
		let user = self
			.finish_login(crate::user::login::FinishLoginArg {
				code,
				default_specifier: github_user.login.parse()?,
				emails,
				identity: Some(crate::user::login::LoginIdentity {
					provider: "github".to_owned(),
					subject: github_user.id.to_string(),
				}),
			})
			.await?;

		// Upsert the GitHub identity.
		self.upsert_github_identity(GithubIdentity {
			access_token,
			avatar_url: github_user.avatar_url.to_string(),
			email: github_user.email,
			refresh_token,
			expires_at,
			github_user_id: github_user.id.to_string(),
			html_url: github_user.html_url.to_string(),
			login: github_user.login,
			name: github_user.name,
			refresh_token_expires_at: None,
			scope,
			token_type: "Bearer".to_owned(),
			user: user.id,
		})
		.await?;

		Ok(())
	}

	async fn finish_login_with_error(&self, code: &str, error: String) -> tg::Result<()> {
		// Fail the login.
		self.server
			.database
			.run(|transaction| {
				let code = code.to_owned();
				let error = error.clone();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
							update logins
							set status = 'finished', error = {p}1, updated_at = {p}2
							where code = {p}3 and status = 'started';
						"
					);
					let now = time::OffsetDateTime::now_utc().unix_timestamp();
					transaction
						.execute(statement.into(), db::params![error, now, code])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await?;
		Ok(())
	}

	async fn upsert_github_identity(&self, identity: GithubIdentity) -> tg::Result<()> {
		// Upsert the GitHub identity.
		self.server
			.database
			.run(|transaction| {
				let identity = identity.clone();
				async move {
					let p = transaction.p();
					let now = time::OffsetDateTime::now_utc().unix_timestamp();
					let statement = formatdoc!(
						r#"
							insert into github_identities (
								"user", github_user_id, login, name, email, avatar_url, html_url,
								access_token, refresh_token, token_type, scope, expires_at,
								refresh_token_expires_at, updated_at
							)
							values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7, {p}8, {p}9, {p}10, {p}11, {p}12, {p}13, {p}14)
							on conflict ("user") do update set
								github_user_id = excluded.github_user_id,
								login = excluded.login,
								name = excluded.name,
								email = excluded.email,
								avatar_url = excluded.avatar_url,
								html_url = excluded.html_url,
								access_token = excluded.access_token,
								refresh_token = excluded.refresh_token,
								token_type = excluded.token_type,
								scope = excluded.scope,
								expires_at = excluded.expires_at,
								refresh_token_expires_at = excluded.refresh_token_expires_at,
								updated_at = excluded.updated_at;
						"#
					);
					transaction
						.execute(
							statement.into(),
							db::params![
								identity.user.to_string(),
								identity.github_user_id,
								identity.login,
								identity.name,
								identity.email,
								identity.avatar_url,
								identity.html_url,
								identity.access_token,
								identity.refresh_token,
								identity.token_type,
								identity.scope,
								identity.expires_at,
								identity.refresh_token_expires_at,
								now,
							],
						)
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await?;
		Ok(())
	}
}

fn github_client(
	github: &Github,
) -> tg::Result<
	oauth2::Client<
		oauth2::StandardErrorResponse<oauth2::basic::BasicErrorResponseType>,
		oauth2::StandardTokenResponse<oauth2::EmptyExtraTokenFields, oauth2::basic::BasicTokenType>,
		oauth2::StandardTokenIntrospectionResponse<
			oauth2::EmptyExtraTokenFields,
			oauth2::basic::BasicTokenType,
		>,
		oauth2::StandardRevocableToken,
		oauth2::StandardErrorResponse<oauth2::RevocationErrorResponseType>,
		oauth2::EndpointSet,
		oauth2::EndpointNotSet,
		oauth2::EndpointNotSet,
		oauth2::EndpointNotSet,
		oauth2::EndpointSet,
	>,
> {
	// Create the client.
	Ok(
		oauth2::basic::BasicClient::new(oauth2::ClientId::new(github.client_id.clone()))
			.set_client_secret(oauth2::ClientSecret::new(github.client_secret.clone()))
			.set_auth_uri(
				oauth2::AuthUrl::new(github.auth_url.clone())
					.map_err(|error| tg::error!(!error, "invalid GitHub auth URL"))?,
			)
			.set_token_uri(
				oauth2::TokenUrl::new(github.token_url.clone())
					.map_err(|error| tg::error!(!error, "invalid GitHub token URL"))?,
			)
			.set_redirect_uri(
				oauth2::RedirectUrl::new(github.redirect_url.clone())
					.map_err(|error| tg::error!(!error, "invalid GitHub redirect URL"))?,
			)
			.set_auth_type(oauth2::AuthType::RequestBody),
	)
}

fn parse_query(input: &str) -> tg::Result<BTreeMap<String, String>> {
	input
		.split('&')
		.filter(|component| !component.is_empty())
		.map(|component| {
			let (key, value) = component.split_once('=').unwrap_or((component, ""));
			let key = decode_form_component(key)?;
			let value = decode_form_component(value)?;
			Ok((key, value))
		})
		.collect()
}

fn decode_form_component(input: &str) -> tg::Result<String> {
	let input = input.replace('+', " ");
	let decoded: Cow<'_, str> = tangram_uri::decode_query_value(&input)
		.map_err(|error| tg::error!(!error, "failed to decode the form value"))?;
	Ok(decoded.into_owned())
}

fn required_form_value<'a>(form: &'a BTreeMap<String, String>, key: &str) -> tg::Result<&'a str> {
	form.get(key)
		.map(String::as_str)
		.ok_or_else(|| tg::error!(%key, "missing form value"))
}
