use tangram_index::Index as _;
use {
	crate::{Session, config::Github},
	futures::FutureExt as _,
	indoc::formatdoc,
	oauth2::{
		AuthType, AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken, RedirectUrl,
		Scope, TokenResponse as _, TokenUrl, basic::BasicClient,
	},
	std::{borrow::Cow, collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

const DEVICE_AUTHORIZATION_EXPIRES_IN: i64 = 900;
const DEVICE_AUTHORIZATION_INTERVAL: u64 = 5;
const DEVICE_CODE_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:device_code";

impl Session {
	pub(crate) async fn create_oauth_device_session_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let fallback_origin = self
			.server
			.config()
			.http
			.as_ref()
			.and_then(|http| http.listeners.first())
			.and_then(|listener| uri_origin(&listener.url));
		let origin = request_origin(&request, fallback_origin.as_deref())?;
		let form = parse_form(
			&request
				.text()
				.await
				.map_err(|error| tg::error!(!error, "failed to read the request body"))?,
		)?;
		let client_id = required_form_value(&form, "client_id")?.to_owned();
		let name = form.get("name").cloned();
		let scope = form.get("scope").cloned();
		let session = Self::create_user_token();
		let device_code = Self::create_user_token();
		let user_code = create_user_code();
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let expires_at = now + DEVICE_AUTHORIZATION_EXPIRES_IN;
		self.server
			.database
			.run(|transaction| {
				let session = session.clone();
				let device_code = device_code.clone();
				let user_code = user_code.clone();
				let client_id = client_id.clone();
				let name = name.clone();
				let scope = scope.clone();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
								insert into oauth_sessions (
									id, flow, device_code, user_code, client_id, scope, status, expires_at, created_at, name
								)
								values ({p}1, 'device', {p}2, {p}3, {p}4, {p}5, 'started', {p}6, {p}7, {p}8);
							"
					);
					transaction
						.execute(
							statement.into(),
							db::params![
								session,
								device_code,
								user_code,
								client_id,
								scope,
								expires_at,
								now,
								name
							],
						)
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await?;
		let web_url = self.login_web_url()?;
		let server = tangram_uri::encode_query_value(&origin);
		let user_code_query = tangram_uri::encode_query_value(&user_code);
		let verification_uri = format!("{web_url}/login?server={server}");
		let verification_uri_complete = format!("{verification_uri}&user_code={user_code_query}");
		let output = tg::oauth::device_session::Output {
			device_code,
			user_code,
			verification_uri,
			verification_uri_complete,
			expires_in: DEVICE_AUTHORIZATION_EXPIRES_IN.try_into().unwrap(),
			interval: DEVICE_AUTHORIZATION_INTERVAL,
		};
		let body = serde_json::to_vec(&output).unwrap();
		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.body(BoxBody::with_bytes(body))
			.unwrap()
			.boxed_body();
		Ok(response)
	}

	pub(crate) async fn get_oauth_token_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let form = parse_form(
			&request
				.text()
				.await
				.map_err(|error| tg::error!(!error, "failed to read the request body"))?,
		)?;
		let grant_type = required_form_value(&form, "grant_type")?;
		if grant_type != DEVICE_CODE_GRANT_TYPE {
			let body = serde_json::to_vec(&tg::oauth::token::Error {
				error: "unsupported_grant_type".to_owned(),
				error_description: None,
			})
			.unwrap();
			let response = http::Response::builder()
				.status(http::StatusCode::BAD_REQUEST)
				.header(
					http::header::CONTENT_TYPE,
					mime::APPLICATION_JSON.to_string(),
				)
				.body(BoxBody::with_bytes(body))
				.unwrap()
				.boxed_body();
			return Ok(response);
		}
		let device_code = required_form_value(&form, "device_code")?;
		let client_id = required_form_value(&form, "client_id")?;
		#[derive(db::row::Deserialize)]
		struct Row {
			access_token: Option<String>,
			client_id: String,
			error: Option<String>,
			expires_at: i64,
			status: String,
		}
		let row = self
			.server
			.database
			.run(|transaction| {
				let device_code = device_code.to_owned();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
							select access_token, client_id, error, expires_at, status
							from oauth_sessions
							where device_code = {p}1;
						"
					);
					let row = transaction
						.query_optional_into::<Row>(statement.into(), db::params![device_code])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(row))
				}
				.boxed()
			})
			.await?;
		let Some(row) = row else {
			let body = serde_json::to_vec(&tg::oauth::token::Error {
				error: "invalid_grant".to_owned(),
				error_description: Some("invalid device code".to_owned()),
			})
			.unwrap();
			let response = http::Response::builder()
				.status(http::StatusCode::BAD_REQUEST)
				.header(
					http::header::CONTENT_TYPE,
					mime::APPLICATION_JSON.to_string(),
				)
				.body(BoxBody::with_bytes(body))
				.unwrap()
				.boxed_body();
			return Ok(response);
		};
		if row.client_id != client_id {
			let body = serde_json::to_vec(&tg::oauth::token::Error {
				error: "invalid_client".to_owned(),
				error_description: None,
			})
			.unwrap();
			let response = http::Response::builder()
				.status(http::StatusCode::BAD_REQUEST)
				.header(
					http::header::CONTENT_TYPE,
					mime::APPLICATION_JSON.to_string(),
				)
				.body(BoxBody::with_bytes(body))
				.unwrap()
				.boxed_body();
			return Ok(response);
		}
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		if now > row.expires_at {
			let body = serde_json::to_vec(&tg::oauth::token::Error {
				error: "expired_token".to_owned(),
				error_description: None,
			})
			.unwrap();
			let response = http::Response::builder()
				.status(http::StatusCode::BAD_REQUEST)
				.header(
					http::header::CONTENT_TYPE,
					mime::APPLICATION_JSON.to_string(),
				)
				.body(BoxBody::with_bytes(body))
				.unwrap()
				.boxed_body();
			return Ok(response);
		}
		match row.status.as_str() {
			"started" => {
				let body = serde_json::to_vec(&tg::oauth::token::Error {
					error: "authorization_pending".to_owned(),
					error_description: None,
				})
				.unwrap();
				let response = http::Response::builder()
					.status(http::StatusCode::BAD_REQUEST)
					.header(
						http::header::CONTENT_TYPE,
						mime::APPLICATION_JSON.to_string(),
					)
					.body(BoxBody::with_bytes(body))
					.unwrap()
					.boxed_body();
				Ok(response)
			},
			"failed" => {
				let body = serde_json::to_vec(&tg::oauth::token::Error {
					error: "access_denied".to_owned(),
					error_description: Some(
						row.error.unwrap_or_else(|| "the login failed".to_owned()),
					),
				})
				.unwrap();
				let response = http::Response::builder()
					.status(http::StatusCode::BAD_REQUEST)
					.header(
						http::header::CONTENT_TYPE,
						mime::APPLICATION_JSON.to_string(),
					)
					.body(BoxBody::with_bytes(body))
					.unwrap()
					.boxed_body();
				Ok(response)
			},
			"succeeded" => {
				let Some(access_token) = row.access_token else {
					let body = serde_json::to_vec(&tg::oauth::token::Error {
						error: "server_error".to_owned(),
						error_description: Some("missing access token".to_owned()),
					})
					.unwrap();
					let response = http::Response::builder()
						.status(http::StatusCode::BAD_REQUEST)
						.header(
							http::header::CONTENT_TYPE,
							mime::APPLICATION_JSON.to_string(),
						)
						.body(BoxBody::with_bytes(body))
						.unwrap()
						.boxed_body();
					return Ok(response);
				};
				let body = serde_json::to_vec(&tg::oauth::token::Token {
					access_token,
					token_type: "Bearer".to_owned(),
					expires_in: None,
				})
				.unwrap();
				let response = http::Response::builder()
					.header(
						http::header::CONTENT_TYPE,
						mime::APPLICATION_JSON.to_string(),
					)
					.body(BoxBody::with_bytes(body))
					.unwrap()
					.boxed_body();
				Ok(response)
			},
			_ => {
				let body = serde_json::to_vec(&tg::oauth::token::Error {
					error: "server_error".to_owned(),
					error_description: Some("invalid session status".to_owned()),
				})
				.unwrap();
				let response = http::Response::builder()
					.status(http::StatusCode::BAD_REQUEST)
					.header(
						http::header::CONTENT_TYPE,
						mime::APPLICATION_JSON.to_string(),
					)
					.body(BoxBody::with_bytes(body))
					.unwrap()
					.boxed_body();
				Ok(response)
			},
		}
	}

	pub(crate) async fn get_login_device_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let fallback_origin = self
			.server
			.config()
			.http
			.as_ref()
			.and_then(|http| http.listeners.first())
			.and_then(|listener| uri_origin(&listener.url));
		let origin = request_origin(&request, fallback_origin.as_deref())?;
		let web_url = self.login_web_url()?;
		let query = parse_query(request.uri().query().unwrap_or(""))?;
		let server = tangram_uri::encode_query_value(&origin);
		let mut location = format!("{web_url}/login?server={server}");
		if let Some(user_code) = query.get("user_code") {
			let user_code = tangram_uri::encode_query_value(user_code);
			location.push_str("&user_code=");
			location.push_str(&user_code);
		}
		let response = http::Response::builder()
			.status(http::StatusCode::FOUND)
			.header(http::header::LOCATION, location)
			.empty()
			.unwrap()
			.boxed_body();
		Ok(response)
	}

	pub(crate) async fn authorize_login_device_with_github_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let github = self.github_config()?;
		let query = parse_query(request.uri().query().unwrap_or(""))?;
		let user_code = query
			.get("user_code")
			.ok_or_else(|| tg::error!("missing user code"))?;
		let session = self.oauth_session_from_user_code(user_code).await?;
		let state = Self::create_user_token();
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		self.server
			.database
			.run(|transaction| {
				let state = state.clone();
				let session = session.clone();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
								insert into oauth_states (state, session, provider, expires_at)
								values ({p}1, {p}2, 'github', {p}3);
							"
					);
					transaction
						.execute(
							statement.into(),
							db::params![state, session, now + DEVICE_AUTHORIZATION_EXPIRES_IN],
						)
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await?;
		let oauth = github_oauth_client(github)?;
		let mut authorize = oauth.authorize_url(|| CsrfToken::new(state));
		for scope in &github.scopes {
			authorize = authorize.add_scope(Scope::new(scope.clone()));
		}
		let (url, _) = authorize.url();
		tracing::info!(url = %url, "redirecting to GitHub authorization URL");
		let response = http::Response::builder()
			.status(http::StatusCode::FOUND)
			.header(http::header::LOCATION, url.to_string())
			.empty()
			.unwrap()
			.boxed_body();
		Ok(response)
	}

	pub(crate) async fn github_login_callback_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let github = self.github_config()?;
		let query = parse_query(request.uri().query().unwrap_or(""))?;
		let code = required_form_value(&query, "code")?.to_owned();
		let state = required_form_value(&query, "state")?.to_owned();
		let (oauth_session, claimed) = self.claim_oauth_state(&state).await?;
		if !claimed {
			if self
				.wait_for_oauth_session_to_succeed(&oauth_session)
				.await?
			{
				let location = format!("{}/login/succeeded", self.login_web_url()?);
				let response = http::Response::builder()
					.status(http::StatusCode::FOUND)
					.header(http::header::LOCATION, location)
					.empty()
					.unwrap()
					.boxed_body();
				return Ok(response);
			}
			return Err(tg::error!("the login has not succeeded"));
		}
		let session = self.clone();
		let github = github.clone();
		let task = tokio::spawn(async move {
			session
				.complete_github_login_with_code(github, code, oauth_session)
				.await
		});
		if let Err(error) = task
			.await
			.map_err(|error| tg::error!(!error, "the login task failed"))?
		{
			let message = tangram_uri::encode_query_value(&error.to_string());
			let location = format!("{}/login/failed?message={message}", self.login_web_url()?);
			let response = http::Response::builder()
				.status(http::StatusCode::FOUND)
				.header(http::header::LOCATION, location)
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
		}
		let location = format!("{}/login/succeeded", self.login_web_url()?);
		let response = http::Response::builder()
			.status(http::StatusCode::FOUND)
			.header(http::header::LOCATION, location)
			.empty()
			.unwrap()
			.boxed_body();
		Ok(response)
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

	async fn oauth_session_from_user_code(&self, user_code: &str) -> tg::Result<String> {
		#[derive(db::row::Deserialize)]
		struct Row {
			id: String,
			expires_at: i64,
			status: String,
		}
		let row = self
			.server
			.database
			.run(|transaction| {
				let user_code = user_code.to_owned();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
								select id, expires_at, status
								from oauth_sessions
								where flow = 'device' and user_code = {p}1;
							"
					);
					let row = transaction
						.query_optional_into::<Row>(statement.into(), db::params![user_code])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(row))
				}
				.boxed()
			})
			.await?
			.ok_or_else(|| tg::error!("invalid user code"))?;
		if row.status != "started" {
			return Err(tg::error!("the login has not started"));
		}
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		if now > row.expires_at {
			return Err(tg::error!("the login has expired"));
		}
		Ok(row.id)
	}

	async fn claim_oauth_state(&self, state: &str) -> tg::Result<(String, bool)> {
		#[derive(db::row::Deserialize)]
		struct Row {
			claimed_at: Option<i64>,
			session: String,
			expires_at: i64,
		}
		let (session, claimed) = self
			.server
			.database
			.run(|transaction| {
				let state = state.to_owned();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
								select claimed_at, session, expires_at
								from oauth_states
								where state = {p}1 and provider = 'github';
						"
					);
					let row = transaction
						.query_optional_into::<Row>(statement.into(), db::params![state])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					let Some(row) = row else {
						return Err(tg::error!("invalid OAuth state").into());
					};
					let now = time::OffsetDateTime::now_utc().unix_timestamp();
					if now > row.expires_at {
						return Err(tg::error!("the login has expired").into());
					}
					if row.claimed_at.is_some() {
						return Ok(ControlFlow::Break((row.session, false)));
					}
					let statement = formatdoc!(
						"
							update oauth_states
							set claimed_at = {p}1
							where state = {p}2 and provider = 'github' and claimed_at is null;
						"
					);
					let n = transaction
						.execute(statement.into(), db::params![now, state])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					Ok::<_, crate::database::Error>(ControlFlow::Break((row.session, n == 1)))
				}
				.boxed()
			})
			.await?;
		Ok((session, claimed))
	}

	async fn wait_for_oauth_session_to_succeed(&self, session: &str) -> tg::Result<bool> {
		for _ in 0..20 {
			if self.oauth_session_is_succeeded(session).await? {
				return Ok(true);
			}
			tokio::time::sleep(std::time::Duration::from_millis(500)).await;
		}
		Ok(false)
	}

	async fn oauth_session_is_succeeded(&self, session: &str) -> tg::Result<bool> {
		#[derive(db::row::Deserialize)]
		struct Row {
			status: String,
		}
		let row = self
			.server
			.database
			.run(|transaction| {
				let session = session.to_owned();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
								select status
								from oauth_sessions
								where id = {p}1;
							"
					);
					let row = transaction
						.query_optional_into::<Row>(statement.into(), db::params![session])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(row))
				}
				.boxed()
			})
			.await?;
		Ok(row.is_some_and(|row| row.status == "succeeded"))
	}

	async fn complete_github_login_with_code(
		&self,
		github: Github,
		code: String,
		session: String,
	) -> tg::Result<()> {
		let result = self
			.complete_github_login_with_code_inner(github, code, session.clone())
			.await;
		if let Err(error) = &result {
			self.fail_oauth_session(&session, error.to_string()).await?;
		}
		result
	}

	async fn complete_github_login_with_code_inner(
		&self,
		github: Github,
		code: String,
		session: String,
	) -> tg::Result<()> {
		let oauth = github_oauth_client(&github)?;
		let http_client = oauth2::reqwest::ClientBuilder::new()
			.redirect(oauth2::reqwest::redirect::Policy::none())
			.build()
			.map_err(|error| tg::error!(!error, "failed to build the HTTP client"))?;
		let http_client_ = http_client.clone();
		let token = oauth
			.exchange_code(AuthorizationCode::new(code))
			.request_async(&move |request: oauth2::HttpRequest| {
				let http_client = http_client_.clone();
				async move {
					let body = String::from_utf8_lossy(request.body());
					tracing::info!(
						uri = %request.uri(),
						body = %redact_oauth_body(&body),
						"exchanging GitHub authorization code"
					);
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
					tracing::info!(
						status = %response.status(),
						"received GitHub token response"
					);
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
				oauth2::RequestTokenError::Parse(error, body) => {
					let body = String::from_utf8_lossy(&body);
					let github_error = serde_json::from_str::<serde_json::Value>(&body)
						.ok()
						.and_then(|body| {
							let error = body
								.get("error")
								.and_then(serde_json::Value::as_str)
								.map(str::to_owned);
							let description = body
								.get("error_description")
								.and_then(serde_json::Value::as_str)
								.map(str::to_owned);
							error.map(|error| (error, description))
						});
					if let Some((error_, description)) = github_error {
						tg::error!(
							!error,
							error = %error_,
							description = ?description,
							"failed to exchange the authorization code"
						)
					} else {
						tg::error!(
							!error,
							body = %body,
							"failed to exchange the authorization code"
						)
					}
				},
				error => tg::error!(!error, "failed to exchange the authorization code"),
			})?;
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
		let crab = octocrab::Octocrab::builder()
			.personal_token(access_token.clone())
			.build()
			.map_err(|error| tg::error!(!error, "failed to build the GitHub client"))?;
		let github_user = crab
			.current()
			.user()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the GitHub user"))?;
		let emails = crab
			.users("current_user")
			.emails()
			.list()
			.await
			.map(|page| {
				page.items
					.into_iter()
					.filter(|email| email.verified)
					.map(|email| email.email)
					.collect::<Vec<_>>()
			})
			.unwrap_or_default();
		let token = self
			.complete_github_login_with_transaction(GithubLoginCompletion {
				session,
				github_user,
				emails,
				access_token,
				refresh_token,
				token_type: "Bearer".to_owned(),
				scope,
				expires_at,
				refresh_token_expires_at: None,
			})
			.await?;
		tracing::debug!(token = %token, "completed OAuth device login");
		Ok(())
	}

	async fn fail_oauth_session(&self, session: &str, error: String) -> tg::Result<()> {
		self.server
			.database
			.run(|transaction| {
				let session = session.to_owned();
				let error = error.clone();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
							update oauth_sessions
							set status = 'failed', error = {p}1
							where id = {p}2 and status = 'started';
						"
					);
					transaction
						.execute(statement.into(), db::params![error, session])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await?;
		Ok(())
	}

	async fn complete_github_login_with_transaction(
		&self,
		completion: GithubLoginCompletion,
	) -> tg::Result<String> {
		let session = self.clone();
		let (token, batch) = self
			.server
			.database
			.run(|transaction| {
				let session = session.clone();
				let completion = completion.clone();
				async move {
					let mut batch = tangram_index::batch::Arg::default();
					#[derive(db::row::Deserialize)]
						struct SessionRow {
							name: Option<String>,
						}
					let p = transaction.p();
					let statement = formatdoc!(
							"
								select name
								from oauth_sessions
								where id = {p}1;
							"
						);
						let oauth_session = transaction
							.query_one_into::<SessionRow>(
								statement.into(),
								db::params![completion.session.clone()],
							)
							.await
							.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
						let name = oauth_session
							.name
						.as_deref()
						.map(str::parse)
						.transpose()?;
					let user = session
						.upsert_github_user_with_transaction(
							transaction,
							&completion.github_user,
							&completion.emails,
							name,
							&mut batch,
						)
						.await?;
					let token = Self::create_user_token();
					let p = transaction.p();
					let statement = formatdoc!(
						r#"
							insert into user_tokens (id, "user")
							values ({p}1, {p}2);
						"#
					);
					transaction
						.execute(
							statement.into(),
							db::params![token.clone(), user.id.to_string()],
						)
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					let statement = formatdoc!(
							"
								update oauth_sessions
								set status = 'succeeded', \"user\" = {p}1, access_token = {p}2
								where id = {p}3;
							"
						);
					transaction
						.execute(
							statement.into(),
							db::params![
									user.id.to_string(),
									token.clone(),
									completion.session
								],
						)
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
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
								user.id.to_string(),
								completion.github_user.id.to_string(),
								completion.github_user.login,
								completion.github_user.name,
								completion.github_user.email,
								completion.github_user.avatar_url.to_string(),
								completion.github_user.html_url.to_string(),
								completion.access_token,
								completion.refresh_token,
								completion.token_type,
								completion.scope,
								completion.expires_at,
								completion.refresh_token_expires_at,
								now,
							],
						)
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					Ok::<_, crate::database::Error>(ControlFlow::Break((token, batch)))
				}
				.boxed()
			})
			.await?;
		if !batch.is_empty() {
			self.server
				.index
				.batch(batch)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the user"))?;
		}
		Ok(token)
	}

	async fn upsert_github_user_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		github_user: &octocrab::models::Author,
		emails: &[String],
		name: Option<tg::Specifier>,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<tg::User> {
		let subject = github_user.id.to_string();
		#[derive(db::row::Deserialize)]
		struct IdentityRow {
			user: String,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select "user"
				from user_identities
				where provider = 'github' and subject = {p}1;
			"#
		);
		let row = transaction
			.query_optional_into::<IdentityRow>(statement.into(), db::params![subject.clone()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let user = if let Some(row) = row {
			let id = row.user.parse::<tg::user::Id>()?;
			let node = Self::try_get_node_by_id_with_transaction(transaction, &id.clone().into())
				.await?
				.ok_or_else(|| tg::error!("invalid GitHub identity"))?;
			Self::user_from_node_with_transaction(transaction, node).await?
		} else {
			let specifier = name.unwrap_or(github_user.login.parse::<tg::Specifier>()?);
			if specifier.components().count() != 1 {
				return Err(tg::error!("invalid user specifier"));
			}
			let user = if let Some(node) =
				Self::try_get_node_by_specifier_with_transaction(transaction, &specifier).await?
			{
				if node.kind != tg::id::Kind::User {
					return Err(tg::error!("specifier is already in use"));
				}
				Self::user_from_node_with_transaction(transaction, node).await?
			} else {
				let id = tg::user::Id::new();
				let node = Self::create_node_with_transaction(
					transaction,
					&id.clone().into(),
					tg::id::Kind::User,
					&specifier,
					None,
				)
				.await?;
				let statement = formatdoc!(
					"
						insert into users (id, name)
						values ({p}1, {p}2);
					"
				);
				transaction
					.execute(
						statement.into(),
						db::params![id.to_string(), node.name.clone()],
					)
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
				batch.put_users.push(tangram_index::user::put::Arg {
					id: id.clone(),
					specifier: node.specifier.clone(),
				});
				Self::user_from_node_with_transaction(transaction, node).await?
			};
			let statement = formatdoc!(
				r#"
					insert into user_identities (provider, subject, "user")
					values ('github', {p}1, {p}2);
				"#
			);
			transaction
				.execute(statement.into(), db::params![subject, user.id.to_string()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			user
		};
		for email in emails {
			let statement = formatdoc!(
				r#"
					insert into user_emails ("user", email)
					values ({p}1, {p}2)
					on conflict ("user", email) do nothing;
				"#
			);
			transaction
				.execute(statement.into(), db::params![user.id.to_string(), email])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		let node = Self::try_get_node_by_id_with_transaction(transaction, &user.id.clone().into())
			.await?
			.unwrap();
		Self::user_from_node_with_transaction(transaction, node).await
	}
}

fn github_oauth_client(
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
	Ok(BasicClient::new(ClientId::new(github.client_id.clone()))
		.set_client_secret(ClientSecret::new(github.client_secret.clone()))
		.set_auth_uri(
			AuthUrl::new(github.auth_url.clone())
				.map_err(|error| tg::error!(!error, "invalid GitHub auth URL"))?,
		)
		.set_token_uri(
			TokenUrl::new(github.token_url.clone())
				.map_err(|error| tg::error!(!error, "invalid GitHub token URL"))?,
		)
		.set_redirect_uri(
			RedirectUrl::new(github.redirect_url.clone())
				.map_err(|error| tg::error!(!error, "invalid GitHub redirect URL"))?,
		)
		.set_auth_type(AuthType::RequestBody))
}

fn request_origin(request: &http::Request<BoxBody>, fallback: Option<&str>) -> tg::Result<String> {
	if let Some(host) = request
		.headers()
		.get(http::header::HOST)
		.and_then(|host| host.to_str().ok())
	{
		let scheme = request
			.headers()
			.get("x-forwarded-proto")
			.and_then(|scheme| scheme.to_str().ok())
			.unwrap_or("http");
		return Ok(format!("{scheme}://{host}"));
	}
	if let Some(scheme) = request.uri().scheme_str()
		&& let Some(authority) = request.uri().authority()
	{
		return Ok(format!("{scheme}://{authority}"));
	}
	fallback
		.map(str::to_owned)
		.ok_or_else(|| tg::error!("missing host header"))
}

fn uri_origin(uri: &tangram_uri::Uri) -> Option<String> {
	let scheme = uri.scheme()?;
	let authority = uri.authority()?;
	Some(format!("{scheme}://{authority}"))
}

fn create_user_code() -> String {
	let bytes = uuid::Uuid::now_v7().into_bytes();
	let encoded = tg::id::ENCODING.encode(&bytes);
	format!("{}-{}", &encoded[..4], &encoded[4..8]).to_ascii_uppercase()
}

fn parse_form(input: &str) -> tg::Result<BTreeMap<String, String>> {
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

fn parse_query(input: &str) -> tg::Result<BTreeMap<String, String>> {
	parse_form(input)
}

fn redact_oauth_body(body: &str) -> String {
	parse_form(body).map_or_else(
		|_| "<invalid form body>".to_owned(),
		|form| {
			form.into_iter()
				.map(|(key, value)| {
					let value = if matches!(key.as_str(), "client_secret" | "code") {
						"<redacted>".to_owned()
					} else {
						value
					};
					let key = tangram_uri::encode_query_value(&key);
					let value = tangram_uri::encode_query_value(&value);
					format!("{key}={value}")
				})
				.collect::<Vec<_>>()
				.join("&")
		},
	)
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

#[derive(Clone)]
struct GithubLoginCompletion {
	session: String,
	github_user: octocrab::models::Author,
	emails: Vec<String>,
	access_token: String,
	refresh_token: Option<String>,
	token_type: String,
	scope: String,
	expires_at: Option<i64>,
	refresh_token_expires_at: Option<i64>,
}
