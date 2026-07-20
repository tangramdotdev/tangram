use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _, response::Ext as _},
};

impl Session {
	pub(crate) async fn create_login(
		&self,
		arg: tg::user::login::create::Arg,
	) -> tg::Result<tg::user::login::create::Output> {
		// Get the location.
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;

		// Start the login.
		match location {
			tg::Location::Local(_) => self.create_login_local(arg).await,
			tg::Location::Remote(remote) => {
				let client = self.get_remote_session(&remote.name).await?;
				let arg = tg::user::login::create::Arg {
					location: Some(tg::Location::Local(tg::location::Local::default()).into()),
					..arg
				};
				client.create_login(arg).await
			},
		}
	}

	async fn create_login_local(
		&self,
		arg: tg::user::login::create::Arg,
	) -> tg::Result<tg::user::login::create::Output> {
		// Get the authentication config.
		let authentication = self
			.server
			.config()
			.authentication
			.users
			.as_ref()
			.ok_or_else(|| tg::error!("no authentication providers are configured"))?;

		// Create the code.
		let code = super::create_code();

		// Start the GitHub login if necessary.
		if authentication.providers.insecure.is_none() {
			if authentication.providers.github.is_some() {
				return self.create_login_with_github(arg, code).await;
			}
			return Err(tg::error!("no authentication providers are configured"));
		}

		// Get the expiration time.
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let ttl = authentication.ttl.as_secs();
		let expires_at = now + i64::try_from(ttl).unwrap_or(i64::MAX);

		// Get the specifier.
		let name = arg
			.name
			.ok_or_else(|| tg::error!("invalid user specifier"))?;
		if name.components().count() != 1 {
			return Err(tg::error!("invalid user specifier"));
		}

		// Create the login.
		let email = arg.email.clone();
		self.server
			.database
			.run(|transaction| {
				let code = code.clone();
				let email = email.clone();
				let name = name.clone();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
							insert into logins (
								code, provider, status, name, email, expires_at, interval, created_at, updated_at
							)
							values ({p}1, 'insecure', 'started', {p}2, {p}3, {p}4, 0, {p}5, {p}5);
						"
					);
					transaction
						.execute(
							statement.into(),
							db::params![code, name.to_string(), email, expires_at, now],
						)
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await?;

		// Finish the login.
		let emails = arg.email.into_iter().collect();
		self.finish_login(super::FinishLoginArg {
			code: code.clone(),
			default_specifier: name,
			emails,
			identity: None,
		})
		.await?;

		// Create the output.
		let output = tg::user::login::create::Output { code, url: None };

		Ok(output)
	}

	async fn create_login_with_github(
		&self,
		arg: tg::user::login::create::Arg,
		code: String,
	) -> tg::Result<tg::user::login::create::Output> {
		// Get the authentication config.
		let authentication = self
			.server
			.config()
			.authentication
			.users
			.as_ref()
			.ok_or_else(|| tg::error!("no authentication providers are configured"))?;

		// Get the login URL.
		let github =
			authentication.providers.github.as_ref().ok_or_else(|| {
				tg::error!("the GitHub authentication provider is not configured")
			})?;
		let server_url = server_url_from_redirect_url(&github.redirect_url)?;
		let web_url = authentication
			.web_url
			.as_deref()
			.ok_or_else(|| tg::error!("missing login web URL"))?
			.trim_end_matches('/')
			.to_owned();

		// Get the expiration time.
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let ttl = authentication.ttl.as_secs();
		let expires_at = now + i64::try_from(ttl).unwrap_or(i64::MAX);
		let interval = i64::try_from(authentication.interval.as_secs()).unwrap_or(i64::MAX);

		// Create the login.
		self.server
			.database
			.run(|transaction| {
				let email = arg.email.clone();
				let code = code.clone();
				let name = arg.name.clone().map(|name| name.to_string());
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
							insert into logins (
								code, provider, status, name, email, expires_at, interval, created_at, updated_at
							)
							values ({p}1, 'github', 'started', {p}2, {p}3, {p}4, {p}5, {p}6, {p}6);
						"
					);
					transaction
						.execute(
							statement.into(),
							db::params![code, name, email, expires_at, interval, now],
						)
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await?;

		// Create the output.
		let code_query = tangram_uri::encode_query_value(&code);
		let url_query = tangram_uri::encode_query_value(&server_url);
		let url = format!("{web_url}/login?url={url_query}&code={code_query}");
		let output = tg::user::login::create::Output {
			code,
			url: Some(url),
		};

		Ok(output)
	}

	pub(crate) async fn create_login_request(
		&self,
		request: http::Request<BoxBody>,
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

		// Create the login.
		let output = self.create_login(arg).await?;

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap().boxed_body();

		Ok(response)
	}
}

fn server_url_from_redirect_url(redirect_url: &str) -> tg::Result<String> {
	// Get the redirect URI.
	let uri = tangram_uri::Uri::parse(redirect_url)
		.map_err(|error| tg::error!(!error, "invalid GitHub redirect URL"))?;

	// Get the server URL.
	let scheme = uri
		.scheme()
		.ok_or_else(|| tg::error!("invalid GitHub redirect URL"))?;
	let authority = uri
		.authority()
		.ok_or_else(|| tg::error!("invalid GitHub redirect URL"))?;
	let url = format!("{scheme}://{authority}");

	Ok(url)
}
