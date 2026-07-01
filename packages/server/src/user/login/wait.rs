use {
	crate::Session,
	indoc::formatdoc,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _, response::Ext as _},
};

impl Session {
	pub(crate) async fn wait_login(
		&self,
		arg: tg::user::login::wait::Arg,
	) -> tg::Result<tg::user::login::wait::Output> {
		// Get the location.
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;

		// Await the login.
		match location {
			tg::Location::Local(_) => self.wait_login_local(&arg.code).await,
			tg::Location::Remote(remote) => {
				let client = self.get_remote_session(&remote.name).await?;
				let arg = tg::user::login::wait::Arg {
					code: arg.code,
					location: Some(tg::Location::Local(tg::location::Local::default()).into()),
				};
				client.wait_login(arg).await
			},
		}
	}

	async fn wait_login_local(&self, code: &str) -> tg::Result<tg::user::login::wait::Output> {
		// Await the login.
		loop {
			match self.try_wait_login_local(code).await? {
				WaitLogin::Pending {
					expires_at,
					interval,
				} => {
					let now = time::OffsetDateTime::now_utc().unix_timestamp();
					if now > expires_at {
						return Err(tg::error!("the login has expired"));
					}
					tokio::time::sleep(Duration::from_secs(interval.max(1))).await;
				},
				WaitLogin::Output(output) => {
					return Ok(output);
				},
			}
		}
	}

	async fn try_wait_login_local(&self, code: &str) -> tg::Result<WaitLogin> {
		// Get the login.
		#[derive(db::row::Deserialize)]
		struct Row {
			error: Option<String>,
			expires_at: i64,
			interval: i64,
			status: String,
			token: Option<String>,
			user: Option<String>,
		}
		let mut connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select error, expires_at, interval, status, token, "user"
				from logins
				where code = {p}1;
			"#
		);
		let row = transaction
			.query_optional_into::<Row>(statement.into(), db::params![code])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(row) = row else {
			return Err(tg::error!("invalid login code"));
		};

		// Check the expiration time.
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		if now > row.expires_at {
			return Err(tg::error!("the login has expired"));
		}

		// Create the output.
		let output = match row.status.as_str() {
			"started" => WaitLogin::Pending {
				expires_at: row.expires_at,
				interval: u64::try_from(row.interval).unwrap_or(1),
			},
			"finished" => {
				if let Some(error) = row.error {
					return Err(tg::error!("{error}"));
				}
				let token = row.token.ok_or_else(|| tg::error!("missing login token"))?;
				let user = row
					.user
					.ok_or_else(|| tg::error!("missing login user"))?
					.parse::<tg::user::Id>()?;
				let node = Self::try_get_node_by_id_with_transaction(&transaction, &user.into())
					.await?
					.ok_or_else(|| tg::error!("missing login user"))?;
				let user = Self::user_from_node_with_transaction(&transaction, node).await?;
				WaitLogin::Output(tg::user::login::wait::Output { token, user })
			},
			_ => return Err(tg::error!("invalid login status")),
		};
		Ok(output)
	}

	pub(crate) async fn wait_login_request(
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
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("missing query params"))?;

		// Await the login.
		let output = self.wait_login(arg).await?;

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

enum WaitLogin {
	Pending { expires_at: i64, interval: u64 },
	Output(tg::user::login::wait::Output),
}
