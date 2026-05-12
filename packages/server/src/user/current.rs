use {
	crate::Session,
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn get_current_user(
		&self,
		arg: tg::user::current::Arg,
	) -> tg::Result<Option<tg::user::User>> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => {
				let Some(token) = self.context.token.as_deref() else {
					return Ok(None);
				};
				self.get_current_user_local(token).await
			},
			tg::Location::Remote(remote) => self.get_current_user_remote(remote).await,
		}
	}

	pub(crate) async fn get_current_user_local(
		&self,
		token: &str,
	) -> tg::Result<Option<tg::user::User>> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Get a database connection.
		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;

		// Get the user ID for the token.
		#[derive(db::row::Deserialize)]
		struct UserRow {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::user::Id,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select users.id
				from users
				join tokens on tokens.\"user\" = users.id
				where tokens.id = {p}1;
			"
		);
		let params = db::params![token];
		let user = connection
			.query_optional_into::<UserRow>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(user) = user else {
			return Ok(None);
		};

		// Get the user's emails.
		#[derive(db::row::Deserialize)]
		struct EmailRow {
			email: String,
		}
		let statement = formatdoc!(
			"
				select email
				from user_emails
				where user_emails.\"user\" = {p}1
				order by email;
			"
		);
		let params = db::params![user.id.to_string()];
		let rows = connection
			.query_all_into::<EmailRow>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let emails = rows.into_iter().map(|row| row.email).collect();
		let user = tg::User {
			id: user.id,
			emails,
			location: None,
		};

		// Drop the database connection.
		drop(connection);

		Ok(Some(user))
	}

	async fn get_current_user_remote(
		&self,
		remote: tg::location::Remote,
	) -> tg::Result<Option<tg::user::User>> {
		let client = self
			.get_remote_session(remote.name.clone())
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					remote = %remote.name,
					"failed to get the remote client"
				)
			})?;
		let arg = tg::user::current::Arg {
			location: Some(tg::Location::Local(tg::location::Local::default()).into()),
		};
		let mut user = client.get_current_user(arg).await.map_err(|error| {
			tg::error!(
				!error,
				remote = %remote.name,
				"failed to get the current user"
			)
		})?;
		if let Some(user) = &mut user {
			user.location = Some(tg::Location::Remote(tg::location::Remote {
				name: remote.name,
				region: None,
			}));
		}
		Ok(user)
	}

	pub(crate) async fn get_current_user_request(
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
			.unwrap_or_default();

		// Get the current user.
		let Some(output) = self.get_current_user(arg).await? else {
			let response = http::Response::builder()
				.status(http::StatusCode::UNAUTHORIZED)
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
		};

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
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
