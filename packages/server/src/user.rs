use {
	crate::{Context, Server},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::request::Ext as _,
};

impl Server {
	pub(crate) async fn get_user_with_context(
		&self,
		context: &Context,
		token: &str,
	) -> tg::Result<Option<tg::user::User>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the user for the token.
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::user::Id,
			email: String,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select users.id, users.email
				from users
				join tokens on tokens.user = users.id
				where tokens.token = {p}1;
			"
		);
		let params = db::params![token];
		let user = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| tg::user::User {
				id: row.id,
				email: row.email,
			});

		// Drop the database connection.
		drop(connection);

		Ok(user)
	}

	pub(crate) async fn authorize(&self, context: &Context) -> tg::Result<Option<tg::User>> {
		let Some(user) = self
			.try_authorize(context)
			.await?
			.ok_or_else(|| tg::error!("failed to authorize"))?
		else {
			return Ok(None);
		};
		Ok(Some(user))
	}

	pub(crate) async fn try_authorize(
		&self,
		context: &Context,
	) -> tg::Result<Option<Option<tg::User>>> {
		if !self.config().authorization {
			return Ok(Some(None));
		}
		let Some(token) = context.token.as_ref() else {
			return Ok(None);
		};
		let Some(user) = self
			.get_user_with_context(context, token)
			.await
			.map_err(|source| tg::error!(!source, "failed to get user"))?
		else {
			return Ok(None);
		};
		Ok(Some(Some(user)))
	}

	pub(crate) async fn handle_get_user_request(
		&self,
		request: tangram_http::Request,
		context: &Context,
	) -> tg::Result<tangram_http::Response> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the token.
		let Some(token) = request.token(None) else {
			let response = http::Response::builder()
				.status(http::StatusCode::UNAUTHORIZED)
				.body(tangram_http::body::Boxed::empty())
				.unwrap();
			return Ok(response);
		};

		// Get the user.
		let Some(output) = self.get_user_with_context(context, token).await? else {
			let response = http::Response::builder()
				.status(http::StatusCode::UNAUTHORIZED)
				.body(tangram_http::body::Boxed::empty())
				.unwrap();
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
				(
					Some(content_type),
					tangram_http::body::Boxed::with_bytes(body),
				)
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
