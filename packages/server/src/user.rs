use {
	crate::{Context, Server},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
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
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		// Get the token.
		let Some(token) = request.token(None) else {
			let response = http::Response::builder()
				.status(http::StatusCode::UNAUTHORIZED)
				.empty()
				.unwrap();
			return Ok(response);
		};

		// Get the user.
		let Some(output) = self.get_user_with_context(context, token).await? else {
			let response = http::Response::builder()
				.status(http::StatusCode::UNAUTHORIZED)
				.empty()
				.unwrap();
			return Ok(response);
		};

		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();

		Ok(response)
	}
}
