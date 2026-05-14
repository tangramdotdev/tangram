use {
	crate::{Context, Server, Session, context::Authentication},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, response::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub(crate) async fn authenticate_request(
		&self,
		context: &mut Context,
	) -> Option<http::Response<BoxBody>> {
		let token = context.token.clone()?;

		let session = self.session(context);
		match session.get_current_user_local(&token).await {
			Ok(Some(user)) => {
				context.authentication = Some(Authentication::User(user));
			},
			Ok(None) => (),
			Err(error) => {
				tracing::error!(error = %error.trace(), "failed to authenticate the request");
				return Some(authentication_error_response(&error));
			},
		}
		if context.authentication.is_none() {
			let session = self.session(context);
			match session
				.try_get_sandbox_token_authentication_local(&token)
				.await
			{
				Ok(Some(authentication)) => {
					context.authentication = Some(authentication);
				},
				Ok(None) => (),
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to authenticate the sandbox");
					return Some(authentication_error_response(&error));
				},
			}
		}
		if context.authentication.is_none() {
			let session = self.session(context);
			match session.has_runner_token_local(&token).await {
				Ok(true) => {
					context.authentication = Some(Authentication::Runner);
				},
				Ok(false) => (),
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to authenticate the runner");
					return Some(authentication_error_response(&error));
				},
			}
		}

		None
	}
}

impl Session {
	pub(crate) async fn has_runner_token_local(&self, token: &str) -> tg::Result<bool> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;

		#[derive(db::row::Deserialize)]
		struct Row {
			id: String,
		}

		let p = connection.p();
		let statement = formatdoc!(
			"
				select id
				from runner_tokens
				where id = {p}1;
			"
		);
		let params = db::params![token];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(row.is_some_and(|row| row.id == token))
	}

	pub(crate) async fn try_get_sandbox_token_authentication_local(
		&self,
		token: &str,
	) -> tg::Result<Option<Authentication>> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;

		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			sandbox: tg::sandbox::Id,
		}

		let p = connection.p();
		let statement = formatdoc!(
			"
				select sandbox
				from sandbox_tokens
				where token = {p}1;
			"
		);
		let params = db::params![token];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(row.map(|row| Authentication::Sandbox(row.sandbox)))
	}
}

fn authentication_error_response(error: &tg::Error) -> http::Response<BoxBody> {
	let bytes = match error.to_data_or_id() {
		tg::Either::Left(data) => serde_json::to_string(&data).unwrap(),
		tg::Either::Right(id) => id.to_string(),
	};
	http::Response::builder()
		.status(http::StatusCode::INTERNAL_SERVER_ERROR)
		.bytes(bytes)
		.unwrap()
		.boxed_body()
}
