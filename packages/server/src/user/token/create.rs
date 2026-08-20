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
	pub(crate) async fn create_user_token(
		&self,
		arg: tg::user::token::create::Arg,
	) -> tg::Result<tg::user::token::create::Output> {
		if !self.server.is_primary_region() {
			return self.create_user_token_primary_region(arg).await;
		}
		let user = self.authenticated_user()?.clone();
		let (id, token) = crate::token::create();
		let created_at = self.server.clock.unix_timestamp()?;
		let token_hash = crate::token::hash(&token);
		self.server
			.database
			.run(|transaction| {
				let id = id.clone();
				let token_hash = token_hash.clone();
				let user = user.clone();
				async move {
					Self::create_user_token_with_transaction(
						transaction,
						created_at,
						&id,
						&token_hash,
						&user,
					)
					.await
				}
				.boxed()
			})
			.await?;
		let data = tg::user::token::Data { created_at, id };
		let output = tg::user::token::create::Output { data, token };

		Ok(output)
	}

	async fn create_user_token_primary_region(
		&self,
		arg: tg::user::token::create::Arg,
	) -> tg::Result<tg::user::token::create::Output> {
		let client = self
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		let output = client.create_user_token(arg).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to create the user token in the primary region"
			)
		})?;

		Ok(output)
	}

	async fn create_user_token_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		created_at: i64,
		id: &tg::token::Id,
		token_hash: &str,
		user: &tg::user::Id,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				insert into user_tokens (created_at, id, token, "user")
				values ({p}1, {p}2, {p}3, {p}4);
			"#
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![created_at, id.to_string(), token_hash, user.to_string()],
			)
			.await;
		crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn create_user_token_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let output = self.create_user_token(arg).await?;
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
}
