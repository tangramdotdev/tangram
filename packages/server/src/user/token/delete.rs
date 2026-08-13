use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, response::Ext as _, response::builder::Ext as _},
};

impl Session {
	pub(crate) async fn try_delete_user_token(
		&self,
		token: &tg::token::Id,
		_arg: tg::user::token::delete::Arg,
	) -> tg::Result<Option<()>> {
		let user = self.authenticated_user()?.clone();
		let token = token.clone();
		let deleted = self
			.server
			.database
			.run(|transaction| {
				let token = token.clone();
				let user = user.clone();
				async move {
					Self::delete_user_token_with_transaction(transaction, &token, &user).await
				}
				.boxed()
			})
			.await?;

		Ok(deleted.then_some(()))
	}

	async fn delete_user_token_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		token: &tg::token::Id,
		user: &tg::user::Id,
	) -> tg::Result<ControlFlow<bool, crate::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				delete from user_tokens
				where id = {p}1 and "user" = {p}2;
			"#
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![token.to_string(), user.to_string()],
			)
			.await;
		let count = crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(count == 1))
	}

	pub(crate) async fn try_delete_user_token_request(
		&self,
		_request: http::Request<BoxBody>,
		token: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let token = token
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the token ID"))?;
		let output = self
			.try_delete_user_token(&token, tg::user::token::delete::Arg::default())
			.await?;
		let response = if output.is_some() {
			http::Response::builder().empty().unwrap().boxed_body()
		} else {
			http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body()
		};

		Ok(response)
	}
}
