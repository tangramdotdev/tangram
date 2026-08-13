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
	pub(crate) async fn create_runner_token(
		&self,
		runner: &tg::runner::Id,
		_arg: tg::runner::token::create::Arg,
	) -> tg::Result<tg::runner::token::create::Output> {
		self.get_authorized_runner(runner).await?;
		let created_at = self.server.clock.unix_timestamp()?;
		let (id, token) = crate::token::create();
		let token_hash = crate::token::hash(&token);
		let runner = runner.clone();
		self.server
			.database
			.run(|transaction| {
				let id = id.clone();
				let runner = runner.clone();
				let token_hash = token_hash.clone();
				async move {
					Self::create_runner_token_with_transaction(
						transaction,
						created_at,
						&id,
						&runner,
						&token_hash,
					)
					.await
				}
				.boxed()
			})
			.await?;
		let data = tg::runner::token::Data { created_at, id };

		Ok(tg::runner::token::create::Output { data, token })
	}

	async fn create_runner_token_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		created_at: i64,
		id: &tg::token::Id,
		runner: &tg::runner::Id,
		token_hash: &str,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into runner_tokens (created_at, id, runner, token)
				values ({p}1, {p}2, {p}3, {p}4);
			"
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![created_at, id.to_string(), runner.to_string(), token_hash],
			)
			.await;
		crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn create_runner_token_request(
		&self,
		request: http::Request<BoxBody>,
		runner: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let runner = runner
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the runner ID"))?;
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let output = self.create_runner_token(&runner, arg).await?;
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
