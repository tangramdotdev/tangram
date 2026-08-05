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
	pub(crate) async fn try_delete_runner_token(
		&self,
		runner: &tg::runner::Id,
		token: &tg::token::Id,
		_arg: tg::runner::token::delete::Arg,
	) -> tg::Result<Option<()>> {
		self.get_authorized_runner(runner).await?;
		let runner = runner.clone();
		let token = token.clone();
		let deleted = self
			.server
			.database
			.run(|transaction| {
				let runner = runner.clone();
				let token = token.clone();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
							delete from runner_tokens
							where id = {p}1 and runner = {p}2;
						"
					);
					let count = transaction
						.execute(
							statement.into(),
							db::params![token.to_string(), runner.to_string()],
						)
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

					Ok::<_, crate::database::Error>(ControlFlow::Break(count == 1))
				}
				.boxed()
			})
			.await?;

		Ok(deleted.then_some(()))
	}

	pub(crate) async fn try_delete_runner_token_request(
		&self,
		_request: http::Request<BoxBody>,
		runner: &str,
		token: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let runner = runner
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the runner ID"))?;
		let token = token
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the token ID"))?;
		let output = self
			.try_delete_runner_token(&runner, &token, tg::runner::token::delete::Arg::default())
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
