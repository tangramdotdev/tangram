use {
	crate::Session,
	futures::FutureExt as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, response::Ext as _, response::builder::Ext as _},
};

impl Session {
	pub(crate) async fn try_delete_runner(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::delete::Arg,
	) -> tg::Result<Option<()>> {
		if !self.server.is_primary_region() {
			return self.try_delete_runner_primary_region(runner, arg).await;
		}
		let Some(data) = self.try_get_runner_data(runner).await? else {
			return Ok(None);
		};
		let owner = data.owner.as_ref().and_then(tg::Principal::to_id);
		self.authorize_runner_owner(owner.as_ref()).await?;
		let runner = runner.clone();
		self.server
			.database
			.run(|transaction| {
				let runner = runner.clone();
				async move { Self::delete_runner_with_transaction(transaction, &runner).await }
					.boxed()
			})
			.await?;

		Ok(Some(()))
	}

	async fn try_delete_runner_primary_region(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::delete::Arg,
	) -> tg::Result<Option<()>> {
		let client = self
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		let output = client
			.try_delete_runner(runner, arg)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to delete the runner in the primary region")
			})?;

		Ok(output)
	}

	async fn delete_runner_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		runner: &tg::runner::Id,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let p = transaction.p();
		let statement = format!("delete from runner_tokens where runner = {p}1;");
		let result = transaction
			.execute(statement.into(), db::params![runner.to_string()])
			.await;
		crate::database::retry!(result, "failed to execute the statement");
		let statement = format!("delete from runners where id = {p}1;");
		let result = transaction
			.execute(statement.into(), db::params![runner.to_string()])
			.await;
		crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn try_delete_runner_request(
		&self,
		_request: http::Request<BoxBody>,
		runner: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let runner = runner
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the runner ID"))?;
		let output = self
			.try_delete_runner(&runner, tg::runner::delete::Arg::default())
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
