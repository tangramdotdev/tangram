use {
	crate::Component,
	futures::FutureExt as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_server_core::Session,
};

impl Component {
	pub(crate) async fn try_delete_runner(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		mut arg: tg::runner::delete::Arg,
	) -> tg::Result<Option<()>> {
		let location = session.server.runner_location(arg.location.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				let client = session.get_region_session(&region).await?;
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				client.try_delete_runner(runner, arg).await
			},
			tg::Location::Local(_) => {
				if !session.server.config.runners {
					return Err(tg::error!("local runner management is disabled"));
				}
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				self.try_delete_runner_local(session, runner, arg).await
			},
			tg::Location::Remote(remote) => {
				let client = session
					.server
					.components
					.remote
					.get_remote_session(session, &remote.name)
					.await?;
				arg.location = Some(
					tg::Location::Local(tg::location::Local {
						region: remote.region,
					})
					.into(),
				);
				client.try_delete_runner(runner, arg).await
			},
		}
	}

	async fn try_delete_runner_local(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		arg: tg::runner::delete::Arg,
	) -> tg::Result<Option<()>> {
		if !session.server.is_primary_region() {
			return self
				.try_delete_runner_primary_region(session, runner, arg)
				.await;
		}
		let Some(data) = self.try_get_runner_data(session, runner).await? else {
			return Ok(None);
		};
		let owner = data.owner.as_ref().and_then(tg::Principal::to_id);
		self.authorize_runner_owner(session, owner.as_ref()).await?;
		let runner = runner.clone();
		session
			.server
			.database
			.run(|transaction| {
				let runner = runner.clone();
				async move { Self::delete_runner_with_transaction(transaction, &runner).await }
					.boxed()
			})
			.await?;

		session
			.server
			.components
			.authentication
			.invalidate_authentication(
				&session.server,
				tangram_server_core::authentication::cache::Invalidation::Principal(
					tg::Principal::Runner(runner),
				),
			)
			.await;

		Ok(Some(()))
	}

	async fn try_delete_runner_primary_region(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		arg: tg::runner::delete::Arg,
	) -> tg::Result<Option<()>> {
		let client = session
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
		transaction: &tangram_server_core::database::Transaction<'_>,
		runner: &tg::runner::Id,
	) -> tg::Result<ControlFlow<(), tangram_server_core::database::Error>> {
		let p = transaction.p();
		let statement = format!("delete from runner_tokens where runner = {p}1;");
		let result = transaction
			.execute(statement.into(), db::params![runner.to_string()])
			.await;
		tangram_server_core::database::retry!(result, "failed to execute the statement");
		let statement = format!("delete from runners where id = {p}1;");
		let result = transaction
			.execute(statement.into(), db::params![runner.to_string()])
			.await;
		tangram_server_core::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn try_delete_runner_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		runner: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let runner = runner
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the runner ID"))?;
		let output = self.try_delete_runner(session, &runner, arg).await?;
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
