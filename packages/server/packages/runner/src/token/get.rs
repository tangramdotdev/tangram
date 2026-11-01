use {
	crate::Component,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_server_core::Session,
};

#[derive(db::row::Deserialize)]
struct Row {
	created_at: i64,
}

impl Component {
	pub(crate) async fn try_get_runner_token(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		token: &tg::token::Id,
		mut arg: tg::runner::token::get::Arg,
	) -> tg::Result<Option<tg::runner::token::get::Output>> {
		let location = session.server.runner_location(arg.location.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				let client = session.get_region_session(&region).await?;
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				client.try_get_runner_token(runner, token, arg).await
			},
			tg::Location::Local(_) => {
				self.try_get_runner_token_local(session, runner, token)
					.await
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
						region: remote.region.clone(),
					})
					.into(),
				);
				let location = tg::Location::Remote(tg::location::Remote {
					name: remote.name.clone(),
					region: remote.region,
				});
				let output =
					client
						.try_get_runner_token(runner, token, arg)
						.await?
						.map(|mut output| {
							output.location = Some(location);
							output
						});
				Ok(output)
			},
		}
	}

	async fn try_get_runner_token_local(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		token: &tg::token::Id,
	) -> tg::Result<Option<tg::runner::token::get::Output>> {
		// Get the runner and the token.
		let output = session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let runner = runner.clone();
				let token = token.clone();
				async move {
					Self::try_get_runner_token_with_transaction(transaction, &runner, &token).await
				}
				.boxed()
			})
			.await?;
		let Some((data, row)) = output else {
			return Ok(None);
		};

		// Treat a runner the session may not manage as absent.
		let owner = data.owner.as_ref().and_then(tg::Principal::to_id);
		let authorized = self
			.is_runner_owner_authorized(session, owner.as_ref())
			.await?;
		if !authorized {
			return Ok(None);
		}
		let Some(row) = row else {
			return Ok(None);
		};

		// Create the output without the token's secret.
		let data = tg::runner::token::Data {
			created_at: row.created_at,
			id: token.clone(),
			token: None,
		};
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let output = tg::runner::token::get::Output {
			data,
			location: Some(location),
		};

		Ok(Some(output))
	}

	async fn try_get_runner_token_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		runner: &tg::runner::Id,
		token: &tg::token::Id,
	) -> tg::Result<
		ControlFlow<Option<(tg::runner::Data, Option<Row>)>, tangram_server_core::database::Error>,
	> {
		// Get the runner.
		let data = match Self::try_get_runner_data_with_transaction(transaction, runner).await? {
			ControlFlow::Break(data) => data,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let Some(data) = data else {
			return Ok(ControlFlow::Break(None));
		};

		// Get the token.
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select created_at
				from runner_tokens
				where "runner" = {p}1 and id = {p}2;
			"#
		);
		let result = transaction
			.query_optional_into::<Row>(
				statement.into(),
				db::params![runner.to_string(), token.to_string()],
			)
			.await;
		let row = tangram_server_core::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(Some((data, row))))
	}

	pub(crate) async fn try_get_runner_token_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		runner: &str,
		token: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let runner = runner
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the runner ID"))?;
		let token = token
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the token ID"))?;
		let Some(output) = self
			.try_get_runner_token(session, &runner, &token, arg)
			.await?
		else {
			let response = http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
		};
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
