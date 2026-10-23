use {
	crate::Component,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _, response::Ext as _},
	tangram_server_core::Session,
};

impl Component {
	pub(crate) async fn create_runner_token(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		mut arg: tg::runner::token::create::Arg,
	) -> tg::Result<tg::runner::token::create::Output> {
		let location = session.server.runner_location(arg.location.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				let client = session.get_region_session(&region).await?;
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				client.create_runner_token(runner, arg).await
			},
			tg::Location::Local(_) => {
				if !session.server.config.runners {
					return Err(tg::error!("local runner management is disabled"));
				}
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				self.create_runner_token_local(session, runner, arg).await
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
				client.create_runner_token(runner, arg).await
			},
		}
	}

	async fn create_runner_token_local(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		arg: tg::runner::token::create::Arg,
	) -> tg::Result<tg::runner::token::create::Output> {
		if !session.server.is_primary_region() {
			return self
				.create_runner_token_primary_region(session, runner, arg)
				.await;
		}
		self.get_authorized_runner(session, runner).await?;
		let created_at = session.server.clock.unix_timestamp();
		let (id, token) = tangram_server_core::token::create();
		let token_hash = tangram_server_core::token::hash(&token);
		let runner = runner.clone();
		session
			.server
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
		let data = tg::runner::token::Data {
			created_at,
			id,
			token: Some(token),
		};

		Ok(tg::runner::token::create::Output { data })
	}

	async fn create_runner_token_primary_region(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		arg: tg::runner::token::create::Arg,
	) -> tg::Result<tg::runner::token::create::Output> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		let output = client
			.create_runner_token(runner, arg)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					"failed to create the runner token in the primary region"
				)
			})?;

		Ok(output)
	}

	async fn create_runner_token_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		created_at: i64,
		id: &tg::token::Id,
		runner: &tg::runner::Id,
		token_hash: &str,
	) -> tg::Result<ControlFlow<(), tangram_server_core::database::Error>> {
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
		tangram_server_core::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn create_runner_token_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		runner: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let runner = runner
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the runner ID"))?;
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let output = self.create_runner_token(session, &runner, arg).await?;
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
