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
	pub(crate) async fn create_runner(
		&self,
		session: &Session,
		mut arg: tg::runner::create::Arg,
	) -> tg::Result<tg::runner::create::Output> {
		let location = session.server.runner_location(arg.location.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				let client = session.get_region_session(&region).await?;
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				client.create_runner(arg).await
			},
			tg::Location::Local(_) => {
				if !session.server.config.runners {
					return Err(tg::error!("local runner management is disabled"));
				}
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				self.create_runner_local(session, arg).await
			},
			tg::Location::Remote(remote) => {
				let client = session.get_remote_session(&remote.name).await?;
				arg.location = Some(
					tg::Location::Local(tg::location::Local {
						region: remote.region,
					})
					.into(),
				);
				client.create_runner(arg).await
			},
		}
	}

	async fn create_runner_local(
		&self,
		session: &Session,
		arg: tg::runner::create::Arg,
	) -> tg::Result<tg::runner::create::Output> {
		if !session.server.is_primary_region() {
			return self.create_runner_primary_region(session, arg).await;
		}
		let owner = match arg.owner {
			Some(owner) => Some(self.resolve_runner_owner(session, &owner).await?),
			None => None,
		};
		let owner_id = owner.as_ref().and_then(tg::Principal::to_id);
		self.authorize_runner_owner(session, owner_id.as_ref())
			.await?;
		let created_at = session.server.clock.unix_timestamp();
		let runner = tg::runner::Id::new();
		let (token_id, token) = tangram_server_core::token::create();
		let token_hash = tangram_server_core::token::hash(&token);
		session
			.server
			.database
			.run(|transaction| {
				let owner_id = owner_id.clone();
				let runner = runner.clone();
				let token_hash = token_hash.clone();
				let token_id = token_id.clone();
				async move {
					Self::create_runner_with_transaction(
						transaction,
						created_at,
						&runner,
						owner_id.as_ref(),
						&token_id,
						&token_hash,
					)
					.await
				}
				.boxed()
			})
			.await?;
		let data = tg::runner::Data {
			created_at,
			id: runner,
			owner,
		};
		let token = tg::runner::token::Data {
			created_at,
			id: token_id,
			token: Some(token),
		};

		Ok(tg::runner::create::Output { data, token })
	}

	async fn create_runner_primary_region(
		&self,
		session: &Session,
		arg: tg::runner::create::Arg,
	) -> tg::Result<tg::runner::create::Output> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		let output = client.create_runner(arg).await.map_err(|error| {
			tg::error!(!error, "failed to create the runner in the primary region")
		})?;

		Ok(output)
	}

	async fn create_runner_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		created_at: i64,
		runner: &tg::runner::Id,
		owner: Option<&tg::Id>,
		token: &tg::token::Id,
		token_hash: &str,
	) -> tg::Result<ControlFlow<(), tangram_server_core::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into runners (created_at, id, owner)
				values ({p}1, {p}2, {p}3);
			"
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![
					created_at,
					runner.to_string(),
					owner.map(ToString::to_string)
				],
			)
			.await;
		tangram_server_core::database::retry!(result, "failed to execute the statement");
		let statement = formatdoc!(
			"
				insert into runner_tokens (created_at, id, runner, token)
				values ({p}1, {p}2, {p}3, {p}4);
			"
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![
					created_at,
					token.to_string(),
					runner.to_string(),
					token_hash
				],
			)
			.await;
		tangram_server_core::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn create_runner_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let output = self.create_runner(session, arg).await?;
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
