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

impl Component {
	pub(crate) async fn try_delete_runner_token(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		token: &tg::token::Id,
		mut arg: tg::runner::token::delete::Arg,
	) -> tg::Result<Option<()>> {
		let location = session.server.runner_location(arg.location.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				let client = session.get_region_session(&region).await?;
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				client.try_delete_runner_token(runner, token, arg).await
			},
			tg::Location::Local(_) => {
				if !session.server.config.runners {
					return Err(tg::error!("local runner management is disabled"));
				}
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				self.try_delete_runner_token_local(session, runner, token, arg)
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
						region: remote.region,
					})
					.into(),
				);
				client.try_delete_runner_token(runner, token, arg).await
			},
		}
	}

	async fn try_delete_runner_token_local(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		token: &tg::token::Id,
		arg: tg::runner::token::delete::Arg,
	) -> tg::Result<Option<()>> {
		if !session.server.is_primary_region() {
			return self
				.try_delete_runner_token_primary_region(session, runner, token, arg)
				.await;
		}
		self.get_authorized_runner(session, runner).await?;
		let runner = runner.clone();
		let token = token.clone();
		let deleted = session
			.server
			.database
			.run(|transaction| {
				let runner = runner.clone();
				let token = token.clone();
				async move {
					Self::delete_runner_token_with_transaction(transaction, &runner, &token).await
				}
				.boxed()
			})
			.await?;

		if let Some(hash) = deleted {
			session
				.server
				.components
				.authentication
				.invalidate_authentication(
					&session.server,
					tangram_server_core::authentication::cache::Invalidation::Token(hash),
				)
				.await;
		}

		Ok(deleted.map(|_| ()))
	}

	async fn try_delete_runner_token_primary_region(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		token: &tg::token::Id,
		arg: tg::runner::token::delete::Arg,
	) -> tg::Result<Option<()>> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		let output = client
			.try_delete_runner_token(runner, token, arg)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					"failed to delete the runner token in the primary region"
				)
			})?;

		Ok(output)
	}

	async fn delete_runner_token_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		runner: &tg::runner::Id,
		token: &tg::token::Id,
	) -> tg::Result<
		ControlFlow<
			Option<tangram_server_core::authentication::cache::Hash>,
			tangram_server_core::database::Error,
		>,
	> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from runner_tokens
				where id = {p}1 and runner = {p}2 returning token;
			"
		);
		let result = transaction
			.query_optional_value_into::<String>(
				statement.into(),
				db::params![token.to_string(), runner.to_string()],
			)
			.await;
		let hash = tangram_server_core::database::retry!(result, "failed to execute the statement");
		let hash = hash
			.map(|hash| blake3::Hash::from_hex(hash).map(|hash| *hash.as_bytes()))
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the token hash"))?;

		Ok(ControlFlow::Break(hash))
	}

	pub(crate) async fn try_delete_runner_token_request(
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
		let output = self
			.try_delete_runner_token(session, &runner, &token, arg)
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
