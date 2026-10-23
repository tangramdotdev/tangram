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
	pub(crate) async fn logout(
		&self,
		session: &Session,
		mut arg: tg::user::logout::Arg,
	) -> tg::Result<()> {
		let location = session.server.account_location(arg.location.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				let client = session.get_region_session(&region).await?;
				let location = tg::Location::Local(tg::location::Local {
					region: Some(region),
				});
				arg.location = Some(location.into());
				client.logout(arg).await?;
			},
			tg::Location::Local(_) => {
				if !session.server.config.accounts {
					return Err(tg::error!("local account management is disabled"));
				}
				self.logout_local(session).await?;
			},
			tg::Location::Remote(remote) => {
				session.verify_request_can_mutate_remotes()?;
				let client = session.get_remote_session(&remote.name).await?;
				arg.location = Some(
					tg::Location::Local(tg::location::Local {
						region: remote.region,
					})
					.into(),
				);
				client
					.logout(arg)
					.await
					.map_err(|error| tg::error!(!error, "failed to log out of the remote"))?;
				session.set_remote_token(&remote.name, None).await?;
			},
		}
		Ok(())
	}

	async fn logout_local(&self, session: &Session) -> tg::Result<()> {
		if !session.server.is_primary_region() {
			return self.logout_primary_region(session).await;
		}
		let tg::Principal::User(user) = &session.context.principal else {
			return Err(tg::error!("not logged in"));
		};
		let token = session
			.context
			.token
			.as_deref()
			.ok_or_else(|| tg::error!("missing the session token"))?;
		let token = tangram_server_core::token::hash(token);
		let user = user.to_string();
		session
			.server
			.database
			.run(|transaction| {
				let token = token.clone();
				let user = user.clone();
				async move { Self::logout_with_transaction(transaction, &token, &user).await }
					.boxed()
			})
			.await?;

		let hash = blake3::Hash::from_hex(token)
			.map_err(|error| tg::error!(!error, "failed to parse the token hash"))?;
		session
			.server
			.invalidate_authentication(
				tangram_server_core::authentication::cache::Invalidation::Token(*hash.as_bytes()),
			)
			.await;

		Ok(())
	}

	async fn logout_primary_region(&self, session: &Session) -> tg::Result<()> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		let arg = tg::user::logout::Arg {
			location: Some(tg::Location::Local(tg::location::Local::default()).into()),
		};
		client
			.logout(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to log out in the primary region"))?;

		Ok(())
	}

	async fn logout_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		token: &str,
		user: &str,
	) -> tg::Result<ControlFlow<(), tangram_server_core::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				delete from user_tokens
				where
					token = {p}1
					and "user" = {p}2;
			"#
		);
		let result = transaction
			.execute(statement.into(), db::params![token, user])
			.await;
		let n = tangram_server_core::database::retry!(result, "failed to execute the statement");
		if n != 1 {
			return Err(tg::error!("invalid session"));
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn logout_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		self.logout(session, arg).await?;
		let response = http::Response::builder()
			.status(http::StatusCode::NO_CONTENT)
			.empty()
			.unwrap()
			.boxed_body();

		Ok(response)
	}
}
