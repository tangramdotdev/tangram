use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn logout(&self, mut arg: tg::user::logout::Arg) -> tg::Result<()> {
		let location = self.server.account_location(arg.location.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != self.server.config.region.as_deref() => {
				let client = self.get_region_session(&region).await?;
				let location = tg::Location::Local(tg::location::Local {
					region: Some(region),
				});
				arg.location = Some(location.into());
				client.logout(arg).await?;
			},
			tg::Location::Local(_) => {
				if !self.server.config.accounts {
					return Err(tg::error!("local account management is disabled"));
				}
				self.logout_local().await?;
			},
			tg::Location::Remote(remote) => {
				self.verify_request_can_mutate_remotes()?;
				let client = self.get_remote_session(&remote.name).await?;
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
				self.set_remote_token(&remote.name, None).await?;
			},
		}
		Ok(())
	}

	async fn logout_local(&self) -> tg::Result<()> {
		if !self.server.is_primary_region() {
			return self.logout_primary_region().await;
		}
		let tg::Principal::User(user) = &self.context.principal else {
			return Err(tg::error!("not logged in"));
		};
		let token = self
			.context
			.token
			.as_deref()
			.ok_or_else(|| tg::error!("missing the session token"))?;
		let token = crate::token::hash(token);
		let user = user.to_string();
		self.server
			.database
			.run(|transaction| {
				let token = token.clone();
				let user = user.clone();
				async move { Self::logout_with_transaction(transaction, &token, &user).await }
					.boxed()
			})
			.await?;

		Ok(())
	}

	async fn logout_primary_region(&self) -> tg::Result<()> {
		let client = self
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
		transaction: &crate::database::Transaction<'_>,
		token: &str,
		user: &str,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
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
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n != 1 {
			return Err(tg::error!("invalid session"));
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn logout_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		self.logout(arg).await?;
		let response = http::Response::builder()
			.status(http::StatusCode::NO_CONTENT)
			.empty()
			.unwrap()
			.boxed_body();

		Ok(response)
	}
}
