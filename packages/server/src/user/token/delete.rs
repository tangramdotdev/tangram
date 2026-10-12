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
	pub(crate) async fn try_delete_user_token(
		&self,
		token: &tg::token::Id,
		mut arg: tg::user::token::delete::Arg,
	) -> tg::Result<Option<()>> {
		let location = self.server.account_location(arg.location.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != self.server.config.region.as_deref() => {
				let client = self.get_region_session(&region).await?;
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				client.try_delete_user_token(token, arg).await
			},
			tg::Location::Local(_) => {
				if !self.server.config.accounts {
					return Err(tg::error!("local account management is disabled"));
				}
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				self.try_delete_user_token_local(token, arg).await
			},
			tg::Location::Remote(remote) => {
				let client = self.get_remote_session(&remote.name).await?;
				arg.location = Some(
					tg::Location::Local(tg::location::Local {
						region: remote.region,
					})
					.into(),
				);
				client.try_delete_user_token(token, arg).await
			},
		}
	}

	async fn try_delete_user_token_local(
		&self,
		token: &tg::token::Id,
		arg: tg::user::token::delete::Arg,
	) -> tg::Result<Option<()>> {
		if !self.server.is_primary_region() {
			return self.try_delete_user_token_primary_region(token, arg).await;
		}
		let user = self.authenticated_user()?.clone();
		let token = token.clone();
		let deleted = self
			.server
			.database
			.run(|transaction| {
				let token = token.clone();
				let user = user.clone();
				async move {
					Self::delete_user_token_with_transaction(transaction, &token, &user).await
				}
				.boxed()
			})
			.await?;

		Ok(deleted.then_some(()))
	}

	async fn try_delete_user_token_primary_region(
		&self,
		token: &tg::token::Id,
		arg: tg::user::token::delete::Arg,
	) -> tg::Result<Option<()>> {
		let client = self
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		let output = client
			.try_delete_user_token(token, arg)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					"failed to delete the user token in the primary region"
				)
			})?;

		Ok(output)
	}

	async fn delete_user_token_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		token: &tg::token::Id,
		user: &tg::user::Id,
	) -> tg::Result<ControlFlow<bool, crate::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				delete from user_tokens
				where id = {p}1 and "user" = {p}2;
			"#
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![token.to_string(), user.to_string()],
			)
			.await;
		let count = crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(count == 1))
	}

	pub(crate) async fn try_delete_user_token_request(
		&self,
		request: http::Request<BoxBody>,
		token: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let token = token
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the token ID"))?;
		let output = self.try_delete_user_token(&token, arg).await?;
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
