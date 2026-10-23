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
	pub(crate) async fn try_delete_user_token(
		&self,
		session: &Session,
		token: &tg::token::Id,
		mut arg: tg::user::token::delete::Arg,
	) -> tg::Result<Option<()>> {
		let location = session.server.account_location(arg.location.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				let client = session.get_region_session(&region).await?;
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				client.try_delete_user_token(token, arg).await
			},
			tg::Location::Local(_) => {
				if !session.server.config.accounts {
					return Err(tg::error!("local account management is disabled"));
				}
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				self.try_delete_user_token_local(session, token, arg).await
			},
			tg::Location::Remote(remote) => {
				let client = session.get_remote_session(&remote.name).await?;
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
		session: &Session,
		token: &tg::token::Id,
		arg: tg::user::token::delete::Arg,
	) -> tg::Result<Option<()>> {
		if !session.server.is_primary_region() {
			return self
				.try_delete_user_token_primary_region(session, token, arg)
				.await;
		}
		let user = Self::authenticated_user(session)?.clone();
		let token = token.clone();
		let deleted = session
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

		if let Some(hash) = deleted {
			session
				.server
				.invalidate_authentication(
					tangram_server_core::authentication::cache::Invalidation::Token(hash),
				)
				.await;
		}

		Ok(deleted.map(|_| ()))
	}

	async fn try_delete_user_token_primary_region(
		&self,
		session: &Session,
		token: &tg::token::Id,
		arg: tg::user::token::delete::Arg,
	) -> tg::Result<Option<()>> {
		let client = session
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
		transaction: &tangram_server_core::database::Transaction<'_>,
		token: &tg::token::Id,
		user: &tg::user::Id,
	) -> tg::Result<
		ControlFlow<
			Option<tangram_server_core::authentication::cache::Hash>,
			tangram_server_core::database::Error,
		>,
	> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				delete from user_tokens
				where id = {p}1 and "user" = {p}2 returning token;
			"#
		);
		let result = transaction
			.query_optional_value_into::<String>(
				statement.into(),
				db::params![token.to_string(), user.to_string()],
			)
			.await;
		let hash = tangram_server_core::database::retry!(result, "failed to execute the statement");
		let hash = hash
			.map(|hash| blake3::Hash::from_hex(hash).map(|hash| *hash.as_bytes()))
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the token hash"))?;

		Ok(ControlFlow::Break(hash))
	}

	pub(crate) async fn try_delete_user_token_request(
		&self,
		session: &Session,
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
		let output = self.try_delete_user_token(session, &token, arg).await?;
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
