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
	pub(crate) async fn create_user_token(
		&self,
		session: &Session,
		mut arg: tg::user::token::create::Arg,
	) -> tg::Result<tg::user::token::create::Output> {
		let location = session.server.account_location(arg.location.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				let client = session.get_region_session(&region).await?;
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				client.create_user_token(arg).await
			},
			tg::Location::Local(_) => {
				if !session.server.config.accounts {
					return Err(tg::error!("local account management is disabled"));
				}
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				self.create_user_token_local(session, arg).await
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
				client.create_user_token(arg).await
			},
		}
	}

	async fn create_user_token_local(
		&self,
		session: &Session,
		arg: tg::user::token::create::Arg,
	) -> tg::Result<tg::user::token::create::Output> {
		if !session.server.is_primary_region() {
			return self.create_user_token_primary_region(session, arg).await;
		}
		let user = Self::authenticated_user(session)?.clone();
		let (id, token) = tangram_server_authentication_common::token::create();
		let created_at = session.server.clock.unix_timestamp();
		let token_hash = tangram_server_authentication_common::token::hash(&token);
		session
			.server
			.database
			.run(|transaction| {
				let id = id.clone();
				let token_hash = token_hash.clone();
				let user = user.clone();
				async move {
					Self::create_user_token_with_transaction(
						transaction,
						created_at,
						&id,
						&token_hash,
						&user,
					)
					.await
				}
				.boxed()
			})
			.await?;
		let data = tg::user::token::Data {
			created_at,
			id,
			token: Some(token),
		};
		let output = tg::user::token::create::Output { data };

		Ok(output)
	}

	async fn create_user_token_primary_region(
		&self,
		session: &Session,
		arg: tg::user::token::create::Arg,
	) -> tg::Result<tg::user::token::create::Output> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		let output = client.create_user_token(arg).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to create the user token in the primary region"
			)
		})?;

		Ok(output)
	}

	async fn create_user_token_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		created_at: i64,
		id: &tg::token::Id,
		token_hash: &str,
		user: &tg::user::Id,
	) -> tg::Result<ControlFlow<(), tangram_server_core::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				insert into user_tokens (created_at, id, token, "user")
				values ({p}1, {p}2, {p}3, {p}4);
			"#
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![created_at, id.to_string(), token_hash, user.to_string()],
			)
			.await;
		tangram_server_core::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn create_user_token_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let output = self.create_user_token(session, arg).await?;
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
