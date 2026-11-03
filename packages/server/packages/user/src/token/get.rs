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
	pub(crate) async fn try_get_user_token(
		&self,
		session: &Session,
		token: &tg::token::Id,
		mut arg: tg::user::token::get::Arg,
	) -> tg::Result<Option<tg::user::token::get::Output>> {
		let location = session.server.account_location(arg.location.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				let client = session.get_region_session(&region).await?;
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				client.try_get_user_token(token, arg).await
			},
			tg::Location::Local(_) => self.try_get_user_token_local(session, token).await,
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
				let output = client
					.try_get_user_token(token, arg)
					.await?
					.map(|mut output| {
						output.location = Some(location);
						output
					});
				Ok(output)
			},
		}
	}

	async fn try_get_user_token_local(
		&self,
		session: &Session,
		token: &tg::token::Id,
	) -> tg::Result<Option<tg::user::token::get::Output>> {
		// Authenticate the user.
		let user = Self::authenticated_user(session)?;

		// Get the token.
		let row = session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let token = token.clone();
				let user = user.clone();
				async move {
					Self::try_get_user_token_with_transaction(transaction, &user, &token).await
				}
				.boxed()
			})
			.await?;
		let Some(row) = row else {
			return Ok(None);
		};

		// Create the output without the token's secret.
		let data = tg::user::token::Data {
			created_at: row.created_at,
			id: token.clone(),
			token: None,
		};
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let output = tg::user::token::get::Output {
			data,
			location: Some(location),
		};

		Ok(Some(output))
	}

	async fn try_get_user_token_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		user: &tg::user::Id,
		token: &tg::token::Id,
	) -> tg::Result<ControlFlow<Option<Row>, tangram_server_core::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select created_at
				from user_tokens
				where "user" = {p}1 and id = {p}2;
			"#
		);
		let result = transaction
			.query_optional_into::<Row>(
				statement.into(),
				db::params![user.to_string(), token.to_string()],
			)
			.await;
		let row = tangram_server_core::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(row))
	}

	pub(crate) async fn try_get_user_token_request(
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
		let Some(output) = self.try_get_user_token(session, &token, arg).await? else {
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
