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
	pub(crate) async fn try_get_user(
		&self,
		user: &tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> tg::Result<Option<tg::user::get::Output>> {
		let selector = match user {
			tg::Selector::Id(id) => tg::Selector::Id(id.clone().into()),
			tg::Selector::Specifier(specifier) => tg::Selector::Specifier(specifier.clone()),
		};
		let Some(output) = self
			.try_get_with_selector(&selector, arg.location.as_ref(), arg.cached, arg.ttl)
			.await?
		else {
			return Ok(None);
		};
		let location = output
			.referent
			.options
			.location
			.clone()
			.unwrap_or_else(|| tg::Location::Local(tg::location::Local::default()));
		let tg::get::Node::Id(id) = output.referent.node else {
			unreachable!();
		};
		let Ok(id) = tg::user::Id::try_from(id) else {
			return Ok(None);
		};
		let tokens = output.referent.options.tokens;
		match location {
			tg::Location::Local(_) => self.try_get_user_local(&id, tokens).await,
			tg::Location::Remote(remote) => {
				self.try_get_user_remote(&id, arg, remote, tokens).await
			},
		}
	}

	async fn try_get_user_local(
		&self,
		id: &tg::user::Id,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<Option<tg::user::get::Output>> {
		let permission = tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Read,
		);
		let authorized = self
			.authorize(
				tg::Referent::with_node_and_tokens(
					tg::user::Selector::Id(id.clone()),
					tokens.clone(),
				),
				permission,
			)
			.await?;
		if !authorized.is_some_and(|permissions| permissions.contains(permission)) {
			return Ok(None);
		}
		let id = id.clone();
		let user = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let id = id.clone();
				async move { Self::try_get_user_with_transaction(transaction, &id).await }.boxed()
			})
			.await?;
		let Some(mut user) = user else {
			return Ok(None);
		};
		user.tokens = tokens;

		Ok(Some(user))
	}

	pub(crate) async fn try_get_user_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		id: &tg::user::Id,
	) -> tg::Result<ControlFlow<Option<tg::User>, crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct EmailRow {
			email: String,
		}

		#[derive(db::row::Deserialize)]
		struct UserRow {
			name: String,
		}

		let specifier =
			match Self::try_get_specifier_for_id_with_transaction(transaction, &id.clone().into())
				.await?
			{
				ControlFlow::Break(specifier) => specifier,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		let Some(specifier) = specifier else {
			return Ok(ControlFlow::Break(None));
		};
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select name
				from users
				where id = {p}1;
			"
		);
		let result = transaction
			.query_optional_into::<UserRow>(statement.into(), db::params![id.to_string()])
			.await;
		let user = crate::database::retry!(result, "failed to execute the statement");
		let Some(user) = user else {
			return Ok(ControlFlow::Break(None));
		};
		let statement = formatdoc!(
			r#"
				select email
				from user_emails
				where "user" = {p}1
				order by email;
			"#
		);
		let result = transaction
			.query_all_into::<EmailRow>(statement.into(), db::params![id.to_string()])
			.await;
		let rows = crate::database::retry!(result, "failed to execute the statement");
		let user = tg::User {
			emails: rows.into_iter().map(|row| row.email).collect(),
			id: id.clone(),
			location: Some(tg::Location::Local(tg::location::Local::default())),
			name: user.name,
			specifier,
			tokens: tg::authorization::Tokens::default(),
		};

		Ok(ControlFlow::Break(Some(user)))
	}

	async fn try_get_user_remote(
		&self,
		id: &tg::user::Id,
		mut arg: tg::user::get::Arg,
		remote: tg::location::Remote,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<Option<tg::user::get::Output>> {
		let cached = arg.cached;
		let ttl = arg.ttl;
		let location = tg::Location::Remote(remote.clone());
		arg.cached = false;
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: remote.region.clone(),
			})
			.into(),
		);
		arg.ttl = tg::remote::cache::Ttl::default();
		let request =
			crate::remote::cache::Request::UserGet(crate::remote::cache::UserGetRequest {
				arg: arg.clone(),
				id: id.clone(),
			});
		if let Some(crate::remote::cache::Response::UserGet(response)) = self
			.try_get_cached_remote_response(&remote.name, &request, ttl)
			.await?
		{
			let mut output = response.output;
			let valid = output.as_ref().is_none_or(|user| {
				crate::remote::cache::token_valid(user.tokens.local(), &self.server.clock)
			});
			if valid || cached {
				if let Some(user) = &mut output {
					if !crate::remote::cache::token_valid(user.tokens.local(), &self.server.clock) {
						user.tokens.remove_local();
					}
					self.update_tokens_for_location(&mut user.tokens, &location)?;
					user.tokens.inherit(&tokens);
					user.location = Some(location);
				}

				return Ok(output);
			}
		}
		if cached {
			return Ok(None);
		}
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let user = tg::user::Selector::Id(id.clone());
		let mut output = client
			.try_get_user(&user, arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote.name, "failed to get the user"))?;
		let response =
			crate::remote::cache::Response::UserGet(crate::remote::cache::UserGetResponse {
				output: output.clone(),
			});
		self.put_cached_remote_response(&remote.name, &request, &response)
			.await?;
		if let Some(user) = &mut output {
			self.update_tokens_for_location(&mut user.tokens, &location)?;
			user.tokens.inherit(&tokens);
			user.location = Some(location);
		}

		Ok(output)
	}

	pub(crate) async fn try_get_user_request(
		&self,
		request: http::Request<BoxBody>,
		user: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let user = user.replace(':', "/").parse()?;
		let Some(output) = self.try_get_user(&user, arg).await? else {
			let response = http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
		};
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap().boxed_body();
		Ok(response)
	}
}
