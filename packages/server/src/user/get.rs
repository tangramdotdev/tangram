use {
	crate::Session,
	indoc::formatdoc,
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
		let tg::get::Node::Id(id) = output.referent.node else {
			unreachable!();
		};
		let Ok(id) = tg::user::Id::try_from(id) else {
			return Ok(None);
		};
		let location = output
			.location
			.unwrap_or_else(|| tg::Location::Local(tg::location::Local::default()));
		let token = output.referent.options.token;
		match location {
			tg::Location::Local(_) => self.try_get_user_local(&id, token).await,
			tg::Location::Remote(remote) => self.try_get_user_remote(&id, arg, remote, token).await,
		}
	}

	async fn try_get_user_local(
		&self,
		id: &tg::user::Id,
		token: Option<tg::grant::Token>,
	) -> tg::Result<Option<tg::user::get::Output>> {
		let permission = tg::grant::Permission::User(tg::grant::permission::user::Permission::Read);
		let authorized = self
			.authorize(tg::user::Selector::Id(id.clone()), permission)
			.await?;
		if !authorized.is_some_and(|permissions| permissions.contains(permission)) {
			return Ok(None);
		}
		let mut connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let Some(mut user) = Self::try_get_user_with_transaction(&transaction, id).await? else {
			return Ok(None);
		};
		user.token = token;

		Ok(Some(user))
	}

	pub(crate) async fn try_get_user_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		id: &tg::user::Id,
	) -> tg::Result<Option<tg::User>> {
		#[derive(db::row::Deserialize)]
		struct EmailRow {
			email: String,
		}

		#[derive(db::row::Deserialize)]
		struct UserRow {
			name: String,
		}

		let Some(specifier) =
			Self::try_get_specifier_for_id_with_transaction(transaction, &id.clone().into())
				.await?
		else {
			return Ok(None);
		};
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select name
				from users
				where id = {p}1;
			"
		);
		let Some(user) = transaction
			.query_optional_into::<UserRow>(statement.into(), db::params![id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		else {
			return Ok(None);
		};
		let statement = formatdoc!(
			r#"
				select email
				from user_emails
				where "user" = {p}1
				order by email;
			"#
		);
		let rows = transaction
			.query_all_into::<EmailRow>(statement.into(), db::params![id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let user = tg::User {
			emails: rows.into_iter().map(|row| row.email).collect(),
			id: id.clone(),
			location: Some(tg::Location::Local(tg::location::Local::default())),
			name: user.name,
			specifier,
			token: None,
		};

		Ok(Some(user))
	}

	async fn try_get_user_remote(
		&self,
		id: &tg::user::Id,
		mut arg: tg::user::get::Arg,
		remote: tg::location::Remote,
		token: Option<tg::grant::Token>,
	) -> tg::Result<Option<tg::user::get::Output>> {
		let cached = arg.cached;
		let ttl = arg.ttl;
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
			if let Some(user) = &mut output {
				user.token = user.token.take().or_else(|| token.clone());
			}
			let valid = output.as_ref().is_none_or(|user| {
				crate::remote::cache::token_valid(user.token.as_ref(), &self.server.clock)
			});
			if valid || cached {
				if let Some(user) = &mut output {
					if !crate::remote::cache::token_valid(user.token.as_ref(), &self.server.clock) {
						user.token = None;
					}
					user.location = Some(tg::Location::Remote(remote));
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
		if let Some(user) = &mut output {
			user.token = user.token.take().or(token);
		}
		let response =
			crate::remote::cache::Response::UserGet(crate::remote::cache::UserGetResponse {
				output: output.clone(),
			});
		self.put_cached_remote_response(&remote.name, &request, &response)
			.await?;
		if let Some(user) = &mut output {
			user.location = Some(tg::Location::Remote(remote));
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
