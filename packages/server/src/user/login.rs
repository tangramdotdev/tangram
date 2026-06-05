use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _, response::Ext as _},
};

impl Session {
	pub(crate) fn create_user_token() -> String {
		tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
	}

	pub(crate) async fn login_user(
		&self,
		arg: tg::user::login::Arg,
	) -> tg::Result<tg::user::login::Output> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.login_user_local(arg).await,
			tg::Location::Remote(remote) => {
				let client = self.get_remote_session(&remote.name).await?;
				let arg = tg::user::login::Arg {
					location: Some(tg::Location::Local(tg::location::Local::default()).into()),
					..arg
				};
				client.login_user(arg).await
			},
		}
	}

	async fn login_user_local(
		&self,
		arg: tg::user::login::Arg,
	) -> tg::Result<tg::user::login::Output> {
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					let output = session
						.login_user_with_transaction(transaction, arg)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(output))
				}
				.boxed()
			})
			.await
	}

	async fn login_user_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: tg::user::login::Arg,
	) -> tg::Result<tg::user::login::Output> {
		let specifier = arg.parent;
		if specifier.components().count() != 1 {
			return Err(tg::error!("invalid user specifier"));
		}
		let mut user = if let Some(node) =
			Self::try_get_node_by_specifier_with_transaction(transaction, &specifier).await?
		{
			if node.kind != tg::id::Kind::User {
				return Err(tg::error!("specifier is already in use"));
			}
			Self::user_from_node_with_transaction(transaction, node).await?
		} else {
			let id = tg::user::Id::new();
			let node = Self::create_node_with_transaction(
				transaction,
				&id.clone().into(),
				tg::id::Kind::User,
				&specifier,
				None,
			)
			.await?;
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into users (id, name)
					values ({p}1, {p}2);
				"
			);
			transaction
				.execute(
					statement.into(),
					db::params![id.to_string(), node.name.clone()],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			let public = Self::ensure_public_group_with_transaction(transaction).await?;
			Self::add_group_member_with_transaction(
				self,
				transaction,
				&tg::Selector::Id(public),
				&tg::group::Member::User(id.clone()),
			)
			.await?;
			let arg = tg::grant::create::Arg {
				principal: tg::grant::Principal::User(id.clone()).into(),
				permission: tg::grant::Permission::Admin,
				resource: tg::grant::Resource::Id(id.clone().into()),
			};
			self.create_grant_with_transaction(transaction, arg).await?;
			Self::user_from_node_with_transaction(transaction, node).await?
		};
		if let Some(email) = arg.email {
			let p = transaction.p();
			let statement = formatdoc!(
				r#"
					insert into user_emails ("user", email)
					values ({p}1, {p}2)
					on conflict ("user", email) do nothing;
				"#
			);
			transaction
				.execute(statement.into(), db::params![user.id.to_string(), email])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			user = Self::user_from_node_with_transaction(
				transaction,
				Self::try_get_node_by_id_with_transaction(transaction, &user.id.clone().into())
					.await?
					.unwrap(),
			)
			.await?;
		}
		let token = Self::create_user_token();
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				insert into user_tokens (id, "user")
				values ({p}1, {p}2);
			"#
		);
		transaction
			.execute(
				statement.into(),
				db::params![token.clone(), user.id.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(tg::user::login::Output { token, user })
	}

	pub(crate) async fn login_user_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let output = self.login_user(arg).await?;
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
