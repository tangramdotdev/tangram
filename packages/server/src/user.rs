use {
	crate::{Session, context::Authentication, node::Node},
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
	pub(crate) fn create_user_token() -> String {
		tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
	}

	pub(crate) async fn get_current_user(
		&self,
		arg: tg::user::current::Arg,
	) -> tg::Result<Option<tg::User>> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => Ok(match &self.context.authentication {
				Some(Authentication::User(user)) => Some(user.clone()),
				_ => None,
			}),
			tg::Location::Remote(remote) => {
				let client = self.get_remote_session(&remote.name).await?;
				let arg = tg::user::current::Arg {
					location: Some(tg::Location::Local(tg::location::Local::default()).into()),
				};
				client.get_current_user(arg).await
			},
		}
	}

	pub(crate) async fn try_get_user(
		&self,
		user: &tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> tg::Result<Option<tg::User>> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.try_get_user_local(user).await,
			tg::Location::Remote(remote) => {
				let client = self.get_remote_session(&remote.name).await?;
				let arg = tg::user::get::Arg {
					location: Some(tg::Location::Local(tg::location::Local::default()).into()),
				};
				client.try_get_user(user, arg).await
			},
		}
	}

	async fn try_get_user_local(&self, user: &tg::user::Selector) -> tg::Result<Option<tg::User>> {
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
		let Some(node) =
			Self::try_get_node_by_selector_with_transaction(&transaction, user).await?
		else {
			return Ok(None);
		};
		if node.kind != tg::id::Kind::User
			|| !self
				.node_is_visible_with_transaction(&transaction, &node.id)
				.await?
		{
			return Ok(None);
		}
		Self::user_from_node_with_transaction(&transaction, node)
			.await
			.map(Some)
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
			let all = Self::ensure_all_group_with_transaction(transaction).await?;
			Self::add_group_member_with_transaction(
				self,
				transaction,
				&tg::Selector::Id(all),
				&tg::group::Member::User(id.clone()),
			)
			.await?;
			let arg = tg::grant::create::Arg {
				principal: tg::grant::Principal::User(id.clone()),
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

	pub(crate) async fn try_get_user_grants(
		&self,
		user: &tg::user::Selector,
		arg: tg::user::grants::Arg,
	) -> tg::Result<Option<tg::user::grants::Output>> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.try_get_user_grants_local(user).await,
			tg::Location::Remote(remote) => {
				let client = self.get_remote_session(&remote.name).await?;
				client.try_get_user_grants(user, arg).await
			},
		}
	}

	async fn try_get_user_grants_local(
		&self,
		user: &tg::user::Selector,
	) -> tg::Result<Option<tg::user::grants::Output>> {
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
		let Some(node) =
			Self::try_get_node_by_selector_with_transaction(&transaction, user).await?
		else {
			return Ok(None);
		};
		if node.kind != tg::id::Kind::User
			|| !self
				.node_is_visible_with_transaction(&transaction, &node.id)
				.await?
		{
			return Ok(None);
		}
		let data = Self::list_direct_grants_with_transaction(&transaction, &node.id).await?;
		Ok(Some(tg::user::grants::Output { data }))
	}

	pub(crate) async fn user_from_node_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		node: Node,
	) -> tg::Result<tg::User> {
		#[derive(db::row::Deserialize)]
		struct Row {
			email: String,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select email
				from user_emails
				where "user" = {p}1
				order by email;
			"#
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![node.id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(tg::User {
			emails: rows.into_iter().map(|row| row.email).collect(),
			id: node.id.try_into()?,
			location: None,
			name: node.name,
			specifier: node.specifier,
		})
	}

	pub(crate) async fn get_current_user_request(
		&self,
		request: http::Request<BoxBody>,
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
		let Some(output) = self.get_current_user(arg).await? else {
			let response = http::Response::builder()
				.status(http::StatusCode::UNAUTHORIZED)
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
		Ok(response.body(body).unwrap().boxed_body())
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
			.unwrap_or(tg::user::get::Arg { location: None });
		let user = parse_selector::<tg::user::Id>(user)?;
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
		Ok(response.body(body).unwrap().boxed_body())
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
		Ok(response.body(body).unwrap().boxed_body())
	}

	pub(crate) async fn try_get_user_grants_request(
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
		let user = parse_selector::<tg::user::Id>(user)?;
		let Some(output) = self.try_get_user_grants(&user, arg).await? else {
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
		Ok(response.body(body).unwrap().boxed_body())
	}
}

pub(crate) fn parse_selector<I>(s: &str) -> tg::Result<tg::Selector<I>>
where
	I: std::str::FromStr<Err = tg::Error>,
{
	s.replace(':', "/").parse()
}
