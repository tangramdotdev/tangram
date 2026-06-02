use {
	crate::{Session, context::Authentication},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Session {
	pub(crate) async fn login_user(
		&self,
		arg: tg::user::login::Arg,
	) -> tg::Result<tg::user::login::Output> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}

		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		let output = match location {
			tg::Location::Local(_) => self.login_user_local(arg.namespace, arg.email).await?,
			tg::Location::Remote(remote) => self.login_user_remote(arg, remote).await?,
		};

		Ok(output)
	}

	async fn login_user_local(
		&self,
		namespace: tg::Namespace,
		email: Option<String>,
	) -> tg::Result<tg::user::login::Output> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}

		self.server
			.database
			.run(|transaction| {
				let email = email.clone();
				let namespace = namespace.clone();
				async move {
					Self::login_user_local_with_transaction(transaction, &namespace, email).await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to log in the user"))
	}

	async fn login_user_local_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		namespace: &tg::Namespace,
		email: Option<String>,
	) -> tg::Result<ControlFlow<tg::user::login::Output, crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct UserRow {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::user::Id,
		}
		Self::namespace_for_user(namespace)?;
		let namespace_string = namespace.to_string();
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select users.id
				from users
				where users.namespace = {p}1;
			"
		);
		let params = db::params![namespace_string.clone()];
		let result = transaction
			.query_optional_into::<UserRow>(statement.into(), params)
			.await;
		let user = crate::database::retry!(result, "failed to execute the statement");
		let id = if let Some(user) = user {
			user.id
		} else {
			if Self::namespace_in_use_with_transaction(transaction, &namespace_string).await? {
				return Err(tg::error!("namespace is already in use"));
			}

			let id = tg::user::Id::new();
			let statement = formatdoc!(
				"
					insert into users (id, namespace)
					values ({p}1, {p}2);
				"
			);
			let params = db::params![id.to_string(), namespace_string.clone()];
			let result = transaction.execute(statement.into(), params).await;
			crate::database::retry!(result, "failed to execute the statement");

			let namespace_id =
				match Self::get_or_create_namespace_with_transaction(transaction, namespace).await?
				{
					ControlFlow::Break(namespace_id) => namespace_id,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
			match Self::create_namespace_grant_for_user_with_transaction(
				transaction,
				namespace,
				namespace_id,
				&id,
				tg::Permission::Admin,
				Some(&id),
			)
			.await?
			{
				ControlFlow::Break(_) => {},
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}

			id
		};

		if let Some(email) = email {
			let statement = formatdoc!(
				r#"
					select "user"
					from user_emails
					where email = {p}1;
				"#
			);
			let result = transaction
				.query_optional_value_into::<String>(statement.into(), db::params![email.clone()])
				.await;
			let email_user = crate::database::retry!(result, "failed to execute the statement");
			match email_user {
				Some(email_user) if email_user != id.to_string() => {
					return Err(tg::error!("email is already in use"));
				},
				Some(_) => {},
				None => {
					let statement = formatdoc!(
						r#"
							insert into user_emails ("user", email)
							values ({p}1, {p}2);
						"#
					);
					let params = db::params![id.to_string(), email];
					let result = transaction.execute(statement.into(), params).await;
					crate::database::retry!(result, "failed to execute the statement");
				},
			}
		}

		let token = Self::create_user_token();
		let statement = formatdoc!(
			r#"
				insert into user_tokens (id, "user")
				values ({p}1, {p}2);
			"#
		);
		let params = db::params![token.clone(), id.to_string()];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");

		#[derive(db::row::Deserialize)]
		struct EmailRow {
			email: String,
		}
		let statement = formatdoc!(
			r#"
				select email
				from user_emails
				where user_emails."user" = {p}1
				order by email;
			"#
		);
		let params = db::params![id.to_string()];
		let result = transaction
			.query_all_into::<EmailRow>(statement.into(), params)
			.await;
		let rows = crate::database::retry!(result, "failed to execute the statement");
		let emails = rows.into_iter().map(|row| row.email).collect();

		let user = tg::User {
			emails,
			namespace: Some(namespace.clone()),
			id,
			location: None,
		};
		let output = tg::user::login::Output { token, user };
		Ok(ControlFlow::Break(output))
	}

	async fn login_user_remote(
		&self,
		arg: tg::user::login::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<tg::user::login::Output> {
		let client = self
			.get_remote_session(&remote.name)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					remote = %remote.name,
					"failed to get the remote client"
				)
			})?;

		let arg = tg::user::login::Arg {
			location: Some(tg::Location::Local(tg::location::Local::default()).into()),
			..arg
		};
		let mut output = client.login_user(arg).await.map_err(|error| {
			tg::error!(
				!error,
				remote = %remote.name,
				"failed to log in the user"
			)
		})?;

		let user = self
			.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_user_ref().ok())
			.map(|user| user.id.to_string());
		let remote_name = remote.name.clone();
		let token = output.token.clone();
		self.server
			.database
			.run(|transaction| {
				let remote_name = remote_name.clone();
				let token = token.clone();
				let user = user.clone();
				async move {
					Self::update_remote_token_with_transaction(
						transaction,
						&remote_name,
						&token,
						user.as_deref(),
					)
					.await
				}
				.boxed()
			})
			.await?;

		output.user.location = Some(tg::Location::Remote(tg::location::Remote {
			name: remote.name,
			region: None,
		}));

		Ok(output)
	}

	async fn update_remote_token_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		remote_name: &str,
		token: &str,
		user: Option<&str>,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				update remotes
				set token = {p}2
				where name = {p}1 and (
					("user" is null and {p}3 is null) or
					"user" = {p}3
				);
			"#,
		);
		let params = db::params![remote_name, token, user];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to update the remote");
		Ok(ControlFlow::Break(()))
	}

	fn create_user_token() -> String {
		tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
	}

	pub(crate) async fn login_user_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Log in the user.
		let output = self
			.login_user(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to log in the user"))?;

		// Create the response.
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
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
