use {
	crate::Session,
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Session {
	pub(crate) async fn login_user(
		&self,
		arg: tg::user::login::Arg,
	) -> tg::Result<tg::user::login::Output> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let location = self
			.server
			.identity_location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.login_user_local(arg.email, arg.handle).await,
			tg::Location::Remote(remote) => self.login_user_remote(arg, remote).await,
		}
	}

	async fn login_user_local(
		&self,
		email: String,
		requested_handle: Option<String>,
	) -> tg::Result<tg::user::login::Output> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let mut connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to create the transaction"))?;

		// Get or create the user.
		#[derive(db::row::Deserialize)]
		struct UserRow {
			handle: Option<String>,
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::user::Id,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select users.id, users.handle
				from user_emails
				join users on users.id = user_emails."user"
				where user_emails.email = {p}1;
			"#
		);
		let params = db::params![email.clone()];
		let user = transaction
			.query_optional_into::<UserRow>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let (id, mut handle) = if let Some(user) = user {
			(user.id, user.handle)
		} else {
			let id = tg::user::Id::new();
			let statement = formatdoc!(
				"
					insert into users (id)
					values ({p}1);
				"
			);
			let params = db::params![id.to_string()];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

			let statement = formatdoc!(
				r#"
					insert into user_emails ("user", email)
					values ({p}1, {p}2);
				"#
			);
			let params = db::params![id.to_string(), email];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

			(id, None)
		};

		if let Some(requested_handle) = requested_handle {
			if let Some(handle) = &handle {
				if handle != &requested_handle {
					return Err(tg::error!("user already has a handle"));
				}
			} else {
				let namespace = Self::namespace_for_handle(&requested_handle)?;

				let statement = formatdoc!(
					"
						select 1
						from groups
						where handle = {p}1;
					"
				);
				let params = db::params![requested_handle.clone()];
				if transaction
					.query_optional(statement.into(), params)
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
					.is_some()
				{
					return Err(tg::error!("handle is already in use"));
				}

				let statement = formatdoc!(
					"
						update users
						set handle = {p}2
						where id = {p}1;
					"
				);
				let params = db::params![id.to_string(), requested_handle.clone()];
				transaction
					.execute(statement.into(), params)
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

				let namespace_id =
					Self::get_or_create_namespace_with_transaction(&transaction, &namespace)
						.await?;
				Self::grant_namespace_to_user_with_transaction(
					&transaction,
					&namespace,
					namespace_id,
					&id,
					tg::Permission::Admin,
					Some(&id),
				)
				.await?;

				handle = Some(requested_handle);
			}
		}

		// Create the token.
		let token = Self::create_user_token();
		let statement = formatdoc!(
			r#"
				insert into user_tokens (id, "user")
				values ({p}1, {p}2);
			"#
		);
		let params = db::params![token.clone(), id.to_string()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

		// Get the user's emails.
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
		let rows = transaction
			.query_all_into::<EmailRow>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let emails = rows.into_iter().map(|row| row.email).collect();

		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		drop(connection);

		let user = tg::User {
			emails,
			handle,
			id,
			location: None,
		};
		let output = tg::user::login::Output { token, user };

		Ok(output)
	}

	async fn login_user_remote(
		&self,
		arg: tg::user::login::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<tg::user::login::Output> {
		let client = self
			.get_remote_session(remote.name.clone())
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
		self.put_remote_token(&remote.name, output.token.clone())
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					remote = %remote.name,
					"failed to update the remote"
				)
			})?;
		output.user.location = Some(tg::Location::Remote(tg::location::Remote {
			name: remote.name,
			region: None,
		}));
		Ok(output)
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
