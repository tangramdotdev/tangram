use {
	crate::{Origin, Server, Session},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

mod token;

pub(crate) struct Authentication {
	pub billing: bool,
	pub principal: tg::Principal,
}

#[derive(Clone)]
pub(crate) struct Process {
	pub debug: Option<tg::process::Debug>,
	pub host: String,
	pub inner_token: Option<String>,
	pub location: Option<tg::Location>,
	pub retry: bool,
	pub sandbox: tg::sandbox::Id,
}

#[derive(Clone)]
pub(crate) struct Sandbox {
	pub location: tg::Location,
	pub token: Option<String>,
}

impl Session {
	pub(crate) async fn try_get_authenticated_process(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<Process>> {
		if let Some(sandbox) = self
			.server
			.runner
			.state()
			.processes()
			.get(id)
			.map(|sandbox| sandbox.value().clone())
			&& let Some(sandbox) = self.server.runner.state().sandboxes().get_by_id(&sandbox)
			&& let Some(process) = sandbox.processes.get(id)
		{
			return Ok(Some(Process {
				debug: process.data.debug.clone(),
				host: process.data.host.clone(),
				inner_token: Some(process.inner_token.clone()),
				location: sandbox.data.location.clone(),
				retry: process.data.retry,
				sandbox: process.data.sandbox.clone(),
			}));
		}

		let Some(process) = self.try_get_process_from_index(id).await? else {
			return Ok(None);
		};
		let Some(data) = process.data else {
			return Ok(None);
		};
		let location = self
			.server
			.config
			.roles
			.contains(&crate::config::Role::Runner)
			.then(|| self.server.config.runner.remote.clone())
			.flatten()
			.map(|name| tg::Location::Remote(tg::location::Remote { name, region: None }));
		Ok(Some(Process {
			debug: data.debug,
			host: data.host,
			inner_token: None,
			location,
			retry: data.retry,
			sandbox: data.sandbox,
		}))
	}

	pub(crate) async fn try_get_authenticated_sandbox(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<Sandbox>> {
		if let Some(sandbox) = self.server.runner.state().sandboxes().get_by_id(id) {
			let location = sandbox
				.data
				.location
				.clone()
				.ok_or_else(|| tg::error!(%id, "missing the sandbox location"))?;
			return Ok(Some(Sandbox {
				location,
				token: sandbox.token.clone(),
			}));
		}

		let Some(sandbox) = self.try_get_sandbox_from_index(id).await? else {
			return Ok(None);
		};
		let Some(data) = sandbox.data else {
			return Ok(None);
		};
		let location = data.location.unwrap_or_else(|| {
			self.server
				.config
				.roles
				.contains(&crate::config::Role::Runner)
				.then(|| self.server.config.runner.remote.clone())
				.flatten()
				.map_or_else(
					|| tg::Location::Local(tg::location::Local::default()),
					|name| tg::Location::Remote(tg::location::Remote { name, region: None }),
				)
		});
		Ok(Some(Sandbox {
			location,
			token: None,
		}))
	}

	pub(crate) async fn try_get_authenticated_principal_remote_token(
		&self,
		remote: &str,
	) -> tg::Result<Option<String>> {
		match &self.context.principal {
			tg::Principal::Process(id) => {
				let Some(process) = self.try_get_authenticated_process(id).await? else {
					return Ok(None);
				};
				let Some(location) = process.location.as_ref() else {
					return Ok(None);
				};
				let tg::Location::Remote(location) = location else {
					return Ok(None);
				};
				if location.name != remote {
					return Ok(None);
				}
				Ok(process.inner_token)
			},
			tg::Principal::Runner(id) => {
				if !self
					.server
					.config
					.roles
					.contains(&crate::config::Role::Runner)
				{
					return Ok(None);
				}
				let runner = &self.server.config.runner;
				if runner.remote.as_deref() != Some(remote) {
					return Ok(None);
				}
				if runner.id.as_ref().is_some_and(|runner| runner != id) {
					return Ok(None);
				}
				Ok(runner.token.clone())
			},
			tg::Principal::Sandbox(sandbox) => {
				let Some(sandbox) = self.try_get_authenticated_sandbox(sandbox).await? else {
					return Ok(None);
				};
				let tg::Location::Remote(location) = &sandbox.location else {
					return Ok(None);
				};
				Ok((location.name == remote).then_some(sandbox.token).flatten())
			},
			tg::Principal::Anonymous
			| tg::Principal::Group(_)
			| tg::Principal::Organization(_)
			| tg::Principal::Root
			| tg::Principal::User(_) => Ok(None),
		}
	}
}

impl Server {
	pub(crate) fn create_process_authentication_token(
		&self,
		id: tg::process::Id,
	) -> tg::Result<String> {
		self.create_authentication_token(token::Principal::Process(id))
	}

	pub(crate) fn create_sandbox_authentication_token(
		&self,
		id: tg::sandbox::Id,
	) -> tg::Result<String> {
		self.create_authentication_token(token::Principal::Sandbox(id))
	}

	fn create_authentication_token(&self, principal: token::Principal) -> tg::Result<String> {
		let issued_at = self.clock.unix_timestamp()?;
		let ttl = i64::try_from(self.config.authentication.tokens.ttl.as_secs())
			.map_err(|_| tg::error!("invalid authentication token ttl"))?;
		let expires_at = issued_at
			.checked_add(ttl)
			.ok_or_else(|| tg::error!("invalid authentication token expiration"))?;
		self.create_authentication_token_inner(principal, expires_at)
	}

	fn create_authentication_token_inner(
		&self,
		principal: token::Principal,
		expires_at: i64,
	) -> tg::Result<String> {
		let private_key = self
			.authentication_tokens
			.private_key
			.as_ref()
			.ok_or_else(|| tg::error!("missing the authentication token private key"))?;
		let issued_at = self.clock.unix_timestamp()?;
		let body = token::Body {
			expires_at,
			issued_at,
			principal,
		};
		let token = token::Token::sign(body, private_key)?;

		Ok(token.to_string())
	}

	pub(crate) async fn authenticate(
		&self,
		origin: Origin,
		token: Option<&str>,
	) -> tg::Result<Authentication> {
		let authentication = self.authenticate_inner(token).await?;
		if self.origin_can_authenticate_as(origin, &authentication.principal)? {
			return Ok(authentication);
		}

		Ok(Authentication {
			billing: false,
			principal: tg::Principal::Anonymous,
		})
	}

	async fn authenticate_inner(&self, token: Option<&str>) -> tg::Result<Authentication> {
		if let Some((token, root_token)) =
			token.zip(self.config().authentication.root.token.as_deref())
			&& crate::token::matches(token, root_token)
		{
			return Ok(Authentication {
				billing: false,
				principal: tg::Principal::Root,
			});
		}

		if let Some(value) = token.filter(|value| token::Token::has_prefix(value)) {
			let principal = self
				.authenticate_token(value)
				.unwrap_or(tg::Principal::Anonymous);
			return Ok(Authentication {
				billing: false,
				principal,
			});
		}

		if let Some(mut process) = token.and_then(|token| {
			self.runner
				.state()
				.process_tokens()
				.get(token)
				.map(|process| process.value().clone())
		}) {
			loop {
				if let Some(id) = process.borrow().clone() {
					return Ok(Authentication {
						billing: false,
						principal: tg::Principal::Process(id),
					});
				}
				if process.changed().await.is_err() {
					return Ok(Authentication {
						billing: false,
						principal: tg::Principal::Anonymous,
					});
				}
			}
		}

		if let Some(token) = token {
			match self.authenticate_user(token).await {
				Ok(Some((billing, user))) => {
					return Ok(Authentication {
						billing,
						principal: tg::Principal::User(user.id),
					});
				},
				Ok(None) => (),
				Err(error) => {
					return Err(error);
				},
			}
			if let Some(runner) = self.authenticate_runner(token).await? {
				return Ok(Authentication {
					billing: false,
					principal: tg::Principal::Runner(runner),
				});
			}
		}

		if self.config().authentication.users.is_none() {
			return Ok(Authentication {
				billing: false,
				principal: tg::Principal::Root,
			});
		}

		Ok(Authentication {
			billing: false,
			principal: tg::Principal::Anonymous,
		})
	}

	fn origin_can_authenticate_as(
		&self,
		origin: Origin,
		principal: &tg::Principal,
	) -> tg::Result<bool> {
		let Some(sandbox) = self.try_get_request_origin_sandbox(origin)? else {
			return Ok(true);
		};
		if sandbox.data.network.is_some() {
			return Ok(true);
		}
		let can_authenticate = match principal {
			tg::Principal::Anonymous => true,
			tg::Principal::Group(_)
			| tg::Principal::Organization(_)
			| tg::Principal::Root
			| tg::Principal::Runner(_)
			| tg::Principal::User(_) => false,
			tg::Principal::Process(id) => sandbox.processes.contains_key(id),
			tg::Principal::Sandbox(id) => sandbox.data.id == *id,
		};

		Ok(can_authenticate)
	}

	async fn authenticate_runner(&self, token: &str) -> tg::Result<Option<tg::runner::Id>> {
		let connection = self
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let token = crate::token::hash(token);
		let p = connection.p();
		let statement = format!("select runner from runner_tokens where token = {p}1;");
		let runner = connection
			.query_optional_value_into::<String>(statement.into(), db::params![token])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
			.map(|runner| runner.parse())
			.transpose()?;

		Ok(runner)
	}

	fn authenticate_token(&self, value: &str) -> Option<tg::Principal> {
		let token = value.parse::<token::Token>().ok()?;
		let now = self.clock.unix_timestamp().ok()?;
		token.validate_at(now).ok()?;
		let principal = token.body.principal.clone().into();
		let matches = match &principal {
			tg::Principal::Process(id) => self
				.runner
				.state()
				.processes()
				.get(id)
				.and_then(|sandbox| self.runner.state().sandboxes().get_by_id(sandbox.value()))
				.and_then(|sandbox| {
					sandbox
						.processes
						.get(id)
						.map(|process| process.inner_token == value)
				})
				.unwrap_or(false),
			tg::Principal::Sandbox(id) => self
				.runner
				.state()
				.sandboxes()
				.get_by_id(id)
				.is_some_and(|sandbox| sandbox.token.as_deref() == Some(value)),
			_ => false,
		};
		if matches {
			return Some(principal);
		}
		let public_key = self
			.authentication_tokens
			.public_keys
			.get(&token.metadata.key)?;
		token.verify_at(public_key, now).ok()?;

		Some(principal)
	}

	pub(crate) async fn authenticate_user(
		&self,
		token: &str,
	) -> tg::Result<Option<(bool, tg::user::User)>> {
		let connection = self
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;

		#[derive(db::row::Deserialize)]
		struct UserRow {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::user::Id,
			name: String,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
			stripe_customer_id: Option<String>,
			stripe_default_payment_method_id: Option<String>,
		}
		let token = crate::token::hash(token);
		let p = connection.p();
		let statement = formatdoc!(
			r#"
				select users.id, users.name, specifiers.specifier, users.stripe_customer_id,
					users.stripe_default_payment_method_id
				from users
				join specifiers on specifiers.id = users.id
				join user_tokens on user_tokens."user" = users.id
				where user_tokens.token = {p}1;
			"#
		);
		let params = db::params![token];
		let user = connection
			.query_optional_into::<UserRow>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(user) = user else {
			return Ok(None);
		};

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
		let params = db::params![user.id.to_string()];
		let rows = connection
			.query_all_into::<EmailRow>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let emails = rows.into_iter().map(|row| row.email).collect();
		let billing =
			user.stripe_customer_id.is_some() && user.stripe_default_payment_method_id.is_some();
		let user = tg::User {
			emails,
			id: user.id,
			location: None,
			name: user.name,
			specifier: user.specifier,
			token: None,
		};

		Ok(Some((billing, user)))
	}
}
