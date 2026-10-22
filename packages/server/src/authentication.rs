use {
	crate::{Origin, Server, Session},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

mod token;

pub(crate) mod cache;

#[derive(Clone)]
pub(crate) struct Entry {
	pub billing_ready: Option<bool>,
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

/// The token with which a principal authenticates to a remote, and the process or sandbox that a runner's token claims to act for.
#[derive(Clone, Default)]
pub(crate) struct RemoteToken {
	pub process: Option<tg::process::Id>,
	pub sandbox: Option<tg::sandbox::Id>,
	pub token: Option<String>,
}

#[derive(Clone)]
pub(crate) struct Sandbox {
	pub location: tg::Location,
	pub token: Option<String>,
}

impl RemoteToken {
	#[must_use]
	fn with_token(token: String) -> Self {
		Self {
			process: None,
			sandbox: None,
			token: Some(token),
		}
	}
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
				inner_token: process.inner_token.borrow().clone(),
				location: Some(sandbox.location.clone()),
				retry: process.data.retry,
				sandbox: process
					.data
					.sandbox
					.clone()
					.ok_or_else(|| tg::error!(%id, "the authenticated process has no sandbox"))?,
			}));
		}

		let indexed = self.try_get_process_with_data_from_index(id).await?;
		let data = if let Some(data) = indexed.and_then(|(_, data)| data) {
			data
		} else {
			// A request can arrive before Start has submitted the process to the index.
			let Some(process) = self
				.try_get_process_local_inner(id, false, tg::process::Source::Auto)
				.boxed()
				.await?
			else {
				return Ok(None);
			};
			process.data
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
			sandbox: data
				.sandbox
				.ok_or_else(|| tg::error!(%id, "the authenticated process has no sandbox"))?,
		}))
	}

	pub(crate) async fn try_get_authenticated_sandbox(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<Sandbox>> {
		if let Some(sandbox) = self.server.runner.state().sandboxes().get_by_id(id) {
			let location = sandbox.location.clone();
			return Ok(Some(Sandbox {
				location,
				token: sandbox.token.borrow().clone(),
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

	/// Get the token with which this session's principal authenticates to a remote, and the process or sandbox that the token claims when it is a runner's token that acts for one of its processes or sandboxes.
	pub(crate) async fn try_get_authenticated_principal_remote_token(
		&self,
		remote: &str,
	) -> tg::Result<RemoteToken> {
		match &self.context.principal {
			tg::Principal::Process(id) => {
				let Some(process) = self.try_get_authenticated_process(id).await? else {
					return Ok(RemoteToken::default());
				};
				let Some(location) = process.location.as_ref() else {
					return Ok(RemoteToken::default());
				};
				let tg::Location::Remote(location) = location else {
					return Ok(RemoteToken::default());
				};
				if location.name != remote {
					return Ok(RemoteToken::default());
				}

				// Use the token of a process that this runner runs, or, until a process on the shortcut path receives its token in its control connection's header, claim the process with the runner's token.
				let receiver = self
					.server
					.runner
					.state()
					.processes()
					.get(id)
					.map(|sandbox| sandbox.value().clone())
					.and_then(|sandbox| self.server.runner.state().sandboxes().get_by_id(&sandbox))
					.and_then(|sandbox| {
						sandbox
							.processes
							.get(id)
							.map(|process| process.inner_token.clone())
					});
				let Some(receiver) = receiver else {
					let output = RemoteToken {
						process: None,
						sandbox: None,
						token: process.inner_token,
					};
					return Ok(output);
				};
				let token = receiver.borrow().clone();
				if let Some(token) = token {
					return Ok(RemoteToken::with_token(token));
				}

				// Send the runner's token only to the runner's remote.
				let runner = &self.server.config.runner;
				if runner.remote.as_deref() != Some(remote) {
					return Ok(RemoteToken::default());
				}
				let output = RemoteToken {
					process: Some(id.clone()),
					sandbox: None,
					token: runner.token.clone(),
				};
				Ok(output)
			},
			tg::Principal::Runner(id) => {
				if !self
					.server
					.config
					.roles
					.contains(&crate::config::Role::Runner)
				{
					return Ok(RemoteToken::default());
				}
				let runner = &self.server.config.runner;
				if runner.remote.as_deref() != Some(remote) {
					return Ok(RemoteToken::default());
				}
				if runner.id.as_ref().is_some_and(|runner| runner != id) {
					return Ok(RemoteToken::default());
				}
				let output = RemoteToken {
					process: None,
					sandbox: None,
					token: runner.token.clone(),
				};
				Ok(output)
			},
			tg::Principal::Sandbox(id) => {
				let Some(sandbox) = self.try_get_authenticated_sandbox(id).await? else {
					return Ok(RemoteToken::default());
				};
				let tg::Location::Remote(location) = &sandbox.location else {
					return Ok(RemoteToken::default());
				};
				if location.name != remote {
					return Ok(RemoteToken::default());
				}
				if let Some(token) = sandbox.token {
					return Ok(RemoteToken::with_token(token));
				}

				// Until a sandbox that this runner hosts receives its token in its control connection's header, claim the sandbox with the runner's token, sending it only to the runner's remote.
				if self
					.server
					.runner
					.state()
					.sandboxes()
					.get_by_id(id)
					.is_none()
				{
					return Ok(RemoteToken::default());
				}
				let runner = &self.server.config.runner;
				if runner.remote.as_deref() != Some(remote) {
					return Ok(RemoteToken::default());
				}
				let output = RemoteToken {
					process: None,
					sandbox: Some(id.clone()),
					token: runner.token.clone(),
				};
				Ok(output)
			},
			tg::Principal::Anonymous
			| tg::Principal::Group(_)
			| tg::Principal::Organization(_)
			| tg::Principal::Root
			| tg::Principal::User(_) => Ok(RemoteToken::default()),
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
		let issued_at = self.clock.unix_timestamp();
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
		let issued_at = self.clock.unix_timestamp();
		let body = token::Body {
			expires_at,
			issued_at,
			principal,
		};
		let token = token::Token::sign(body, private_key)?;

		Ok(token.to_string())
	}

	/// Authenticate a request, and authenticate a runner's request that claims to act for one of its processes or sandboxes as the process or sandbox, once its control service verifies that the runner started the process or hosts the sandbox.
	pub(crate) async fn authenticate_with_claim(
		&self,
		context: &crate::Context,
		token: Option<&str>,
		process: Option<tg::process::Id>,
		sandbox: Option<tg::sandbox::Id>,
	) -> tg::Result<Entry> {
		let authentication = self.authenticate(context.origin, token).await?;
		if process.is_none() && sandbox.is_none() {
			return Ok(authentication);
		}
		let tg::Principal::Runner(_) = &authentication.principal else {
			return Err(tg::error!(
				unauthorized,
				"only a runner may claim to act for a process or a sandbox"
			));
		};
		let context = crate::Context {
			principal: authentication.principal.clone(),
			..context.clone()
		};
		let session = self.session(&context);
		let principal = match (process, sandbox) {
			(Some(process), None) => {
				session
					.authorize_with_process_control_service(&process, &authentication.principal)
					.await?;
				tg::Principal::Process(process)
			},
			(None, Some(sandbox)) => {
				session
					.authorize_with_sandbox_control_service(&sandbox, &authentication.principal)
					.await?;
				tg::Principal::Sandbox(sandbox)
			},
			(Some(_), Some(_)) => {
				return Err(tg::error!(
					argument,
					"a request may not claim both a process and a sandbox"
				));
			},
			(None, None) => unreachable!(),
		};
		let authentication = Entry {
			billing_ready: authentication.billing_ready,
			principal,
		};

		Ok(authentication)
	}

	pub(crate) async fn authenticate(
		&self,
		origin: Origin,
		token: Option<&str>,
	) -> tg::Result<Entry> {
		let authentication = self.authenticate_inner(token).await?;
		if self.origin_can_authenticate_as(origin, &authentication.principal)? {
			return Ok(authentication);
		}

		Ok(Entry {
			billing_ready: None,
			principal: tg::Principal::Anonymous,
		})
	}

	async fn authenticate_inner(&self, token: Option<&str>) -> tg::Result<Entry> {
		if let Some((token, root_token)) =
			token.zip(self.config().authentication.root.token.as_deref())
			&& crate::token::matches(token, root_token)
		{
			return Ok(Entry {
				billing_ready: None,
				principal: tg::Principal::Root,
			});
		}

		if let Some(value) = token.filter(|value| token::Token::has_prefix(value)) {
			let principal = self
				.authenticate_token(value)
				.unwrap_or(tg::Principal::Anonymous);
			return Ok(Entry {
				billing_ready: None,
				principal,
			});
		}

		if let Some((sandbox_index, id)) = token.and_then(|token| {
			self.runner
				.state()
				.process_for_token()
				.get(token)
				.map(|process| process.value().clone())
		}) {
			let state = self.runner.state();
			let Some(sandbox) = state.sandboxes().get(sandbox_index) else {
				return Ok(Entry {
					billing_ready: None,
					principal: tg::Principal::Anonymous,
				});
			};
			let Some(process) = sandbox.processes.get(&id) else {
				return Ok(Entry {
					billing_ready: None,
					principal: tg::Principal::Anonymous,
				});
			};
			let index_task = process.index_task.clone();
			drop(process);
			drop(sandbox);
			index_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the process index task panicked"))?
				.map_err(|error| tg::error!(!error, %id, "failed to index the process"))?;
			return Ok(Entry {
				billing_ready: None,
				principal: tg::Principal::Process(id),
			});
		}

		if let Some(token) = token
			&& let Some(entry) = self.authenticate_token_cached(token).await?
		{
			return Ok(entry);
		}

		if self.config().authentication.users.is_none() {
			return Ok(Entry {
				billing_ready: None,
				principal: tg::Principal::Root,
			});
		}

		Ok(Entry {
			billing_ready: None,
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
		if sandbox.create.network.is_some() {
			return Ok(true);
		}
		let can_authenticate = match principal {
			tg::Principal::Anonymous => true,
			tg::Principal::Group(_)
			| tg::Principal::Organization(_)
			| tg::Principal::Root
			| tg::Principal::Runner(_)
			| tg::Principal::User(_) => false,
			tg::Principal::Process(id) => sandbox.processes.get(id).is_some(),
			tg::Principal::Sandbox(id) => sandbox.id == *id,
		};

		Ok(can_authenticate)
	}

	fn authenticate_token(&self, value: &str) -> Option<tg::Principal> {
		let token = value.parse::<token::Token>().ok()?;
		let now = self.clock.unix_timestamp();
		let clock_skew = self.config.authentication.tokens.clock_skew;
		token.validate_at(now, clock_skew).ok()?;
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
						.map(|process| process.inner_token.borrow().as_deref() == Some(value))
				})
				.unwrap_or(false),
			tg::Principal::Sandbox(id) => self
				.runner
				.state()
				.sandboxes()
				.get_by_id(id)
				.is_some_and(|sandbox| sandbox.token.borrow().as_deref() == Some(value)),
			_ => false,
		};
		if matches {
			return Some(principal);
		}
		let public_key = self
			.authentication_tokens
			.public_keys
			.get(&token.metadata.key)?;
		token.verify_at(public_key, now, clock_skew).ok()?;

		Some(principal)
	}

	async fn authenticate_token_cached(&self, token: &str) -> tg::Result<Option<Entry>> {
		let hash = crate::token::hash_bytes(token);
		if let Some(entry) = self.authentication_cache.get(&hash).await {
			return Ok(Some(entry));
		}
		let entry = self.authenticate_token_from_database(&hash).await?;
		if let Some(entry) = &entry {
			self.authentication_cache.insert(hash, entry.clone()).await;
		}

		Ok(entry)
	}

	async fn authenticate_token_from_database(
		&self,
		hash: &cache::Hash,
	) -> tg::Result<Option<Entry>> {
		let hash = blake3::Hash::from_bytes(*hash).to_hex().to_string();
		let entry = self
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let hash = hash.clone();
				async move {
					Self::authenticate_token_from_database_with_transaction(transaction, &hash)
						.await
				}
				.boxed()
			})
			.await?;

		Ok(entry)
	}

	async fn authenticate_token_from_database_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		hash: &str,
	) -> tg::Result<ControlFlow<Option<Entry>, crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			billing_ready: bool,
			id: String,
			kind: String,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select users.billing_ready, users.id, 'user' as kind
				from user_tokens join users on users.id = user_tokens."user"
				where user_tokens.token = {p}1
				union all
				select false as billing_ready, runner_tokens.runner as id, 'runner' as kind
				from runner_tokens join runners on runners.id = runner_tokens.runner
				where runner_tokens.token = {p}1
				order by kind desc limit 1;
			"#
		);
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![hash])
			.await;
		let row = crate::database::retry!(result, "failed to authenticate the token");
		let Some(row) = row else {
			return Ok(ControlFlow::Break(None));
		};
		let principal = match row.kind.as_str() {
			"runner" => tg::Principal::Runner(row.id.parse()?),
			"user" => tg::Principal::User(row.id.parse()?),
			_ => return Err(tg::error!("invalid token principal kind")),
		};
		let entry = Entry {
			billing_ready: row.billing_ready.then_some(true),
			principal,
		};

		Ok(ControlFlow::Break(Some(entry)))
	}
}
