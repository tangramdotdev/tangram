use {
	crate::Component,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_server_core::{
		Server, Session,
		authentication::{Entry, Process, RemoteToken, Sandbox, token},
		context::Origin,
	},
};

impl Component {
	pub async fn try_get_authenticated_process(
		&self,
		session: &Session,
		id: &tg::process::Id,
	) -> tg::Result<Option<Process>> {
		if let Some(sandbox) = session
			.server
			.components
			.runner
			.state()
			.processes()
			.get(id)
			.map(|sandbox| sandbox.value().clone())
			&& let Some(sandbox) = session
				.server
				.components
				.runner
				.state()
				.sandboxes()
				.get_by_id(&sandbox)
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

		let indexed = session
			.server
			.components
			.process
			.try_get_process_with_data_from_index(session, id)
			.await?;
		let data = if let Some(data) = indexed.and_then(|(_, data)| data) {
			data
		} else {
			// A request can arrive before Start has submitted the process to the index.
			let Some(process) = session
				.server
				.components
				.process
				.try_get_process_local_inner(session, id, false, tg::process::Source::Auto)
				.boxed()
				.await?
			else {
				return Ok(None);
			};
			process.data
		};
		let location = session
			.server
			.config
			.roles
			.contains(&tangram_server_core::config::Role::Runner)
			.then(|| session.server.config.runner.remote.clone())
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
		session: &Session,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<Sandbox>> {
		if let Some(sandbox) = session
			.server
			.components
			.runner
			.state()
			.sandboxes()
			.get_by_id(id)
		{
			let location = sandbox.location.clone();
			return Ok(Some(Sandbox {
				location,
				token: sandbox.token.borrow().clone(),
			}));
		}

		let Some(sandbox) = session
			.server
			.components
			.sandbox
			.try_get_sandbox_from_index(session, id)
			.await?
		else {
			return Ok(None);
		};
		let Some(data) = sandbox.data else {
			return Ok(None);
		};
		let location = data.location.unwrap_or_else(|| {
			session
				.server
				.config
				.roles
				.contains(&tangram_server_core::config::Role::Runner)
				.then(|| session.server.config.runner.remote.clone())
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
		session: &Session,
		remote: &str,
	) -> tg::Result<RemoteToken> {
		match &session.context.principal {
			tg::Principal::Process(id) => {
				let Some(process) = self.try_get_authenticated_process(session, id).await? else {
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
				let receiver = session
					.server
					.components
					.runner
					.state()
					.processes()
					.get(id)
					.map(|sandbox| sandbox.value().clone())
					.and_then(|sandbox| {
						session
							.server
							.components
							.runner
							.state()
							.sandboxes()
							.get_by_id(&sandbox)
					})
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
				let runner = &session.server.config.runner;
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
				if !session
					.server
					.config
					.roles
					.contains(&tangram_server_core::config::Role::Runner)
				{
					return Ok(RemoteToken::default());
				}
				let runner = &session.server.config.runner;
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
				let Some(sandbox) = self.try_get_authenticated_sandbox(session, id).await? else {
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
				if session
					.server
					.components
					.runner
					.state()
					.sandboxes()
					.get_by_id(id)
					.is_none()
				{
					return Ok(RemoteToken::default());
				}
				let runner = &session.server.config.runner;
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

impl Component {
	pub fn create_process_authentication_token(
		&self,
		server: &Server,
		id: tg::process::Id,
	) -> tg::Result<String> {
		Self::create_authentication_token(server, token::Principal::Process(id))
	}

	pub fn create_sandbox_authentication_token(
		&self,
		server: &Server,
		id: tg::sandbox::Id,
	) -> tg::Result<String> {
		Self::create_authentication_token(server, token::Principal::Sandbox(id))
	}

	fn create_authentication_token(
		server: &Server,
		principal: token::Principal,
	) -> tg::Result<String> {
		let issued_at = server.clock.unix_timestamp();
		let ttl = i64::try_from(server.config.authentication.tokens.ttl.as_secs())
			.map_err(|_| tg::error!("invalid authentication token ttl"))?;
		let expires_at = issued_at
			.checked_add(ttl)
			.ok_or_else(|| tg::error!("invalid authentication token expiration"))?;
		Self::create_authentication_token_inner(server, principal, expires_at)
	}

	fn create_authentication_token_inner(
		server: &Server,
		principal: token::Principal,
		expires_at: i64,
	) -> tg::Result<String> {
		let private_key = server
			.authentication_tokens
			.private_key
			.as_ref()
			.ok_or_else(|| tg::error!("missing the authentication token private key"))?;
		let issued_at = server.clock.unix_timestamp();
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
		server: &Server,
		context: &tangram_server_core::context::Context,
		token: Option<&str>,
		process: Option<tg::process::Id>,
		sandbox: Option<tg::sandbox::Id>,
	) -> tg::Result<Entry> {
		let authentication = self.authenticate(server, context.origin, token).await?;
		if process.is_none() && sandbox.is_none() {
			return Ok(authentication);
		}
		let tg::Principal::Runner(_) = &authentication.principal else {
			return Err(tg::error!(
				unauthorized,
				"only a runner may claim to act for a process or a sandbox"
			));
		};
		let context = tangram_server_core::context::Context {
			principal: authentication.principal.clone(),
			..context.clone()
		};
		let session = server.session(&context);
		let principal = match (process, sandbox) {
			(Some(process), None) => {
				session
					.server
					.components
					.process
					.authorize_with_process_control_service(
						&session,
						&process,
						&authentication.principal,
					)
					.await?;
				tg::Principal::Process(process)
			},
			(None, Some(sandbox)) => {
				session
					.server
					.components
					.sandbox
					.authorize_with_sandbox_control_service(
						&session,
						&sandbox,
						&authentication.principal,
					)
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

	pub async fn authenticate(
		&self,
		server: &Server,
		origin: Origin,
		token: Option<&str>,
	) -> tg::Result<Entry> {
		let authentication = self.authenticate_inner(server, token).await?;
		if Self::origin_can_authenticate_as(server, origin, &authentication.principal)? {
			return Ok(authentication);
		}

		Ok(Entry {
			billing_ready: None,
			principal: tg::Principal::Anonymous,
		})
	}

	async fn authenticate_inner(&self, server: &Server, token: Option<&str>) -> tg::Result<Entry> {
		if let Some((token, root_token)) =
			token.zip(server.config().authentication.root.token.as_deref())
			&& tangram_server_core::token::matches(token, root_token)
		{
			return Ok(Entry {
				billing_ready: None,
				principal: tg::Principal::Root,
			});
		}

		if let Some(value) = token.filter(|value| token::Token::has_prefix(value)) {
			let principal =
				Self::authenticate_token(server, value).unwrap_or(tg::Principal::Anonymous);
			return Ok(Entry {
				billing_ready: None,
				principal,
			});
		}

		if let Some((sandbox_index, id)) = token.and_then(|token| {
			server
				.components
				.runner
				.state()
				.process_for_token()
				.get(token)
				.map(|process| process.value().clone())
		}) {
			let state = server.components.runner.state();
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
			&& let Some(entry) = self.authenticate_token_cached(server, token).await?
		{
			return Ok(entry);
		}

		if server.config().authentication.users.is_none() {
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
		server: &Server,
		origin: Origin,
		principal: &tg::Principal,
	) -> tg::Result<bool> {
		let Some(sandbox) = server.try_get_request_origin_sandbox(origin)? else {
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

	fn authenticate_token(server: &Server, value: &str) -> Option<tg::Principal> {
		let token = value.parse::<token::Token>().ok()?;
		let now = server.clock.unix_timestamp();
		let clock_skew = server.config.authentication.tokens.clock_skew;
		token.validate_at(now, clock_skew).ok()?;
		let principal = token.body.principal.clone().into();
		let matches = match &principal {
			tg::Principal::Process(id) => server
				.components
				.runner
				.state()
				.processes()
				.get(id)
				.and_then(|sandbox| {
					server
						.components
						.runner
						.state()
						.sandboxes()
						.get_by_id(sandbox.value())
				})
				.and_then(|sandbox| {
					sandbox
						.processes
						.get(id)
						.map(|process| process.inner_token.borrow().as_deref() == Some(value))
				})
				.unwrap_or(false),
			tg::Principal::Sandbox(id) => server
				.components
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
		let public_key = server
			.authentication_tokens
			.public_keys
			.get(&token.metadata.key)?;
		token.verify_at(public_key, now, clock_skew).ok()?;

		Some(principal)
	}

	async fn authenticate_token_cached(
		&self,
		server: &Server,
		token: &str,
	) -> tg::Result<Option<Entry>> {
		let hash = tangram_server_core::token::hash_bytes(token);
		if let Some(entry) = self.cache.get(&hash).await {
			return Ok(Some(entry));
		}
		let entry = self.authenticate_token_from_database(server, &hash).await?;
		if let Some(entry) = &entry {
			self.cache.insert(hash, entry.clone()).await;
		}

		Ok(entry)
	}

	async fn authenticate_token_from_database(
		&self,
		server: &Server,
		hash: &tangram_server_core::authentication::cache::Hash,
	) -> tg::Result<Option<Entry>> {
		let hash = blake3::Hash::from_bytes(*hash).to_hex().to_string();
		let entry = server
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
		transaction: &tangram_server_core::database::Transaction<'_>,
		hash: &str,
	) -> tg::Result<ControlFlow<Option<Entry>, tangram_server_core::database::Error>> {
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
		let row = tangram_server_core::database::retry!(result, "failed to authenticate the token");
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
