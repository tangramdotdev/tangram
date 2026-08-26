use {
	crate::{Server, Session},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_index::prelude::*,
	tangram_uri::Uri,
};

pub(crate) mod cache;

pub mod delete;
pub mod get;
pub mod list;
pub mod put;

impl Session {
	async fn authorize_remote_principal(
		&self,
		principal: Option<&tg::Principal>,
	) -> tg::Result<()> {
		let Some(principal) = principal else {
			if matches!(self.context.principal, tg::Principal::Root) {
				return Ok(());
			}
			return Err(tg::error!("unauthorized"));
		};
		let id: tg::Id = match principal {
			tg::Principal::Group(id) => id.clone().into(),
			tg::Principal::Organization(id) => id.clone().into(),
			tg::Principal::User(id) => id.clone().into(),
			_ => return Err(tg::error!("invalid remote principal")),
		};
		let permission = Self::write_permission_for_resource(&id)?;
		let Some(permissions) = self.authorize(id, permission).await? else {
			return Err(tg::error!("unauthorized"));
		};
		if !permissions.contains(permission) {
			return Err(tg::error!("unauthorized"));
		}
		Ok(())
	}

	pub(crate) fn verify_request_can_mutate_remotes(&self) -> tg::Result<()> {
		if matches!(
			self.context.principal,
			tg::Principal::Process(_) | tg::Principal::Sandbox(_)
		) {
			return Err(tg::error!("unauthorized"));
		}

		Ok(())
	}

	async fn resolve_remote_arg_principal(
		&self,
		principal: Option<tg::principal::Selector>,
	) -> tg::Result<Option<tg::Principal>> {
		if let Some(principal) = principal {
			let Some(principal) = self.resolve_remote_principal_selector(&principal).await? else {
				return Err(tg::error!("failed to resolve the remote principal"));
			};
			self.authorize_remote_principal(principal.as_ref()).await?;
			return Ok(principal);
		}
		let principal = match &self.context.principal {
			tg::Principal::Process(_) | tg::Principal::Sandbox(_) => {
				self.resolve_remote_context_principal(&self.context.principal)
					.await?
			},
			principal => principal.clone(),
		};
		let principal = match principal {
			tg::Principal::Anonymous => return Err(tg::error!("unauthorized")),
			tg::Principal::Group(id) => Some(tg::Principal::Group(id)),
			tg::Principal::Organization(id) => Some(tg::Principal::Organization(id)),
			tg::Principal::Process(_) => return Err(tg::error!("unauthorized")),
			tg::Principal::Root => None,
			tg::Principal::Runner(_) => return Err(tg::error!("unauthorized")),
			tg::Principal::Sandbox(_) => return Err(tg::error!("unauthorized")),
			tg::Principal::User(id) => Some(tg::Principal::User(id)),
		};
		Ok(principal)
	}

	async fn resolve_remote_principal_selector(
		&self,
		principal: &tg::principal::Selector,
	) -> tg::Result<Option<Option<tg::Principal>>> {
		let principal = principal.clone();
		self.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let principal = principal.clone();
				async move {
					Self::resolve_remote_principal_selector_with_transaction(
						transaction,
						&principal,
					)
					.await
				}
				.boxed()
			})
			.await
	}

	async fn resolve_remote_principal_selector_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		principal: &tg::principal::Selector,
	) -> tg::Result<ControlFlow<Option<Option<tg::Principal>>, crate::database::Error>> {
		match principal {
			tg::principal::Selector::Principal(principal) => match principal {
				tg::Principal::Group(id) => {
					let id = id.clone();
					let specifier = match Self::try_get_specifier_for_id_with_transaction(
						transaction,
						&id.clone().into(),
					)
					.await?
					{
						ControlFlow::Break(specifier) => specifier,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
					Ok(ControlFlow::Break(
						specifier.map(|_| Some(tg::Principal::Group(id))),
					))
				},
				tg::Principal::Organization(id) => {
					let id = id.clone();
					let specifier = match Self::try_get_specifier_for_id_with_transaction(
						transaction,
						&id.clone().into(),
					)
					.await?
					{
						ControlFlow::Break(specifier) => specifier,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
					Ok(ControlFlow::Break(
						specifier.map(|_| Some(tg::Principal::Organization(id))),
					))
				},
				tg::Principal::Root => Ok(ControlFlow::Break(Some(None))),
				tg::Principal::User(id) => {
					let id = id.clone();
					let specifier = match Self::try_get_specifier_for_id_with_transaction(
						transaction,
						&id.clone().into(),
					)
					.await?
					{
						ControlFlow::Break(specifier) => specifier,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
					Ok(ControlFlow::Break(
						specifier.map(|_| Some(tg::Principal::User(id))),
					))
				},
				tg::Principal::Process(_)
				| tg::Principal::Anonymous
				| tg::Principal::Runner(_)
				| tg::Principal::Sandbox(_) => Err(tg::error!("invalid remote principal")),
			},
			tg::principal::Selector::Specifier(specifier) => {
				let id =
					match Self::try_get_id_for_specifier_with_transaction(transaction, specifier)
						.await?
					{
						ControlFlow::Break(id) => id,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
				let Some(id) = id else {
					return Ok(ControlFlow::Break(None));
				};
				let principal = match id.kind() {
					tg::id::Kind::Group => Some(tg::Principal::Group(id.try_into()?)),
					tg::id::Kind::Organization => Some(tg::Principal::Organization(id.try_into()?)),
					tg::id::Kind::User => Some(tg::Principal::User(id.try_into()?)),
					_ => return Err(tg::error!("invalid remote principal")),
				};
				Ok(ControlFlow::Break(Some(principal)))
			},
		}
	}

	pub(crate) async fn resolve_remote_context_principal(
		&self,
		principal: &tg::Principal,
	) -> tg::Result<tg::Principal> {
		self.try_resolve_remote_context_principal(principal)
			.await?
			.ok_or_else(|| tg::error!("failed to resolve the sandbox owner"))
	}

	pub(crate) async fn try_resolve_remote_context_principal(
		&self,
		principal: &tg::Principal,
	) -> tg::Result<Option<tg::Principal>> {
		let owner = match principal {
			tg::Principal::Process(id) => self
				.server
				.runner
				.state()
				.try_get_process_sandbox(id)
				.and_then(|sandbox| self.server.runner.state().try_get_sandbox(&sandbox))
				.map(|sandbox| sandbox.owner),
			tg::Principal::Sandbox(id) => self
				.server
				.runner
				.state()
				.try_get_sandbox(id)
				.map(|sandbox| sandbox.owner),
			_ => None,
		};
		if let Some(owner) = owner {
			return Ok(Some(owner.unwrap_or(tg::Principal::Root)));
		}

		let owner = match principal {
			tg::Principal::Process(id) => {
				let sandbox = self
					.server
					.index
					.try_get_process(id)
					.await
					.map_err(
						|error| tg::error!(!error, %id, "failed to get the process from the index"),
					)?
					.and_then(|process| {
						process
							.sandbox
							.or_else(|| process.data.map(|data| data.sandbox))
					});
				if let Some(sandbox) = sandbox {
					self.server
						.index
						.try_get_sandbox(&sandbox)
						.await
						.map_err(
							|error| tg::error!(!error, %sandbox, "failed to get the sandbox from the index"),
						)?
						.and_then(|sandbox| sandbox.data)
						.map(|sandbox| sandbox.owner)
				} else {
					None
				}
			},
			tg::Principal::Sandbox(id) => self
				.server
				.index
				.try_get_sandbox(id)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to get the sandbox from the index"),
				)?
				.and_then(|sandbox| sandbox.data)
				.map(|sandbox| sandbox.owner),
			_ => return Ok(Some(principal.clone())),
		};
		let owner = owner.map(|owner| owner.unwrap_or(tg::Principal::Root));

		Ok(owner)
	}

	pub async fn get_remote_session(&self, remote: &str) -> tg::Result<tg::Session> {
		self.try_get_remote_session(remote)
			.await?
			.ok_or_else(|| tg::error!("failed to find the remote"))
	}

	pub(crate) async fn get_remote_session_for_process(
		&self,
		remote: &str,
	) -> tg::Result<tg::Session> {
		self.verify_request_can_access_remote_process()?;
		self.try_get_remote_session_inner(remote)
			.await?
			.ok_or_else(|| tg::error!("failed to find the remote"))
	}

	pub async fn try_get_remote_session(&self, remote: &str) -> tg::Result<Option<tg::Session>> {
		self.verify_request_with_network_access()?;
		self.try_get_remote_session_inner(remote).await
	}

	async fn try_get_remote_session_inner(&self, remote: &str) -> tg::Result<Option<tg::Session>> {
		let Some(output) = self
			.try_get_remote(remote, tg::remote::get::Arg::default())
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote"))?
		else {
			return Ok(None);
		};
		let client = self
			.server
			.get_or_create_remote_client(output.url)
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote client"))?;
		let mut context = client.context().clone();
		context.set_token(
			self.try_get_authenticated_principal_remote_token(remote)
				.await?
				.or(output.token),
		);
		context.set_trusted(output.trusted);
		let session = client.session(&context);
		Ok(Some(session))
	}

	pub(crate) async fn set_remote_token(&self, remote: &str, token: String) -> tg::Result<()> {
		self.verify_request_can_mutate_remotes()?;
		let principal = self.resolve_remote_arg_principal(None).await?;
		let principal = principal.as_ref().map(ToString::to_string);
		let remote = remote.to_owned();
		let n = self
			.server
			.database
			.run(|transaction| {
				let principal = principal.clone();
				let remote = remote.clone();
				let token = token.clone();
				async move {
					Self::set_remote_token_with_transaction(
						transaction,
						&remote,
						principal.as_deref(),
						&token,
					)
					.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to set the remote token"))?;
		if n == 0 {
			return Err(tg::error!("failed to find the remote"));
		}
		Ok(())
	}

	async fn set_remote_token_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		remote: &str,
		principal: Option<&str>,
		token: &str,
	) -> tg::Result<ControlFlow<u64, crate::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				update remotes
				set token = {p}3
				where name = {p}1 and (
					(principal is null and cast({p}2 as text) is null) or
					principal = {p}2
				);
			",
		);
		let params = tangram_database::params![remote, principal, token];
		let result = transaction.execute(statement.into(), params).await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(n))
	}
}

impl Server {
	pub(crate) fn get_or_create_remote_client(&self, url: Uri) -> tg::Result<tg::Client> {
		if let Some(client) = self.remote_clients.get(&url) {
			return Ok(client.clone());
		}
		let client = self.create_remote_client(url.clone())?;
		self.remote_clients.insert(url, client.clone());
		Ok(client)
	}

	pub(crate) fn create_remote_client(&self, url: Uri) -> tg::Result<tg::Client> {
		tg::Client::new(tg::Arg {
			http: tg::Http::default(),
			url: Some(url),
			version: Some(self.version.clone()),
			token: None,
			pool: None,
			reconnect: None,
			retry: None,
			sync: tg::sync::Config {
				max_frame_size: self.config().sync.max_frame_size,
			},
		})
	}
}
