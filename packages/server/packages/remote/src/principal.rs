use {
	crate::Component, futures::FutureExt as _, std::ops::ControlFlow, tangram_client::prelude::*,
	tangram_database as db, tangram_index::prelude::*, tangram_server_core::Session,
};

impl Component {
	pub(crate) fn verify_request_can_mutate_remotes(session: &Session) -> tg::Result<()> {
		if matches!(
			session.context.principal,
			tg::Principal::Process(_) | tg::Principal::Sandbox(_)
		) {
			return Err(tg::error!("unauthorized"));
		}

		Ok(())
	}

	pub(crate) async fn resolve_remote_arg_principal(
		&self,
		session: &Session,
		principal: Option<tg::principal::Selector>,
	) -> tg::Result<Option<tg::Principal>> {
		if let Some(principal) = principal {
			let Some(principal) =
				Self::resolve_remote_principal_selector(session, &principal).await?
			else {
				return Err(tg::error!("failed to resolve the remote principal"));
			};
			Self::authorize_remote_principal(session, principal.as_ref()).await?;
			return Ok(principal);
		}
		let principal = match &session.context.principal {
			tg::Principal::Process(_) | tg::Principal::Sandbox(_) => {
				self.resolve_remote_context_principal(session, &session.context.principal)
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

	async fn authorize_remote_principal(
		session: &Session,
		principal: Option<&tg::Principal>,
	) -> tg::Result<()> {
		let Some(principal) = principal else {
			if matches!(session.context.principal, tg::Principal::Root) {
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
		let permission = tangram_server_grant_common::write_permission_for_resource(&id)?;
		let authorization = session
			.authorize(id, permission)
			.await?
			.check_exhaustion()?;
		let permissions = authorization.permissions;
		if !permissions.contains(permission) {
			return Err(tg::error!("unauthorized"));
		}
		Ok(())
	}

	async fn resolve_remote_principal_selector(
		session: &Session,
		principal: &tg::principal::Selector,
	) -> tg::Result<Option<Option<tg::Principal>>> {
		let principal = principal.clone();
		session
			.server
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
		transaction: &tangram_server_core::database::Transaction<'_>,
		principal: &tg::principal::Selector,
	) -> tg::Result<ControlFlow<Option<Option<tg::Principal>>, tangram_server_core::database::Error>>
	{
		match principal {
			tg::principal::Selector::Principal(principal) => match principal {
				tg::Principal::Group(id) => {
					let id = id.clone();
					let specifier = match Session::try_get_specifier_for_id_with_transaction(
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
					let specifier = match Session::try_get_specifier_for_id_with_transaction(
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
					let specifier = match Session::try_get_specifier_for_id_with_transaction(
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
				let id = match Session::try_get_id_for_specifier_with_transaction(
					transaction,
					specifier,
				)
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
		session: &Session,
		principal: &tg::Principal,
	) -> tg::Result<tg::Principal> {
		self.try_resolve_remote_context_principal(session, principal)
			.await?
			.ok_or_else(|| tg::error!("failed to resolve the sandbox owner"))
	}

	pub(crate) async fn try_resolve_remote_context_principal(
		&self,
		session: &Session,
		principal: &tg::Principal,
	) -> tg::Result<Option<tg::Principal>> {
		let owner = match principal {
			tg::Principal::Process(id) => session
				.server
				.components
				.runner
				.state()
				.try_get_process_sandbox(id)
				.and_then(|sandbox| {
					session
						.server
						.components
						.runner
						.state()
						.try_get_sandbox(&sandbox)
				})
				.map(|sandbox| sandbox.data.owner),
			tg::Principal::Sandbox(id) => session
				.server
				.components
				.runner
				.state()
				.try_get_sandbox(id)
				.map(|sandbox| sandbox.data.owner),
			_ => None,
		};
		if let Some(owner) = owner {
			return Ok(Some(owner.unwrap_or(tg::Principal::Root)));
		}

		let owner = match principal {
			tg::Principal::Process(id) => {
				let sandbox = session
					.server
					.index
					.try_get_process(id)
					.await
					.map_err(
						|error| tg::error!(!error, %id, "failed to get the process from the index"),
					)?
					.map(|process| {
						process.sandbox.ok_or_else(
							|| tg::error!(%id, "the authenticated process has no sandbox"),
						)
					})
					.transpose()?;
				if let Some(sandbox) = sandbox {
					session
						.server
						.index
						.try_get_sandbox(&sandbox)
						.await
						.map_err(
							|error| tg::error!(!error, %sandbox, "failed to get the sandbox from the index"),
						)?
						.and_then(|sandbox| sandbox.data)
						.map(|sandbox| sandbox.data.owner)
				} else {
					None
				}
			},
			tg::Principal::Sandbox(id) => session
				.server
				.index
				.try_get_sandbox(id)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to get the sandbox from the index"),
				)?
				.and_then(|sandbox| sandbox.data)
				.map(|sandbox| sandbox.data.owner),
			_ => return Ok(Some(principal.clone())),
		};
		let owner = owner.map(|owner| owner.unwrap_or(tg::Principal::Root));

		Ok(owner)
	}
}
