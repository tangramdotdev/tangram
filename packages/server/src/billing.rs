use {crate::Session, tangram_client::prelude::*, tangram_index::prelude::*};

mod stripe;

pub(super) use stripe::{CreateCustomerArg, Stripe};

impl Session {
	pub(crate) async fn storage_owner_for_specifier(
		&self,
		specifier: &tg::Specifier,
	) -> tg::Result<Option<tangram_index::storage::Owner>> {
		let prefix = specifier
			.prefixes()
			.next()
			.expect("a specifier should have a component");
		let principal = match self.server.index.try_get_node(&prefix).await? {
			Some(id) => match id.kind() {
				tg::id::Kind::Group => Some(tg::Principal::Group(id.try_into()?)),
				tg::id::Kind::Organization => Some(tg::Principal::Organization(id.try_into()?)),
				tg::id::Kind::User => Some(tg::Principal::User(id.try_into()?)),
				_ => None,
			},
			None => None,
		};
		if let Some(principal) = principal
			&& let Some(owner) = self.storage_owner(&principal).await?
		{
			return Ok(Some(owner));
		}

		self.storage_owner(&self.context.principal).await
	}

	pub(crate) async fn storage_owner(
		&self,
		principal: &tg::Principal,
	) -> tg::Result<Option<tangram_index::storage::Owner>> {
		let mut principal = match principal {
			tg::Principal::Process(_) | tg::Principal::Sandbox(_) => {
				let Ok(principal) = self.resolve_remote_context_principal(principal).await else {
					return Ok(None);
				};
				principal
			},
			_ => principal.clone(),
		};
		loop {
			match principal {
				tg::Principal::Group(id) => {
					let group = self
						.server
						.index
						.try_get_group(&id)
						.await?
						.ok_or_else(|| tg::error!(%id, "failed to find the storage owner"))?;
					let specifier = group
						.specifier
						.prefixes()
						.next()
						.expect("a specifier should have a component");
					let id = self
						.server
						.index
						.try_get_node(&specifier)
						.await?
						.ok_or_else(|| tg::error!("the group does not have a storage owner"))?;
					principal = match id.kind() {
						tg::id::Kind::Organization => tg::Principal::Organization(id.try_into()?),
						tg::id::Kind::User => tg::Principal::User(id.try_into()?),
						_ => return Ok(None),
					};
				},
				tg::Principal::Organization(id) => {
					return Ok(Some(tangram_index::storage::Owner::Organization(id)));
				},
				tg::Principal::User(id) => {
					return Ok(Some(tangram_index::storage::Owner::User(id)));
				},
				tg::Principal::Anonymous | tg::Principal::Root | tg::Principal::Runner(_) => {
					return Ok(None);
				},
				tg::Principal::Process(_) | tg::Principal::Sandbox(_) => return Ok(None),
			}
		}
	}

	pub(crate) async fn verify_billing(&self, owner: Option<&tg::Principal>) -> tg::Result<()> {
		if self.server.billing.is_none() {
			return Ok(());
		}
		let Some(owner) = owner else {
			return Ok(());
		};
		if matches!(owner, tg::Principal::Root) {
			return Ok(());
		}

		let (billing, command) = self.billing(owner.clone()).await?;
		if !billing {
			return Err(tg::error!(
				"billing is not ready for the sandbox owner; run `{command}`"
			));
		}

		Ok(())
	}

	async fn billing(&self, mut owner: tg::Principal) -> tg::Result<(bool, String)> {
		loop {
			match owner {
				tg::Principal::Group(id) => {
					let group = self
						.server
						.index
						.try_get_group(&id)
						.await?
						.ok_or_else(|| tg::error!(%id, "failed to find the sandbox owner"))?;
					let specifier = group
						.specifier
						.prefixes()
						.next()
						.expect("a specifier should have a component");
					let id = self
						.server
						.index
						.try_get_id_for_specifier(&specifier)
						.await?
						.ok_or_else(|| {
							tg::error!("the sandbox owner does not have a billing account")
						})?;
					owner = match id.kind() {
						tg::id::Kind::Organization => tg::Principal::Organization(id.try_into()?),
						tg::id::Kind::User => tg::Principal::User(id.try_into()?),
						_ => {
							return Err(tg::error!(
								"the sandbox owner does not have a billing account"
							));
						},
					};
				},
				tg::Principal::Organization(id) => {
					let organization = self
						.server
						.index
						.try_get_organization(&id)
						.await?
						.ok_or_else(|| tg::error!(%id, "failed to find the sandbox owner"))?;
					let command = format!("tg organization billing manage {id}");

					return Ok((organization.billing, command));
				},
				tg::Principal::User(id) => {
					let billing = if self.context.principal == tg::Principal::User(id.clone()) {
						self.context.billing
					} else {
						self.server
							.index
							.try_get_user(&id)
							.await?
							.ok_or_else(|| tg::error!(%id, "failed to find the sandbox owner"))?
							.billing
					};

					return Ok((billing, "tg user billing manage".to_owned()));
				},
				tg::Principal::Anonymous
				| tg::Principal::Process(_)
				| tg::Principal::Root
				| tg::Principal::Runner(_)
				| tg::Principal::Sandbox(_) => {
					return Err(tg::error!(
						"the sandbox owner does not have a billing account"
					));
				},
			}
		}
	}
}
