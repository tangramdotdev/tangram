use {
	crate::{Session, database::Transaction},
	tangram_client::prelude::*,
	tangram_index::prelude::*,
};

mod stripe;

pub(super) use stripe::{CreateCustomerArg, Stripe};

impl Session {
	pub(crate) async fn storage_account_for_specifier_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		specifier: &tg::Specifier,
	) -> tg::Result<Option<tangram_index::storage::Account>> {
		let prefix = specifier
			.prefixes()
			.next()
			.expect("a specifier should have a component");
		let principal =
			match Self::try_get_id_for_specifier_with_transaction(transaction, &prefix).await? {
				Some(id) => match id.kind() {
					tg::id::Kind::Group => Some(tg::Principal::Group(id.try_into()?)),
					tg::id::Kind::Organization => Some(tg::Principal::Organization(id.try_into()?)),
					tg::id::Kind::User => Some(tg::Principal::User(id.try_into()?)),
					_ => None,
				},
				None => None,
			};
		if let Some(principal) = principal
			&& let Some(account) = self
				.storage_account_with_transaction(transaction, &principal)
				.await?
		{
			return Ok(Some(account));
		}

		self.storage_account_with_transaction(transaction, &self.context.principal)
			.await
	}

	async fn storage_account_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		principal: &tg::Principal,
	) -> tg::Result<Option<tangram_index::storage::Account>> {
		let mut principal = principal.clone();
		loop {
			match principal {
				tg::Principal::Group(id) => {
					let Some(group) =
						Self::try_get_group_with_transaction(transaction, &id).await?
					else {
						return Ok(None);
					};
					let specifier = group
						.specifier
						.prefixes()
						.next()
						.expect("a specifier should have a component");
					let Some(id) =
						Self::try_get_id_for_specifier_with_transaction(transaction, &specifier)
							.await?
					else {
						return Ok(None);
					};
					principal = match id.kind() {
						tg::id::Kind::Organization => tg::Principal::Organization(id.try_into()?),
						tg::id::Kind::User => tg::Principal::User(id.try_into()?),
						_ => return Ok(None),
					};
				},
				tg::Principal::Organization(id) => {
					return Ok(Some(tangram_index::storage::Account::Organization(id)));
				},
				tg::Principal::User(id) => {
					return Ok(Some(tangram_index::storage::Account::User(id)));
				},
				tg::Principal::Process(_) | tg::Principal::Sandbox(_) => {
					return self.storage_account(&principal).await;
				},
				_ => return Ok(None),
			}
		}
	}

	pub(crate) async fn storage_account(
		&self,
		principal: &tg::Principal,
	) -> tg::Result<Option<tangram_index::storage::Account>> {
		let mut principal = match principal {
			tg::Principal::Process(_) | tg::Principal::Sandbox(_) => {
				let Some(principal) = self.try_resolve_remote_context_principal(principal).await?
				else {
					return Ok(None);
				};
				principal
			},
			_ => principal.clone(),
		};
		loop {
			match principal {
				tg::Principal::Group(id) => {
					let Some(group) = self.server.index.try_get_group(&id).await? else {
						return Ok(None);
					};
					let specifier = group
						.specifier
						.prefixes()
						.next()
						.expect("a specifier should have a component");
					let Some(id) = self
						.server
						.index
						.try_get_id_for_specifier(&specifier)
						.await?
					else {
						return Ok(None);
					};
					principal = match id.kind() {
						tg::id::Kind::Organization => tg::Principal::Organization(id.try_into()?),
						tg::id::Kind::User => tg::Principal::User(id.try_into()?),
						_ => return Ok(None),
					};
				},
				tg::Principal::Organization(id) => {
					return Ok(Some(tangram_index::storage::Account::Organization(id)));
				},
				tg::Principal::User(id) => {
					return Ok(Some(tangram_index::storage::Account::User(id)));
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
