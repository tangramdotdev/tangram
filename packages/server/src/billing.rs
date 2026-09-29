use {crate::Session, tangram_client::prelude::*, tangram_index::prelude::*};

mod stripe;

pub(super) use stripe::{CreateCustomerArg, Stripe};

impl Session {
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
