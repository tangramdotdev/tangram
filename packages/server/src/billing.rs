use {
	crate::Session, tangram_client::prelude::*, tangram_index::billing::Status,
	tangram_index::prelude::*,
};

mod stripe;

pub(super) use stripe::{CreateCustomerArg, Stripe};

impl Session {
	pub(crate) async fn require_billing(&self, owner: Option<&tg::Principal>) -> tg::Result<()> {
		if self.server.billing.is_none() {
			return Ok(());
		}
		let Some(owner) = owner else {
			return Ok(());
		};
		if matches!(owner, tg::Principal::Root) {
			return Ok(());
		}

		let (command, status) = self.billing_status(owner.clone()).await?;
		match status {
			Status::Incomplete => {
				return Err(tg::error!(
					"billing setup is incomplete for the sandbox owner; run `{command}`"
				));
			},
			Status::Ready => (),
			Status::Unconfigured => {
				return Err(tg::error!(
					"billing is not configured for the sandbox owner; run `{command}`"
				));
			},
		}

		Ok(())
	}

	async fn billing_status(&self, mut owner: tg::Principal) -> tg::Result<(String, Status)> {
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
						.try_get_node(&specifier)
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

					return Ok((command, organization.billing));
				},
				tg::Principal::User(id) => {
					let status = if self.context.principal == tg::Principal::User(id.clone()) {
						self.context.billing
					} else {
						None
					};
					let status = match status {
						Some(status) => status,
						None => {
							self.server
								.index
								.try_get_user(&id)
								.await?
								.ok_or_else(|| tg::error!(%id, "failed to find the sandbox owner"))?
								.billing
						},
					};

					return Ok(("tg user billing manage".to_owned(), status));
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
