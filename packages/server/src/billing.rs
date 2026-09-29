use {crate::Session, tangram_client::prelude::*, tangram_index::prelude::*};

mod webhook;

#[derive(Clone)]
pub(crate) enum Billing {
	Stripe(tangram_billing_stripe::Billing),
}

impl Billing {
	#[must_use]
	pub fn new(config: &crate::config::Billing) -> Self {
		Self::Stripe(tangram_billing_stripe::Billing::new(&config.stripe))
	}
}

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

		let (billing_ready, command) = self.billing_ready(owner.clone()).await?;
		if !billing_ready {
			return Err(tg::error!(
				"billing is not ready for the sandbox owner; run `{command}`"
			));
		}

		Ok(())
	}

	async fn billing_ready(&self, mut owner: tg::Principal) -> tg::Result<(bool, String)> {
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

					return Ok((organization.billing_ready, command));
				},
				tg::Principal::User(id) => {
					let billing_ready = if self.context.principal == tg::Principal::User(id.clone())
					{
						self.context.billing_ready
					} else {
						self.server
							.index
							.try_get_user(&id)
							.await?
							.ok_or_else(|| tg::error!(%id, "failed to find the sandbox owner"))?
							.billing_ready
					};

					return Ok((billing_ready, "tg user billing manage".to_owned()));
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

impl tangram_billing::Billing for Billing {
	async fn create_customer(
		&self,
		arg: tangram_billing::customer::create::Arg,
	) -> tg::Result<String> {
		match self {
			Self::Stripe(billing) => billing.create_customer(arg).await,
		}
	}

	async fn create_management_url(&self, customer: &str) -> tg::Result<String> {
		match self {
			Self::Stripe(billing) => billing.create_management_url(customer).await,
		}
	}

	async fn customer_ready(&self, customer: &str) -> tg::Result<bool> {
		match self {
			Self::Stripe(billing) => billing.customer_ready(customer).await,
		}
	}

	fn try_parse_webhook(
		&self,
		headers: &http::HeaderMap,
		body: &[u8],
		now: i64,
	) -> tg::Result<Option<tangram_billing::webhook::Event>> {
		match self {
			Self::Stripe(billing) => billing.try_parse_webhook(headers, body, now),
		}
	}
}
