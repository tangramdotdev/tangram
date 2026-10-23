use {
	futures::{FutureExt as _, future::BoxFuture},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
	tangram_index::prelude::*,
	tangram_server_core::Session,
};

mod webhook;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	pub provider: Option<tangram_server_core::billing::Billing>,
}

impl Component {
	#[must_use]
	pub fn new(arg: &tangram_server_core::component::Arg<'_>) -> Self {
		// Create the billing provider.
		let provider = arg
			.config
			.billing
			.as_ref()
			.map(tangram_server_core::billing::Billing::new);

		// Create the state.
		let state = State { provider };

		Self(Arc::new(state))
	}

	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::POST, ["webhooks", "stripe"]) => {
				self.handle_billing_webhook_request(session, request).await
			},
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}

	pub async fn verify_billing(
		&self,
		session: &Session,
		owner: Option<&tg::Principal>,
	) -> tg::Result<()> {
		if self.provider.is_none() {
			return Ok(());
		}
		let Some(owner) = owner else {
			return Ok(());
		};
		if matches!(owner, tg::Principal::Root) {
			return Ok(());
		}

		let (billing_ready, command) = self.billing_ready(session, owner.clone()).await?;
		if !billing_ready {
			return Err(tg::error!(
				"billing is not ready for the sandbox owner; run `{command}`"
			));
		}

		Ok(())
	}

	async fn billing_ready(
		&self,
		session: &Session,
		mut owner: tg::Principal,
	) -> tg::Result<(bool, String)> {
		loop {
			match owner {
				tg::Principal::Group(id) => {
					let group = session
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
					let id = session
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
					let organization = session
						.server
						.index
						.try_get_organization(&id)
						.await?
						.ok_or_else(|| tg::error!(%id, "failed to find the sandbox owner"))?;
					let command = format!("tg organization billing manage {id}");

					return Ok((organization.billing_ready, command));
				},
				tg::Principal::User(id) => {
					let billing_ready =
						if session.context.principal == tg::Principal::User(id.clone()) {
							session.context.billing_ready
						} else {
							session
								.server
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

impl std::ops::Deref for Component {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::billing::Component for Component {
	fn verify_billing<'a>(
		&'a self,
		session: &'a Session,
		owner: Option<&'a tg::Principal>,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.verify_billing(session, owner).boxed()
	}

	fn provider(&self) -> Option<&tangram_server_core::billing::Billing> {
		self.provider.as_ref()
	}

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
