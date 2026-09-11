use {crate::Session, tangram_client::prelude::*};

pub(crate) struct Runner {
	pub changed: tokio::sync::watch::Receiver<()>,
	pub index: u64,
	pub location: tg::Location,
	pub location_arg: tg::location::Arg,
}

impl Session {
	#[must_use]
	pub(crate) fn try_get_sandbox_runner_inner(
		&self,
		id: &tg::sandbox::Id,
		location: Option<&tg::location::Arg>,
	) -> Option<Runner> {
		let sandbox = self.server.runner.state().sandboxes().get_by_id(id)?;
		// Regions guide subsequent routing, but do not constrain a runner state match.
		let location_arg = if let Some(location) = location {
			let components = location
				.0
				.iter()
				.filter(|component| match (component, &sandbox.location) {
					(tg::location::arg::Component::Local(_), tg::Location::Local(_)) => true,
					(
						tg::location::arg::Component::Remote(component),
						tg::Location::Remote(remote),
					) => component.name == remote.name,
					_ => false,
				})
				.cloned()
				.collect::<Vec<_>>();
			if components.is_empty() {
				return None;
			}
			tg::location::Arg(components)
		} else {
			sandbox.location.clone().into()
		};
		let changed = sandbox.changed.subscribe();
		let index = *sandbox.key();
		let location = sandbox.location.clone();
		let runner = Runner {
			changed,
			index,
			location,
			location_arg,
		};
		Some(runner)
	}

	pub(crate) async fn authorize_sandbox_runner(
		&self,
		id: &tg::sandbox::Id,
		tokens: &[tg::authorization::Token],
		permission: tg::authorization::permission::sandbox::Permission,
	) -> tg::Result<bool> {
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
		let permission = tg::authorization::Permission::Sandbox(permission);
		// Never use the sandbox's own authorization tokens to authorize a caller.
		let permissions = self.authorize(resource, permission).await?;
		Ok(permissions.is_some_and(|permissions| permissions.contains(permission)))
	}
}
