use {
	crate::Component, tangram_client::prelude::*, tangram_server_core::runner::Runner,
	tangram_server_sandbox_core::control::local::Local,
};

impl Component {
	pub(crate) fn try_get_sandbox_control_runner(
		&self,
		id: &tg::sandbox::Id,
		location: Option<&tg::location::Arg>,
	) -> Option<Local> {
		let runner = self.try_get_sandbox_runner(id, location)?;
		let sandbox = self.state.sandboxes().get(runner.index)?;
		let sender = &sandbox.control_sender;
		(!sender.is_closed()).then(|| sender.clone())
	}

	pub(crate) fn try_get_sandbox_runner(
		&self,
		id: &tg::sandbox::Id,
		location: Option<&tg::location::Arg>,
	) -> Option<Runner> {
		let sandbox = self.state.sandboxes().get_by_id(id)?;
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
}
