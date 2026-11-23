use {
	crate::Component,
	tangram_client::prelude::*,
	tangram_server_core::runner::{Runner, SandboxControl},
};

impl Component {
	/// Get the control of a sandbox whose boot the runner holds and the boot from one lookup, so that an abandonment between two lookups cannot be missed.
	pub(crate) fn try_get_sandbox_control_runner(
		&self,
		id: &tg::sandbox::Id,
		location: Option<&tg::location::Arg>,
	) -> Option<SandboxControl> {
		let sandbox = self.state.try_get_held_boot(id)?;
		Self::sandbox_runner(&sandbox, location)?;
		let sender = &sandbox.control_sender;
		if sender.is_closed() {
			return None;
		}
		let control = SandboxControl {
			boot: sandbox.boot.number,
			sender: sender.clone(),
		};

		Some(control)
	}

	pub(crate) fn try_get_sandbox_runner(
		&self,
		id: &tg::sandbox::Id,
		location: Option<&tg::location::Arg>,
	) -> Option<Runner> {
		let sandbox = self.state.try_get_held_boot(id)?;
		Self::sandbox_runner(&sandbox, location)
	}

	fn sandbox_runner(
		sandbox: &dashmap::mapref::one::Ref<'_, u64, tangram_server_runner_core::sandbox::State>,
		location: Option<&tg::location::Arg>,
	) -> Option<Runner> {
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
