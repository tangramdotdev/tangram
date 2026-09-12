use {crate::Session, std::sync::Arc, tangram_client::prelude::*};

pub(crate) struct Runner {
	pub changed: tokio::sync::watch::Receiver<()>,
	pub index: u64,
	pub location: tg::Location,
	pub location_arg: tg::location::Arg,
	pub processes: Arc<super::Processes>,
}

impl Session {
	#[must_use]
	pub(crate) fn try_get_process_runner_inner(
		&self,
		id: &tg::process::Id,
		location: Option<&tg::location::Arg>,
	) -> Option<Runner> {
		// Get the process.
		let state = self.server.runner.state();
		let sandbox = state.try_get_process_sandbox(id)?;
		let runner = self.try_get_sandbox_runner_inner(&sandbox, location)?;
		let sandbox = state.sandboxes().get(runner.index)?;
		let process = sandbox.processes.get_by_id(id)?;

		// Fall back to normal dispatch for finished processes.
		if process.data.status.is_finished() {
			return None;
		}

		// Create the runner handle.
		let changed = process.changed.subscribe();
		let index = *process.key();
		let location = runner.location;
		let location_arg = runner.location_arg;
		let processes = sandbox.processes.clone();
		let runner = Runner {
			changed,
			index,
			location,
			location_arg,
			processes,
		};

		Some(runner)
	}

	pub(crate) async fn authorize_process_runner(
		&self,
		id: &tg::process::Id,
		tokens: &tg::Tokens,
		permissions: tg::authorization::permission::process::Set,
	) -> tg::Result<Option<tg::authorization::permission::Set>> {
		let resource = tg::Referent::with_node_and_local_tokens(
			id.clone(),
			tokens.local_authorization().to_vec(),
		);
		let permissions = tg::authorization::permission::Set::Process(permissions);
		// Only the caller's local authority applies here; remote capabilities remain with their issuer.
		let permissions = self.authorize(resource, permissions).await?;
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);
		Ok(permissions.filter(|permissions| permissions.contains(permission)))
	}
}
