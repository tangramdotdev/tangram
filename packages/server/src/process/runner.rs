use {crate::Session, std::sync::Arc, tangram_client::prelude::*};

pub(crate) struct Runner {
	pub changed: tokio::sync::watch::Receiver<()>,
	pub location: tg::Location,
	pub location_arg: tg::location::Arg,
	pub processes: Arc<super::Processes>,
}

pub(crate) struct Control {
	pub control_sender: super::control::local::Local,
	pub data: tg::process::Data,
}

impl Session {
	#[must_use]
	pub(crate) fn try_get_process_control_runner_inner(
		&self,
		id: &tg::process::Id,
		location: Option<&tg::location::Arg>,
	) -> Option<Control> {
		let state = self.server.runner.state();
		let sandbox = state.try_get_process_sandbox(id)?;
		let runner = self.try_get_sandbox_runner_inner(&sandbox, location)?;
		let sandbox = state.sandboxes().get(runner.index)?;
		let process = sandbox.processes.get(id)?;
		let control = Control {
			control_sender: process.control_sender.clone(),
			data: process.data.clone(),
		};
		Some(control)
	}

	pub(crate) async fn try_get_process_control_runner(
		&self,
		id: &tg::process::Id,
		location: Option<&tg::location::Arg>,
		tokens: &tg::Tokens,
		required: tg::authorization::permission::process::Set,
	) -> tg::Result<Option<Control>> {
		let Some(control) = self.try_get_process_control_runner_inner(id, location) else {
			return Ok(None);
		};
		let resource = tg::Referent::with_node_and_local_tokens(
			id.clone(),
			tokens.local_authorization().to_vec(),
		);
		let required = tg::authorization::permission::Set::Process(required);
		// Select the local route only when it can authorize the entire operation.
		let permissions = self.authorize(resource, required).await?;
		if !permissions.is_some_and(|permissions| permissions.contains(required)) {
			return Ok(None);
		}
		Ok(Some(control))
	}

	#[must_use]
	pub(crate) fn try_get_process_runner_inner(
		&self,
		id: &tg::process::Id,
		location: Option<&tg::location::Arg>,
	) -> Option<Runner> {
		let runner = self.try_get_process_runner_including_finished(id, location)?;
		let finished = runner.processes.get(id)?.data.status.is_finished();
		(!finished).then_some(runner)
	}

	#[must_use]
	pub(crate) fn try_get_process_runner_including_finished(
		&self,
		id: &tg::process::Id,
		location: Option<&tg::location::Arg>,
	) -> Option<Runner> {
		// Get the process.
		let state = self.server.runner.state();
		let sandbox = state.try_get_process_sandbox(id)?;
		let runner = self.try_get_sandbox_runner_inner(&sandbox, location)?;
		let sandbox = state.sandboxes().get(runner.index)?;
		let process = sandbox.processes.get(id)?;

		// Create the runner handle.
		let changed = process.changed.subscribe();
		let location = runner.location;
		let location_arg = runner.location_arg;
		let processes = sandbox.processes.clone();
		let runner = Runner {
			changed,
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
		let required = tg::authorization::permission::Set::Process(
			tg::authorization::permission::process::Set::NODE,
		);
		let mut permissions = self
			.authorize_batch_with_required([(resource, permissions)], required)
			.await?;
		let permissions = permissions.pop().unwrap();
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);
		Ok(permissions.filter(|permissions| permissions.contains(permission)))
	}
}
