use {
	super::local::Output,
	crate::Session,
	futures::{FutureExt as _, TryStreamExt as _},
	tangram_client::prelude::*,
	tangram_messenger::Messenger as _,
};

impl Session {
	pub(super) async fn spawn_process_start_local(
		&self,
		arg: &tg::process::spawn::Arg,
		mut output: Option<Output>,
	) -> tg::Result<Option<Output>> {
		let Some(process) = output.as_mut() else {
			return Ok(output);
		};
		if process.cached || process.data.status.is_finished() {
			return Ok(output);
		}

		match &arg.sandbox {
			Some(tg::Either::Left(_)) if process.allocation.is_some() => {
				let started = self
					.spawn_process_in_new_sandbox(process)
					.await?
					.ok_or_else(|| tg::error!("expected the sandbox to be started"))?;
				let process_connected = started
					.process
					.ok_or_else(|| tg::error!("expected the process to be connected"))?;
				process.data.sandbox = started.sandbox;
				Self::spawn_process_apply_connected(process, process_connected)?;
			},
			Some(tg::Either::Left(_)) => {
				let id = process.id.clone();
				let mut connected = self
					.server
					.messenger
					.subscribe::<crate::process::control::Connected>(
						crate::process::control::connected_subject(&id),
					)
					.await
					.map_err(|error| {
						tg::error!(
							!error,
							process = %id,
							"failed to subscribe to the process control connection"
						)
					})?;
				self.spawn_process_in_new_sandbox(process).await?;
				let connected = connected
					.try_next()
					.await
					.map_err(|error| {
						tg::error!(
							!error,
							process = %id,
							"failed to receive the process control connection"
						)
					})?
					.ok_or_else(|| tg::error!("the process control connection stream ended"))?;
				process.lease = Some(connected.payload.lease);
			},
			Some(tg::Either::Right(_)) => {
				let connected = self.spawn_process_in_existing_sandbox(process).await?;
				Self::spawn_process_apply_connected(process, connected)?;
			},
			None => return Err(tg::error!("expected the sandbox to be set")),
		}

		Ok(output)
	}

	fn spawn_process_apply_connected(
		output: &mut Output,
		connected: crate::runner::ProcessConnected,
	) -> tg::Result<()> {
		let assigned = output.process_token.is_some();
		if !assigned && connected.grant.is_none() {
			return Err(tg::error!(
				process = %connected.process,
				"missing the process grant"
			));
		}
		output.id = connected.process;
		output.lease = Some(connected.lease);
		if let Some(grant) = connected.grant {
			output.token = Some(grant);
		}
		Ok(())
	}

	fn spawn_process_runner_arg(output: &Output) -> tg::runner::control::Process {
		let id = output.process_token.as_ref().map(|_| output.id.clone());
		let token = output.process_token.clone();
		tg::runner::control::Process {
			data: output.data.clone(),
			id,
			parent: output.parent.clone(),
			token,
		}
	}

	async fn spawn_process_in_existing_sandbox(
		&self,
		output: &Output,
	) -> tg::Result<crate::runner::ProcessConnected> {
		let process = Self::spawn_process_runner_arg(output);
		let id = output.id.clone();
		let assigned = process.id.is_some();
		let sandbox = output.data.sandbox.clone();
		let request = tg::sandbox::control::ServerRequestArg::SpawnProcess(
			tg::sandbox::control::SpawnProcessServerRequestArg { process },
		);
		let options = crate::control::Options {
			retry: tangram_futures::retry::Options::default(),
			timeout: std::time::Duration::from_secs(10),
		};
		let response = self
			.send_sandbox_control_request(&sandbox, request, options)
			.boxed()
			.await
			.map_err(
				|error| tg::error!(!error, %sandbox, process = %id, "failed to send the spawn process request"),
			)?
			.map_err(
				|error| tg::error!(!error, %sandbox, process = %id, "the spawn process request failed"),
			)?;
		let output = response
			.try_unwrap_spawn_process()
			.map_err(|_| tg::error!("expected a spawn process response"))?;
		if assigned && output.process != id {
			return Err(tg::error!(
				actual = %output.process,
				expected = %id,
				"the runner returned an invalid process"
			));
		}
		let connected = crate::runner::ProcessConnected {
			grant: output.grant,
			lease: output.lease,
			process: output.process,
		};

		Ok(connected)
	}

	async fn spawn_process_in_new_sandbox(
		&self,
		output: &mut Output,
	) -> tg::Result<Option<crate::runner::SandboxStarted>> {
		let arg = output
			.sandbox_arg
			.clone()
			.ok_or_else(|| tg::error!("missing the sandbox arg"))?;
		let process = Self::spawn_process_runner_arg(output);
		if let Some(allocation) = output.allocation.take() {
			let location = self.server.location(arg.location.as_ref())?;
			let id = output
				.sandbox_token
				.as_ref()
				.map(|_| output.data.sandbox.clone());
			let token = output.sandbox_token.clone();
			let task = self
				.server
				.spawn_sandbox_task(crate::runner::SpawnSandboxTaskArg {
					allocation,
					arg,
					creator: Some(self.context.principal.clone()),
					id,
					location,
					process: Some(process),
					token,
				});
			let mut events = task.events;
			let event = events
				.recv()
				.await
				.ok_or_else(|| tg::error!("the sandbox event sender was dropped"))??;
			match event {
				crate::runner::SandboxEvent::Destroy => {
					return Err(tg::error!("the sandbox was destroyed before it started"));
				},
				crate::runner::SandboxEvent::Start(event) => return Ok(Some(event)),
			}
		}
		let request = crate::scheduler::CreateSandboxRequestArg {
			arg,
			creator: Some(self.context.principal.clone()),
			parent: output.parent_sandbox.clone(),
			process: Some(process),
			sandbox: output.data.sandbox.clone(),
			token: output.sandbox_token.clone(),
		};
		self.enqueue_sandbox(request).await.map_err(|error| {
			tg::error!(!error, sandbox = %output.data.sandbox, process = %output.id, "failed to enqueue the sandbox")
		})?;
		Ok(None)
	}
}
