use {
	super::local::Output, crate::Session, futures::TryStreamExt as _, tangram_client::prelude::*,
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
				let process_started = started
					.process
					.ok_or_else(|| tg::error!("expected the process to be started"))?;
				process.data.sandbox = started.sandbox;
				Self::spawn_process_apply_started(process, process_started)?;
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
				let started = self.spawn_process_in_existing_sandbox(process).await?;
				Self::spawn_process_apply_started(process, started)?;
			},
			None => return Err(tg::error!("expected the sandbox to be set")),
		}

		Ok(output)
	}

	fn spawn_process_apply_started(
		output: &mut Output,
		started: crate::runner::ProcessStarted,
	) -> tg::Result<()> {
		let assigned = output.process_token.is_some();
		if !assigned && started.grant.is_none() {
			return Err(tg::error!(
				process = %started.id,
				"missing the process grant"
			));
		}
		output.id = started.id;
		output.lease = Some(started.lease);
		if let Some(grant) = started.grant {
			output.token = Some(grant);
		}
		Ok(())
	}

	fn spawn_process_runner_arg(output: &Output) -> tg::runner::control::Process {
		let identity =
			output
				.process_token
				.clone()
				.map(|token| tg::runner::control::ProcessIdentity {
					id: output.id.clone(),
					token,
				});
		tg::runner::control::Process {
			data: output.data.clone(),
			identity,
			parent: output.parent.clone(),
		}
	}

	async fn spawn_process_in_existing_sandbox(
		&self,
		output: &Output,
	) -> tg::Result<crate::runner::ProcessStarted> {
		let process = Self::spawn_process_runner_arg(output);
		let id = output.id.clone();
		let assigned = process.identity.is_some();
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
		let started = crate::runner::ProcessStarted {
			grant: output.grant,
			id: output.process,
			lease: output.lease,
		};

		Ok(started)
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
			let identity =
				output
					.sandbox_token
					.clone()
					.map(|token| crate::runner::SandboxIdentity {
						id: output.data.sandbox.clone(),
						token,
					});
			let task = self
				.server
				.spawn_sandbox_task(crate::runner::SpawnSandboxTaskArg {
					allocation,
					arg,
					creator: Some(self.context.principal.clone()),
					identity,
					location,
					process: Some(process),
				});
			let started = task
				.started
				.await
				.map_err(|_| tg::error!("the sandbox start sender was dropped"))??;
			return Ok(Some(started));
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
