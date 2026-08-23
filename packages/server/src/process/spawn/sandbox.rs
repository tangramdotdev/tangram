use {super::local::Output, crate::Session, futures::FutureExt as _, tangram_client::prelude::*};

impl Session {
	pub(super) async fn spawn_process_in_new_or_existing_sandbox(
		&self,
		arg: &tg::process::spawn::Arg,
		mut output: Option<Output>,
		mut scheduler_sender: Option<tokio::sync::oneshot::Sender<tg::scheduler::Id>>,
	) -> tg::Result<Option<Output>> {
		let Some(process) = output.as_mut() else {
			return Ok(output);
		};
		if process.cached || process.data.status.is_finished() {
			return Ok(output);
		}

		match &arg.sandbox {
			Some(tg::Either::Left(_)) if process.allocation.is_some() => {
				let ready_event = self
					.spawn_process_in_new_sandbox(process)
					.await?
					.ok_or_else(|| tg::error!("expected the sandbox to be ready"))?;
				let connected_event = ready_event
					.connected_event
					.ok_or_else(|| tg::error!("expected the process to be connected"))?;
				process.data.sandbox = ready_event.sandbox;
				Self::spawn_process_apply_connected(process, connected_event)?;
			},
			Some(tg::Either::Left(_)) => {
				let id = process.id.clone();
				let mut process_connection_future = self.subscribe_process_connection(&id).await?;
				let sandbox = process.data.sandbox.clone();
				let mut sandbox_connection_future =
					self.subscribe_sandbox_connection(&sandbox).await?;
				self.spawn_process_in_new_sandbox(process).await?;
				if let Some(sender) = scheduler_sender.take() {
					let scheduler = process
						.scheduler
						.clone()
						.ok_or_else(|| tg::error!("missing the scheduler"))?;
					sender.send(scheduler).ok();
				}
				let scheduler = process
					.scheduler
					.clone()
					.ok_or_else(|| tg::error!("missing the scheduler"))?;
				let connected = self
					.try_wait_process_connection(
						&scheduler,
						&mut process_connection_future,
						&mut sandbox_connection_future,
					)
					.await?;
				let Some(connected) = connected else {
					self.spawn_process_sandbox_cleanup_after_scheduler_heartbeat_expiration(
						id.clone(),
						sandbox.clone(),
						scheduler.clone(),
						sandbox_connection_future,
					);
					return Err(tg::error!(
						code = tg::error::Code::HeartbeatExpiration,
						process = %id,
						%sandbox,
						%scheduler,
						"the scheduler heartbeat expired"
					));
				};
				process.lease = Some(connected.lease);
			},
			Some(tg::Either::Right(_)) => {
				let connected_event = self.spawn_process_in_existing_sandbox(process).await?;
				Self::spawn_process_apply_connected(process, connected_event)?;
			},
			None => return Err(tg::error!("expected the sandbox to be set")),
		}

		Ok(output)
	}

	async fn try_wait_process_connection(
		&self,
		scheduler: &tg::scheduler::Id,
		process_connection_future: &mut crate::process::ConnectionFuture,
		sandbox_connection_future: &mut crate::sandbox::ConnectionFuture,
	) -> tg::Result<Option<crate::process::control::Connected>> {
		tokio::select! {
			result = process_connection_future.as_mut() => result.map(Some),
			result = sandbox_connection_future.as_mut() => {
				result?;
				process_connection_future.as_mut().await.map(Some)
			},
			result = self.scheduler_heartbeat_expired(scheduler) => result.map(|()| None),
		}
	}

	fn spawn_process_sandbox_cleanup_after_scheduler_heartbeat_expiration(
		&self,
		process: tg::process::Id,
		sandbox: tg::sandbox::Id,
		scheduler: tg::scheduler::Id,
		connection_future: crate::sandbox::ConnectionFuture,
	) {
		let error = tg::error::Data {
			code: Some(tg::error::Code::HeartbeatExpiration),
			message: Some("the scheduler heartbeat expired".into()),
			..Default::default()
		};
		let arg = tg::sandbox::destroy::Arg {
			error: Some(tg::Either::Left(error)),
			location: Some(tg::Location::Local(tg::location::Local::default()).into()),
		};
		let session = self.server.session(&self.server.context);
		tokio::spawn(async move {
			session
				.destroy_sandbox_when_available(&sandbox, arg, connection_future)
				.await
				.inspect_err(|error| {
					tracing::error!(
						error = %error.trace(),
						%process,
						%sandbox,
						%scheduler,
						"failed to destroy the sandbox after the scheduler heartbeat expired"
					);
				})
				.ok();
		});
	}

	fn spawn_process_apply_connected(
		output: &mut Output,
		connected_event: crate::runner::process::ConnectedEvent,
	) -> tg::Result<()> {
		let assigned = output.process_token.is_some();
		if !assigned && connected_event.grant.is_none() {
			return Err(tg::error!(
				process = %connected_event.process,
				"missing the process grant"
			));
		}
		output.id = connected_event.process;
		output.lease = Some(connected_event.lease);
		if let Some(grant) = connected_event.grant {
			output.token = Some(grant);
		}
		Ok(())
	}

	fn spawn_process_runner_arg(output: &Output) -> tg::runner::control::Process {
		let id = output.process_token.as_ref().map(|_| output.id.clone());
		let options = output.command_options.clone();
		let parent = output.parent.clone();
		let token = output.process_token.clone();
		tg::runner::control::Process {
			data: output.data.clone(),
			id,
			options,
			parent,
			token,
		}
	}

	async fn spawn_process_in_existing_sandbox(
		&self,
		output: &Output,
	) -> tg::Result<crate::runner::process::ConnectedEvent> {
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
		let connected_event = crate::runner::process::ConnectedEvent {
			grant: output.grant,
			lease: output.lease,
			process: output.process,
		};

		Ok(connected_event)
	}

	async fn spawn_process_in_new_sandbox(
		&self,
		output: &mut Output,
	) -> tg::Result<Option<crate::runner::sandbox::ReadyEvent>> {
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
			let spawn_sandbox_task_arg = crate::runner::sandbox::SpawnSandboxTaskArg {
				allocation,
				arg,
				creator: Some(self.context.principal.clone()),
				id,
				location,
				process: Some(process),
				token,
			};
			let task = self.server.spawn_sandbox_task(spawn_sandbox_task_arg);
			let mut events = task.events;
			let event = events
				.recv()
				.await
				.ok_or_else(|| tg::error!("the sandbox event sender was dropped"))??;
			match event {
				crate::runner::sandbox::Event::Destroyed => {
					return Err(tg::error!(
						"the sandbox was destroyed before it became ready"
					));
				},
				crate::runner::sandbox::Event::Ready(ready_event) => {
					return Ok(Some(ready_event));
				},
			}
		}
		let request = crate::scheduler::EnqueueSandboxRequestArg {
			arg,
			creator: Some(self.context.principal.clone()),
			parent: output.parent_sandbox.clone(),
			process: Some(process),
			sandbox: output.data.sandbox.clone(),
			scheduler: output.scheduler.clone(),
			token: output.sandbox_token.clone(),
		};
		let scheduler = self.enqueue_sandbox(request).await.map_err(|error| {
			tg::error!(!error, sandbox = %output.data.sandbox, process = %output.id, "failed to enqueue the sandbox")
		})?;
		output.scheduler = Some(scheduler);
		Ok(None)
	}
}
