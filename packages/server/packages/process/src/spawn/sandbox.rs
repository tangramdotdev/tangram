use {
	super::local::Output,
	crate::Component,
	futures::{FutureExt as _, TryStreamExt as _},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_server_core::Session,
};

impl Component {
	pub(super) async fn spawn_process_in_new_or_existing_sandbox(
		&self,
		session: &Session,
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
					.spawn_process_in_new_sandbox(session, process)
					.await?
					.ok_or_else(|| tg::error!("expected the sandbox to be ready"))?;
				let connected_event = ready_event
					.connected_event
					.ok_or_else(|| tg::error!("expected the process to be connected"))?;
				process.data.sandbox = Some(ready_event.sandbox);
				Self::spawn_process_apply_connected(process, connected_event);
			},
			Some(tg::Either::Left(_)) => {
				let id = process.id.clone();
				let mut process_connection_future =
					self.subscribe_process_connection(session, &id).await?;
				let sandbox = process
					.data
					.sandbox
					.clone()
					.ok_or_else(|| tg::error!("the scheduled process has no sandbox"))?;
				let mut sandbox_connection_future = session
					.server
					.components
					.sandbox
					.subscribe_sandbox_connection(session, &sandbox)
					.await?;
				self.spawn_process_in_new_sandbox(session, process).await?;
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
						session,
						&scheduler,
						&sandbox,
						&mut process_connection_future,
						&mut sandbox_connection_future,
					)
					.await?;
				let Some(connected) = connected else {
					Self::spawn_process_sandbox_cleanup_after_scheduler_heartbeat_expiration(
						session,
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
				let connected_event = self
					.spawn_process_in_existing_sandbox(session, process, arg.location.as_ref())
					.await?;
				Self::spawn_process_apply_connected(process, connected_event);
			},
			None => return Err(tg::error!("expected the sandbox to be set")),
		}

		Ok(output)
	}

	async fn try_wait_process_connection(
		&self,
		session: &Session,
		scheduler: &tg::scheduler::Id,
		sandbox: &tg::sandbox::Id,
		process_connection_future: &mut crate::ConnectionFuture,
		sandbox_connection_future: &mut tangram_server_core::sandbox::ConnectionFuture,
	) -> tg::Result<Option<tangram_server_process_common::control::Connected>> {
		// The scheduler only governs startup until the sandbox connects.
		tokio::select! {
			result = process_connection_future.as_mut() => return result.map(Some),
			result = sandbox_connection_future.as_mut() => result?,
			result = session.server.components.scheduler.scheduler_heartbeat_expired(session, scheduler) => return result.map(|()| None),
		}
		tangram_server_core::checkpoint!(session.server, "process.spawn.connection.wait", %sandbox)
			.await;
		let wakeups = session
			.server
			.components
			.sandbox
			.create_sandbox_status_wakeup_stream(session, sandbox, None, None)
			.await?;
		let mut status_stream = session
			.server
			.components
			.sandbox
			.create_sandbox_status_stream_local_with_wakeups(
				session,
				sandbox,
				None,
				Some(wakeups),
				tg::sandbox::Source::Auto,
			);
		let destroyed_future = async {
			while let Some(event) = status_stream.try_next().await? {
				if let tg::sandbox::status::Event::Status(status) = event
					&& status.is_destroyed()
				{
					return Err(tg::error!(%sandbox, "the sandbox was destroyed"));
				}
			}
			Err(tg::error!(%sandbox, "the sandbox status stream ended"))
		};
		let destroyed_future = pin!(destroyed_future);
		tokio::select! {
			result = process_connection_future.as_mut() => result.map(Some),
			result = destroyed_future => result,
		}
	}

	fn spawn_process_sandbox_cleanup_after_scheduler_heartbeat_expiration(
		session: &Session,
		process: tg::process::Id,
		sandbox: tg::sandbox::Id,
		scheduler: tg::scheduler::Id,
		connection_future: tangram_server_core::sandbox::ConnectionFuture,
	) {
		let error = tg::error::Data {
			code: Some(tg::error::Code::HeartbeatExpiration),
			message: Some("the scheduler heartbeat expired".into()),
			..Default::default()
		};
		let arg = tg::sandbox::destroy::Arg {
			error: Some(tg::Either::Left(error)),
			location: Some(tg::Location::Local(tg::location::Local::default()).into()),
			tokens: tg::authorization::Tokens::default(),
		};
		let session = session.server.session(&session.server.context);
		tokio::spawn(async move {
			session
				.server
				.components
				.sandbox
				.destroy_sandbox_when_available(&session, &sandbox, arg, connection_future)
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
		connected_event: tangram_server_runner_core::process::ConnectedEvent,
	) {
		let tokens = connected_event.process.options.tokens;
		output.id = connected_event.process.node;
		output.lease = Some(connected_event.lease);
		if !tokens.is_empty() {
			output.tokens = tokens;
		}
	}

	fn spawn_process_runner_arg(output: &Output) -> tg::runner::control::Process {
		let data = output.data.clone();
		let id = output.process_token.as_ref().map(|_| output.id.clone());
		let mut options = data.command.options.clone();
		options.tokens.clear();
		let parent = output.parent.clone();
		let token = output.process_token.clone();
		tg::runner::control::Process {
			data,
			id,
			open: Vec::new(),
			options,
			parent,
			token,
		}
	}

	#[tracing::instrument(name = "process.spawn_existing", level = "debug", skip_all, fields(process = %output.id, sandbox = output.data.sandbox.as_ref().map(ToString::to_string)), err(level = "debug"))]
	async fn spawn_process_in_existing_sandbox(
		&self,
		session: &Session,
		output: &Output,
		location: Option<&tg::location::Arg>,
	) -> tg::Result<tangram_server_runner_core::process::ConnectedEvent> {
		let process = Self::spawn_process_runner_arg(output);
		let id = output.id.clone();
		let assigned = process.id.is_some();
		let sandbox = output
			.data
			.sandbox
			.clone()
			.ok_or_else(|| tg::error!("the scheduled process has no sandbox"))?;
		let request = tg::sandbox::control::ServerRequestArg::SpawnProcess(
			tg::sandbox::control::SpawnProcessServerRequestArg { process },
		);
		let retry = session.server.config.control.request_retry.clone().into();
		let timeout = session.server.config.control.request_timeout;
		let options = tangram_server_core::control::Options { retry, timeout };
		// Spawn preparation has already checked sandbox access and the request origin.
		let response = if let Some(control_sender) =
			session.try_get_sandbox_control_runner_inner(&sandbox, location)
		{
			control_sender.request(request).await
		} else {
			session.server.components.sandbox.request_sandbox_control(session, &sandbox, request, options)
				.boxed()
				.await
				.map_err(|error| {
					tg::error!(!error, %sandbox, process = %id, "failed to send the spawn process request")
				})?
		}
		.map_err(
			|error| tg::error!(!error, %sandbox, process = %id, "the spawn process request failed"),
		)?;
		let output = response
			.try_unwrap_spawn_process()
			.map_err(|_| tg::error!("expected a spawn process response"))?;
		if assigned && output.process.node != id {
			return Err(tg::error!(
				actual = %output.process.node,
				expected = %id,
				"the runner returned an invalid process"
			));
		}
		let connected_event = tangram_server_runner_core::process::ConnectedEvent {
			lease: output.lease,
			process: output.process,
		};

		Ok(connected_event)
	}

	#[tracing::instrument(name = "process.spawn_new", level = "debug", skip_all, fields(process = %output.id, sandbox = output.data.sandbox.as_ref().map(ToString::to_string)), err(level = "debug"))]
	async fn spawn_process_in_new_sandbox(
		&self,
		session: &Session,
		output: &mut Output,
	) -> tg::Result<Option<tangram_server_runner_core::sandbox::ReadyEvent>> {
		let sandbox = output
			.data
			.sandbox
			.clone()
			.ok_or_else(|| tg::error!("the scheduled process has no sandbox"))?;
		let arg = output
			.sandbox_arg
			.clone()
			.ok_or_else(|| tg::error!("missing the sandbox arg"))?;
		let process = Self::spawn_process_runner_arg(output);
		if let Some(allocation) = output.allocation.take() {
			let location = session.server.location(arg.location.as_ref())?;
			let id = output.sandbox_token.as_ref().map(|_| sandbox.clone());
			let token = output.sandbox_token.clone();
			let spawn_sandbox_task_arg = tangram_server_runner_core::sandbox::SpawnSandboxTaskArg {
				allocation,
				arg,
				creator: Some(session.context.principal.clone()),
				id,
				location,
				process: Some(process),
				token,
			};
			let task = session
				.server
				.components
				.runner
				.spawn_sandbox_task(&session.server, spawn_sandbox_task_arg);
			let mut events = task.events;
			let event = events
				.recv()
				.await
				.ok_or_else(|| tg::error!("the sandbox event sender was dropped"))??;
			match event {
				tangram_server_runner_core::sandbox::Event::Destroyed => {
					return Err(tg::error!(
						"the sandbox was destroyed before it became ready"
					));
				},
				tangram_server_runner_core::sandbox::Event::Ready(ready_event) => {
					return Ok(Some(ready_event));
				},
			}
		}

		// Give the assignment the connect's opens, and offer the opens to the connect's readers and writers.
		let mut process = process;
		if let Some(open) = output.open.take() {
			process.open = open.requests();
			open.offer();
		}

		let request = tangram_server_scheduler_core::EnqueueSandboxRequestArg {
			arg,
			creator: Some(session.context.principal.clone()),
			parent: output.parent_sandbox.clone(),
			process: Some(process),
			sandbox: sandbox.clone(),
			scheduler: output.scheduler.clone(),
			token: output.sandbox_token.clone(),
		};
		let scheduler = session.server.components.scheduler.enqueue_sandbox(session, request).await.map_err(|error| {
			tg::error!(!error, sandbox = %sandbox, process = %output.id, "failed to enqueue the sandbox")
		})?;
		output.scheduler = Some(scheduler);
		Ok(None)
	}
}
