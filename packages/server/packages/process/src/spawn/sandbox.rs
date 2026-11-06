use {
	super::local::Output,
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_index::prelude::*,
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
				let lease = tg::process::Lease::new();
				let ready_event = self
					.spawn_process_in_new_sandbox(session, process, lease)
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
				let lease = tg::process::Lease::new();
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
				self.spawn_process_in_new_sandbox(session, process, lease)
					.await?;
				if let Some(sender) = scheduler_sender.take() {
					let scheduler = process
						.scheduler
						.clone()
						.ok_or_else(|| tg::error!("missing the scheduler"))?;
					sender.send(scheduler).ok();
				}
				if arg.wait {
					self.wait_process_start(session, &id, &sandbox, &mut sandbox_connection_future)
						.await?;
				}
				process.lease = Some(lease);
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

	async fn wait_process_start(
		&self,
		session: &Session,
		id: &tg::process::Id,
		sandbox: &tg::sandbox::Id,
		sandbox_connection_future: &mut tangram_server_core::sandbox::ConnectionFuture,
	) -> tg::Result<()> {
		// Watch the process's status, whose wakeups poll, so that a lost notification only delays the spawn.
		let wakeups = self
			.create_process_status_wakeup_stream(session, id, session.context.stopper.clone(), None)
			.await?;
		let started_future = self.wait_for_process_start_from_index(session, id, wakeups);
		let mut started_future = pin!(started_future);

		// The scheduler only governs startup until the sandbox connects.
		tokio::select! {
			result = started_future.as_mut() => return result,
			result = sandbox_connection_future.as_mut() => result?,
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
				if let tg::sandbox::status::Event::Status(status) = event {
					match status {
						tg::sandbox::Status::Destroyed | tg::sandbox::Status::Destroying => {
							return Err(tg::error!(%sandbox, "the sandbox was destroyed"));
						},
						tg::sandbox::Status::Stopped => {
							return Err(tg::error!(%sandbox, "the sandbox is not started"));
						},
						tg::sandbox::Status::Started
						| tg::sandbox::Status::Starting
						| tg::sandbox::Status::Stopping => {},
					}
				}
			}
			Err(tg::error!(%sandbox, "the sandbox status stream ended"))
		};
		let destroyed_future = pin!(destroyed_future);
		tokio::select! {
			result = started_future.as_mut() => result,
			result = destroyed_future => {
				let started = Self::try_get_process_start_from_index(session, id).await?;
				tangram_server_core::checkpoint!(session.server, "process.spawn.sandbox.ended", process = %id)
					.await;
				if started {
					return Ok(());
				}

				// Wait for pending writes before checking whether the process started before its sandbox ended.
				session
					.index(tg::index::Arg::default())
					.await
					.map_err(|error| tg::error!(!error, "failed to index"))?
					.try_last()
					.await
					.map_err(|error| tg::error!(!error, "failed to index"))?;
				if Self::try_get_process_start_from_index(session, id).await? {
					return Ok(());
				}
				result
			},
		}
	}

	async fn wait_for_process_start_from_index(
		&self,
		session: &Session,
		id: &tg::process::Id,
		mut wakeups: BoxStream<'static, ()>,
	) -> tg::Result<()> {
		loop {
			tangram_server_core::checkpoint!(session.server, "process.spawn.start.index", process = %id)
				.await;
			if Self::try_get_process_start_from_index(session, id).await? {
				return Ok(());
			}
			if wakeups.next().await.is_none() {
				return Err(tg::error!("the process status wakeup stream ended"));
			}
		}
	}

	async fn try_get_process_start_from_index(
		session: &Session,
		id: &tg::process::Id,
	) -> tg::Result<bool> {
		let process = session
			.server
			.index
			.try_get_process(id)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
		let started = process
			.and_then(|process| process.status)
			.is_some_and(|status| !status.is_starting());

		Ok(started)
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

	fn spawn_process_runner_arg(
		output: &Output,
		lease: tg::process::Lease,
	) -> tg::runner::control::Process {
		let data = output.data.clone();
		let id = output.process_token.as_ref().map(|_| output.id.clone());
		let mut options = data.command.options.clone();
		options.tokens.clear();
		let parent = output.parent.clone();
		let token = output.process_token.clone();
		tg::runner::control::Process {
			data,
			id,
			lease,
			leases: Vec::new(),
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
		let lease = tg::process::Lease::new();
		let process = Self::spawn_process_runner_arg(output, lease);
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
		let response = if let Some(control_sender) = session
			.server
			.components
			.runner
			.try_get_sandbox_control_runner(&sandbox, location)
		{
			control_sender.request(request).await
		} else {
			// Send the request right away, and let the sandbox's status decide what happens if nobody answers.
			let check = Self::create_check_sandbox_started_future(session, &sandbox);
			let hold = check.clone().map(|_| ()).boxed();
			let request_future = session
				.server
				.components
				.sandbox
				.request_sandbox_control_with_hold(session, &sandbox, request, options, Some(hold));
			let check_future = async move {
				match check.await {
					Ok(()) => future::pending().await,
					Err(error) => error,
				}
			};
			let result = match future::select(request_future, pin!(check_future)).await {
				future::Either::Left((result, _)) => result,
				future::Either::Right((error, _)) => return Err(error),
			};
			match result {
				Ok(response) => response,
				Err(error) => {
					// Report a sandbox that is not running rather than the failure to reach it.
					let status = session
						.server
						.components
						.sandbox
						.try_get_sandbox_from_index(session, &sandbox)
						.await
						.ok()
						.flatten()
						.and_then(|sandbox| sandbox.data)
						.map(|data| data.data.status);
					return Err(match status {
						Some(tg::sandbox::Status::Destroyed | tg::sandbox::Status::Destroying) => {
							tg::error!(!error, %sandbox, "the sandbox was destroyed")
						},
						Some(tg::sandbox::Status::Stopped | tg::sandbox::Status::Stopping) => {
							tg::error!(!error, %sandbox, "the sandbox is not started")
						},
						Some(tg::sandbox::Status::Started | tg::sandbox::Status::Starting)
						| None => {
							tg::error!(!error, %sandbox, process = %id, "failed to send the spawn process request")
						},
					});
				},
			}
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

	/// Create a future that waits for a sandbox that is starting to connect, and fails if the index records that the sandbox is not running, waiting for pending indexing before trusting a stopped status, because a start that just finished may not be indexed yet.
	fn create_check_sandbox_started_future(
		session: &Session,
		sandbox: &tg::sandbox::Id,
	) -> future::Shared<future::BoxFuture<'static, tg::Result<()>>> {
		let session = session.clone();
		let sandbox = sandbox.clone();
		async move { Self::check_sandbox_started(&session, &sandbox).await }
			.boxed()
			.shared()
	}

	async fn check_sandbox_started(session: &Session, sandbox: &tg::sandbox::Id) -> tg::Result<()> {
		let mut wakeups = session
			.server
			.components
			.sandbox
			.create_sandbox_status_wakeup_stream(
				session,
				sandbox,
				session.context.stopper.clone(),
				None,
			)
			.await?;
		let mut indexed = true;
		loop {
			let status = session
				.server
				.components
				.sandbox
				.try_get_sandbox_from_index(session, sandbox)
				.await?
				.and_then(|sandbox| sandbox.data)
				.map(|data| data.data.status);
			match status {
				Some(tg::sandbox::Status::Destroyed | tg::sandbox::Status::Destroying) => {
					return Err(tg::error!(%sandbox, "the sandbox was destroyed"));
				},
				Some(tg::sandbox::Status::Stopped | tg::sandbox::Status::Stopping) if indexed => {
					indexed = false;
					session
						.index(tg::index::Arg::default())
						.await
						.map_err(|error| tg::error!(!error, "failed to index"))?
						.try_last()
						.await
						.map_err(|error| tg::error!(!error, "failed to index"))?;
				},
				Some(tg::sandbox::Status::Stopped | tg::sandbox::Status::Stopping) => {
					return Err(tg::error!(%sandbox, "the sandbox is not started"));
				},
				Some(tg::sandbox::Status::Starting) => {
					if wakeups.next().await.is_none() {
						return Err(tg::error!("the sandbox status wakeup stream ended"));
					}
				},
				Some(tg::sandbox::Status::Started) | None => {
					return Ok(());
				},
			}
		}
	}

	#[tracing::instrument(name = "process.spawn_new", level = "debug", skip_all, fields(process = %output.id, sandbox = output.data.sandbox.as_ref().map(ToString::to_string)), err(level = "debug"))]
	async fn spawn_process_in_new_sandbox(
		&self,
		session: &Session,
		output: &mut Output,
		lease: tg::process::Lease,
	) -> tg::Result<Option<tangram_server_runner_core::sandbox::ReadyEvent>> {
		let sandbox = output
			.data
			.sandbox
			.clone()
			.ok_or_else(|| tg::error!("the scheduled process has no sandbox"))?;
		let super::local::SandboxArg { arg, owner } = output
			.sandbox_arg
			.clone()
			.ok_or_else(|| tg::error!("missing the sandbox arg"))?;
		let process = Self::spawn_process_runner_arg(output, lease);
		if let Some(allocation) = output.allocation.take() {
			let location = session.server.location(arg.location.as_ref())?;
			let id = output.sandbox_token.as_ref().map(|_| sandbox.clone());
			let token = output.sandbox_token.clone();
			let spawn_sandbox_task_arg = tangram_server_runner_core::sandbox::SpawnSandboxTaskArg {
				allocation,
				arg,
				boot: 0,
				creator: session.context.principal.clone(),
				id,
				location,
				owner,
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
			boot: 0,
			created_at: session.server.clock.unix_timestamp(),
			creator: session.context.principal.clone(),
			owner,
			parent: output.parent_sandbox.clone(),
			process: Some(process),
			runner: None,
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
