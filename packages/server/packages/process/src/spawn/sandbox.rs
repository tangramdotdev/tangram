use {
	super::local::Output,
	crate::Component,
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _, future::BoxFuture, stream::BoxStream,
	},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_index::prelude::*,
	tangram_server_core::Session,
	tracing::Instrument as _,
};

/// A process that a spawn hands to its runner.
struct HandOffArg {
	/// The creation of the process's new sandbox, if the spawn creates the sandbox, whose record the spawn writes with the process's.
	create: Option<tg::process::control::SandboxCreate>,

	process: tg::runner::control::Process,

	public: bool,

	/// The process's sandbox, which is absent until the sandbox is ready if its runner derives the sandbox's ID.
	sandbox: Option<tg::sandbox::Id>,
}

/// The result of spawning a process in a new sandbox.
enum NewSandboxOutput {
	/// No scheduler accepts the sandbox's host.
	Declined,

	/// A scheduler enqueued the sandbox.
	Enqueued,

	/// This server's runner created the sandbox.
	Ready(tangram_server_runner_core::sandbox::ReadyEvent),
}

impl Component {
	/// Spawn the process in a new or existing sandbox, returning `None` if no scheduler accepts the new sandbox's host.
	pub(super) async fn spawn_process_local_in_new_or_existing_sandbox(
		&self,
		session: &Session,
		arg: &tg::process::spawn::Arg,
		mut output: Option<Output>,
		mut scheduler_sender: Option<tokio::sync::oneshot::Sender<tg::scheduler::Id>>,
		events: Option<&tokio::sync::mpsc::UnboundedSender<super::Event>>,
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
				let NewSandboxOutput::Ready(ready_event) = self
					.spawn_process_local_in_new_sandbox(session, process, lease, arg.public)
					.await?
				else {
					return Err(tg::error!("expected the sandbox to be ready"));
				};
				let connected_event = ready_event
					.connected_event
					.ok_or_else(|| tg::error!("expected the process to be connected"))?;
				process.data.sandbox = Some(ready_event.sandbox);
				Self::spawn_process_apply_connected(session, process, connected_event)?;
			},
			Some(tg::Either::Left(_)) => {
				let id = process
					.id
					.clone()
					.ok_or_else(|| tg::error!("the scheduled process has no id"))?;
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
				let output = self
					.spawn_process_local_in_new_sandbox(session, process, lease, arg.public)
					.await?;
				if matches!(output, NewSandboxOutput::Declined) {
					return Ok(None);
				}
				if let Some(sender) = scheduler_sender.take() {
					let scheduler = process
						.scheduler
						.clone()
						.ok_or_else(|| tg::error!("missing the scheduler"))?;
					sender.send(scheduler).ok();
				}
				if let Some(events) = events {
					let event = super::CandidateEvent {
						lease,
						process: id.clone(),
					};
					events.send(super::Event::Candidate(event)).ok();
				}
				if arg.wait {
					self.wait_process_start(session, &id, &sandbox, &mut sandbox_connection_future)
						.await?;
				}
				process.lease = Some(lease);
			},
			Some(tg::Either::Right(_)) => {
				let connected_event = self
					.spawn_process_local_in_existing_sandbox(
						session,
						process,
						arg.location.as_ref(),
						arg.public,
					)
					.await?;
				Self::spawn_process_apply_connected(session, process, connected_event)?;
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
			.create_sandbox_status_stream_local_with_wakeups(session, sandbox, None, Some(wakeups));
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
			.is_some_and(|status| !status.is_queued());

		Ok(started)
	}

	fn spawn_process_apply_connected(
		session: &Session,
		output: &mut Output,
		connected_event: tangram_server_runner_core::process::ConnectedEvent,
	) -> tg::Result<()> {
		let tokens = connected_event.process.options.tokens;
		let id = connected_event.process.node;
		output.lease = Some(connected_event.lease);

		// Create a wait token for the process ID that the runner assigned, so that the spawner can read the outcome.
		if tokens.is_empty() {
			let now = session.server.clock.unix_timestamp();
			let token = Self::create_process_wait_token(session, &id, now)?;
			output.tokens = tg::authorization::Tokens::with_authorization(token);
		} else {
			output.tokens = tokens;
		}
		output.id = Some(id);

		Ok(())
	}

	fn spawn_process_runner_arg(
		output: &Output,
		lease: tg::process::Lease,
	) -> tg::runner::control::Process {
		let data = output.data.clone();
		let id = output.id.clone();
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

	#[tracing::instrument(name = "process.spawn_existing", level = "debug", skip_all, fields(process = output.id.as_ref().map(ToString::to_string), sandbox = output.data.sandbox.as_ref().map(ToString::to_string)), err(level = "debug"))]
	async fn spawn_process_local_in_existing_sandbox(
		&self,
		session: &Session,
		output: &Output,
		location: Option<&tg::location::Arg>,
		public: bool,
	) -> tg::Result<tangram_server_runner_core::process::ConnectedEvent> {
		let lease = tg::process::Lease::new();
		let process = Self::spawn_process_runner_arg(output, lease);
		let sandbox = output
			.data
			.sandbox
			.clone()
			.ok_or_else(|| tg::error!("the scheduled process has no sandbox"))?;

		// Send the request to the sandbox's holder in the hand-off.
		let hand_off = {
			let component = self.clone();
			let id = output.id.clone();
			let location = location.cloned();
			let process = process.clone();
			let sandbox = sandbox.clone();
			let session = session.clone();
			async move {
				component
					.spawn_process_local_in_existing_sandbox_request(
						&session,
						&sandbox,
						location.as_ref(),
						process,
						public,
						id.as_ref(),
					)
					.boxed()
					.await
			}
			.boxed()
		};
		let arg = HandOffArg {
			create: None,
			process,
			public,
			sandbox: Some(sandbox),
		};
		let connected_event = self.spawn_process_hand_off(session, arg, hand_off).await?;

		Ok(connected_event)
	}

	async fn spawn_process_local_in_existing_sandbox_request(
		&self,
		session: &Session,
		sandbox: &tg::sandbox::Id,
		location: Option<&tg::location::Arg>,
		process: tg::runner::control::Process,
		public: bool,
		id: Option<&tg::process::Id>,
	) -> tg::Result<tangram_server_runner_core::process::ConnectedEvent> {
		// Send the spawn with the public flag, so that the spawn's holder writes the public permission of a process whose ID the spawn created, and a runner that derives the process's ID carries it to the process's home.
		let queued = process.clone();
		let arg = tg::sandbox::control::SpawnProcessServerRequestArg {
			boot: None,
			process,
			public,
		};
		let request = tg::sandbox::control::ServerRequestArg::SpawnProcess(arg);
		let retry = session.server.config.control.request_retry.clone().into();
		let timeout = session.server.config.control.request_timeout;
		let options = tangram_server_core::control::Options { retry, timeout };
		// Spawn preparation has already checked sandbox access and the request origin.
		let response = if let Some(control) = session
			.server
			.components
			.runner
			.try_get_sandbox_control_runner(sandbox, location)
		{
			// Write the queued record of a process whose ID the spawn created before this server's runner holds the process, because this server is the runner's server, with the boot from the lookup of the control.
			let control_sender = control.sender;
			if id.is_some() {
				self.write_queued_process(session, &queued, sandbox, Some(control.boot), public)
					.await?;
			}
			let result = async {
				let response = control_sender.request(request).await.map_err(
					|error| tg::error!(!error, %sandbox, process = ?id, "the spawn process request failed"),
				)?;
				let std::ops::ControlFlow::Break(response) = response else {
					return Err(tg::error!(%sandbox, process = ?id, "the sandbox is not started"));
				};
				Ok(response)
			}
			.await;

			// Finish the process as failed from this server's memory if this server's runner did not take it, because this server holds the process that it recorded, and return the recorded process.
			match (result, id) {
				(Ok(response), _) => response,
				(Err(error), Some(id)) => {
					let data = queued
						.data
						.clone()
						.without_location_and_tokens_except_sync();
					if let Err(error) = self
						.finish_queued_process(&session.server, id, data, error.to_data_or_id(), 0)
						.await
					{
						tracing::error!(error = %error.trace(), process = %id, "failed to finish the unstarted process");
					}
					let output = tg::sandbox::control::SpawnProcessClientResponseOutput {
						lease: queued.lease,
						process: tg::Referent::with_node(id.clone()),
					};
					tg::sandbox::control::ClientResponseOutput::SpawnProcess(output)
				},
				(Err(error), None) => return Err(error),
			}
		} else {
			// Subscribe before sending the request, so that a stop or destroy between the request and the wait cannot be missed.
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

			// Send the request to the sandbox's holder, and send it again on each status wakeup, so that it reaches the runner once the sandbox starts.
			let arg = tangram_server_core::holder::Arg {
				checkpoint: None,
				deadline: session.server.control_read_deadline(),
				exhaustive: true,
				id: tangram_server_core::control::id(),
				mode: tangram_server_core::holder::Mode::Write,
				options,
				request,
				wakeups: &mut wakeups,
			};
			let output = session
				.server
				.components
				.sandbox
				.request_sandbox_holder(session, sandbox, arg)
				.await?
				.ok_or_else(|| tg::error!("the sandbox status wakeup stream ended"))?;

			// A sandbox that is stopped or destroyed has no holder to spawn the process.
			let status = output
				.indexed
				.and_then(|sandbox| sandbox.data)
				.map(|data| data.data.status);
			match (output.reply, status) {
				(Some(Ok(response)), _) => response,
				(_, Some(tg::sandbox::Status::Destroyed | tg::sandbox::Status::Destroying)) => {
					return Err(tg::error!(
						code = tg::error::Code::Cancellation,
						%sandbox,
						"the sandbox was destroyed"
					));
				},
				(_, Some(tg::sandbox::Status::Stopped | tg::sandbox::Status::Stopping)) => {
					return Err(tg::error!(
						code = tg::error::Code::Cancellation,
						%sandbox,
						"the sandbox is not started"
					));
				},
				(Some(Err(error)), _) => {
					return Err(
						tg::error!(!error, %sandbox, process = ?id, "the spawn process request failed"),
					);
				},
				(None, _) => {
					return Err(
						tg::error!(%sandbox, process = ?id, "failed to find the sandbox to spawn the process"),
					);
				},
			}
		};
		let output = response
			.try_unwrap_spawn_process()
			.map_err(|_| tg::error!("expected a spawn process response"))?;
		if let Some(id) = id
			&& output.process.node != *id
		{
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

	/// Write the queued record of a process with its creator's permissions and the public permission of a public process.
	async fn write_queued_process(
		&self,
		session: &Session,
		process: &tg::runner::control::Process,
		sandbox: &tg::sandbox::Id,
		boot: Option<u64>,
		public: bool,
	) -> tg::Result<()> {
		let now = session.server.clock.unix_timestamp();
		let record =
			Self::create_queued_process_arg(session, process, sandbox, boot, now, true, public)?;
		let arg = tangram_index::write::Arg {
			items: vec![tangram_index::Item::PutProcess(Box::new(record))],
		};
		session
			.server
			.components
			.indexer
			.write(&session.server, arg)
			.await
			.map_err(|error| tg::error!(!error, %sandbox, "failed to write the process"))?;

		Ok(())
	}

	#[tracing::instrument(name = "process.spawn_new", level = "debug", skip_all, fields(process = output.id.as_ref().map(ToString::to_string), sandbox = output.data.sandbox.as_ref().map(ToString::to_string)), err(level = "debug"))]
	async fn spawn_process_local_in_new_sandbox(
		&self,
		session: &Session,
		output: &mut Output,
		lease: tg::process::Lease,
		public: bool,
	) -> tg::Result<NewSandboxOutput> {
		let super::local::SandboxArg { arg, owner } = output
			.sandbox_arg
			.clone()
			.ok_or_else(|| tg::error!("missing the sandbox arg"))?;
		let process = Self::spawn_process_runner_arg(output, lease);
		if let Some(allocation) = output.allocation.take() {
			let location = session.server.location(arg.location.as_ref())?;
			let sandbox = output.data.sandbox.clone();
			let id = sandbox.clone().filter(|_| output.sandbox_token.is_some());
			let token = output.sandbox_token.clone();
			let runner = session.server.components.runner.state().id();
			let create = tg::process::control::SandboxCreate {
				arg: arg.clone(),
				created_at: output.data.created_at,
				creator: session.context.principal.clone(),
				owner: owner.clone(),
				runner,
			};
			let spawn_sandbox_task_arg = tangram_server_runner_core::sandbox::SpawnSandboxTaskArg {
				allocation,
				arg,
				boot: 0,
				creator: session.context.principal.clone(),
				id,
				location,
				owner,
				process: Some(process.clone()),
				public: public && process.id.is_none(),
				scheduled: false,
				sequence: tangram_index::sandbox::put::FIRST_HOLDER_WRITE,
				token,
			};

			// Hand the process to this server's runner with its sandbox in the hand-off, and wait for the sandbox to be ready.
			let hand_off = {
				let session = session.clone();
				async move {
					let task = session
						.server
						.components
						.runner
						.spawn_sandbox_task(&session.server, spawn_sandbox_task_arg);
					let mut events = task.events;
					let event = events
						.recv()
						.await
						.ok_or_else(|| tg::error!("the sandbox event sender was dropped"))?;
					match event {
						Ok(tangram_server_runner_core::sandbox::Event::Destroyed) => {
							Err(tg::error!(
								code = tg::error::Code::Cancellation,
								"the sandbox was destroyed before it became ready"
							))
						},
						Ok(tangram_server_runner_core::sandbox::Event::Ready(ready_event)) => {
							Ok(ready_event)
						},
						Err(error) => {
							// Wait for the sandbox to end, so that the runner records the end of a sandbox that connected before the spawn does.
							while let Some(event) = events.recv().await {
								if matches!(
									event,
									Ok(tangram_server_runner_core::sandbox::Event::Destroyed)
								) {
									break;
								}
							}
							Err(error)
						},
					}
				}
				.boxed()
			};
			let arg = HandOffArg {
				create: Some(create),
				process,
				public,
				sandbox,
			};
			let ready_event = self.spawn_process_hand_off(session, arg, hand_off).await?;

			return Ok(NewSandboxOutput::Ready(ready_event));
		}

		let sandbox = output
			.data
			.sandbox
			.clone()
			.ok_or_else(|| tg::error!("the scheduled process has no sandbox"))?;

		// Give the assignment the connect's opens, and offer the opens to the connect's readers and writers.
		let mut process = process;
		if let Some(open) = output.open.take() {
			process.open = open.requests();
			open.offer();
		}

		let owner_ancestors = session
			.server
			.components
			.scheduler
			.get_owner_ancestors(session, &owner)
			.await?;
		let request = tangram_server_scheduler_core::EnqueueSandboxRequestArg {
			arg,
			boot: 0,
			created_at: session.server.clock.unix_timestamp(),
			creator: session.context.principal.clone(),
			owner,
			owner_ancestors,
			parent: output.parent_sandbox.clone(),
			process: Some(process),
			public,
			runner: None,
			sandbox: sandbox.clone(),
			scheduler: output.scheduler.clone(),
			token: output.sandbox_token.clone(),
		};
		let scheduler = session
			.server
			.components
			.scheduler
			.try_enqueue_sandbox(session, request)
			.await
			.map_err(|error| {
				tg::error!(!error, sandbox = %sandbox, process = ?output.id, "failed to enqueue the sandbox")
			})?;
		let Some(scheduler) = scheduler else {
			return Ok(NewSandboxOutput::Declined);
		};
		output.scheduler = Some(scheduler);

		Ok(NewSandboxOutput::Enqueued)
	}

	/// Hand a process to its runner with the hand-off future, after writing the process's queued record with its creator's permissions in one index write if the spawn created the process's ID, inside the record of its new sandbox in the starting state if the spawn creates the sandbox, and record the process as failed and the new sandbox as destroyed if the hand-off fails. The write and the hand-off run in a task, so that a dropped spawn does not leave the process queued.
	async fn spawn_process_hand_off<T: Send + 'static>(
		&self,
		session: &Session,
		arg: HandOffArg,
		hand_off: BoxFuture<'static, tg::Result<T>>,
	) -> tg::Result<T> {
		let Some(id) = arg.process.id.clone() else {
			return hand_off.await;
		};
		let component = self.clone();
		let session = session.clone();
		let task = tokio::spawn(
			async move {
				component
					.spawn_process_hand_off_task(&session, arg, hand_off)
					.await
			}
			.instrument(tracing::Span::current()),
		);
		task.await
			.map_err(|error| tg::error!(!error, process = %id, "the hand-off task panicked"))?
	}

	async fn spawn_process_hand_off_task<T>(
		&self,
		session: &Session,
		arg: HandOffArg,
		hand_off: BoxFuture<'static, tg::Result<T>>,
	) -> tg::Result<T> {
		let HandOffArg {
			create,
			process,
			public,
			sandbox,
		} = arg;
		let sandbox = sandbox.ok_or_else(|| tg::error!("expected the process's sandbox"))?;

		// Create the queued record of a process in the first boot of a new sandbox with the creator's permissions, the public permission, and the command captures. The holder of a spawn into an existing sandbox writes the process's record.
		let now = session.server.clock.unix_timestamp();
		let process_arg = Self::create_queued_process_arg(
			session,
			&process,
			&sandbox,
			Some(0),
			now,
			true,
			public,
		)?;
		let id = process_arg.id.clone();

		// Carry the queued record in the record of a new sandbox in the starting state, so that the two are written together.
		let sandbox_arg = if let Some(create) = create {
			let mut sandbox_arg = session
				.server
				.components
				.sandbox
				.prepare_sandbox_control_index_arg(session, &sandbox, create)
				.await?;
			if let Some(data) = &mut sandbox_arg.data {
				data.data.status = tg::sandbox::Status::Starting;
			}
			Some(sandbox_arg)
		} else {
			None
		};

		// Write the records of a new sandbox.
		if let Some(sandbox_arg) = sandbox_arg.clone() {
			let mut sandbox_arg = sandbox_arg;
			sandbox_arg.process = Some(Box::new(process_arg));
			let item = tangram_index::Item::PutSandbox(Box::new(sandbox_arg));
			let arg = tangram_index::write::Arg { items: vec![item] };
			session
				.server
				.components
				.indexer
				.write(&session.server, arg)
				.await
				.map_err(
					|error| tg::error!(!error, process = %id, "failed to write the process"),
				)?;
			session
				.server
				.components
				.sandbox
				.spawn_publish_sandbox_status_task(&session.server, &sandbox);
		}

		// Hand the process off. If the hand-off to this server's runner of a new sandbox fails, then this server holds the process and its new sandbox, so it records the sandbox as destroyed and the process as failed from its own memory. The holder of a spawn into an existing sandbox finishes the process.
		let result = hand_off.await;
		if let Err(error) = &result
			&& let Some(sandbox_arg) = sandbox_arg
		{
			let error = error.to_data_or_id();
			if let Err(error) = self
				.spawn_process_destroy_new_sandbox(session, sandbox_arg, error.clone())
				.await
			{
				tracing::error!(error = %error.trace(), %sandbox, "failed to destroy the new sandbox");
			}
			if let Err(error) = self
				.finish_queued_process(&session.server, &id, process.data.clone(), error, 0)
				.await
			{
				tracing::error!(error = %error.trace(), process = %id, "failed to finish the unstarted process");
			}
		}

		result
	}

	/// Record a new sandbox that failed before it became ready as destroyed with the error, as the scheduler records a new sandbox that fails after its acceptance.
	async fn spawn_process_destroy_new_sandbox(
		&self,
		session: &Session,
		mut sandbox_arg: tangram_index::sandbox::put::Arg,
		error: tg::Either<tg::error::Data, tg::error::Id>,
	) -> tg::Result<()> {
		let id = sandbox_arg.id.clone();
		if let Some(data) = &mut sandbox_arg.data {
			data.data.error = Some(error);
			data.data.status = tg::sandbox::Status::Destroyed;
		}
		sandbox_arg.touched_at = session.server.clock.unix_timestamp();
		let arg = tangram_index::write::Arg {
			items: vec![tangram_index::Item::PutSandbox(Box::new(sandbox_arg))],
		};
		session
			.server
			.components
			.indexer
			.write(&session.server, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to put the sandbox in the index"))?;
		session
			.server
			.components
			.sandbox
			.spawn_publish_sandbox_status_task(&session.server, &id);

		Ok(())
	}
}
