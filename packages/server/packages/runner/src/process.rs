use {
	self::control::RunProcessControlTaskArg,
	bytes::Bytes,
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::{self, BoxFuture, Shared},
		stream::BoxStream,
	},
	std::{
		collections::{BTreeMap, BTreeSet},
		path::{Path, PathBuf},
		sync::Arc,
	},
	tangram_client::prelude::*,
	tangram_futures::{
		stream::TryExt as _,
		task::{Stopper, Task},
	},
	tangram_index::prelude::*,
	tangram_messenger::Messenger as _,
	tangram_server_core::runner::process::ConnectedEvent,
	tokio::task::JoinSet,
	tokio_stream::wrappers::UnboundedReceiverStream,
	tracing::Instrument as _,
	{
		crate::Component,
		tangram_server_core::{
			Session,
			context::{Context, Origin},
		},
	},
};

mod control;
mod progress;
mod stdio;
#[cfg(test)]
mod tests;

type CommandFuture = Shared<BoxFuture<'static, tg::Result<tg::process::data::Command>>>;
type ControlConnection = (
	tg::process::control::Header,
	BoxStream<'static, tg::Result<tg::control::Event<tg::process::control::ServerMessage>>>,
);

pub(super) struct SpawnProcessTaskArg<'a> {
	pub guest_url: &'a tangram_uri::Uri,
	pub location: tg::Location,
	pub process: tg::runner::control::Process,
	pub process_stopper: &'a Stopper,
	pub process_tasks: &'a mut JoinSet<tg::Result<()>>,
	pub processes: Arc<tangram_server_core::process::Processes>,
	pub retention_stopper: Stopper,
	pub sandbox: &'a tangram_sandbox::Sandbox,
	pub sandbox_create: Option<tg::process::control::SandboxCreate>,
	pub sandbox_ready_receiver: Option<tokio::sync::oneshot::Receiver<()>>,
}

#[must_use]
pub(super) struct SpawnProcessTaskOutput {
	pub events: tokio::sync::mpsc::UnboundedReceiver<tg::Result<Event>>,
}

struct ProcessTaskArg {
	event_sender: tokio::sync::mpsc::UnboundedSender<tg::Result<Event>>,
	guest_url: tangram_uri::Uri,
	location: tg::Location,
	process: tg::runner::control::Process,
	processes: Arc<tangram_server_core::process::Processes>,
	retention_stopper: Stopper,
	sandbox: tangram_sandbox::Sandbox,
	sandbox_create: Option<tg::process::control::SandboxCreate>,
	sandbox_ready_receiver: Option<tokio::sync::oneshot::Receiver<()>>,
	sandbox_stopper: Stopper,
}

struct FinishProcessRunArg {
	command_push: Option<Task<tg::Result<()>>>,
	control: control::ProcessControlSender,
	exited: Stopper,
	finish_sender: tokio::sync::oneshot::Sender<control::ProcessControlResponseReceiver>,
	id: tg::process::Id,
	index_receiver: tokio::sync::oneshot::Receiver<()>,
	location: tg::Location,
	log_task: Option<Task<tg::Result<tg::Referent<tg::blob::Id>>>>,
	outcome_sync: tokio::sync::oneshot::Sender<()>,
	processes: Arc<tangram_server_core::process::Processes>,
	ready_receiver: tokio::sync::oneshot::Receiver<()>,
	run_task: Task<tg::Result<RunProcessOutcome>>,
	sandbox: tangram_sandbox::Sandbox,
	state: tg::process::State,
}

struct FinishProcessRunOutput {
	index_task: Option<tangram_server_core::process::IndexTask>,
}

struct IndexFinishedProcessTaskArg {
	data: tg::process::Data,
	id: tg::process::Id,
	location: tg::Location,
}

struct FinishProcessTaskArg {
	buffered_task: Task<tg::Result<()>>,
	control_task: Task<tg::Result<()>>,
	id: tg::process::Id,
	log_task: Option<Task<tg::Result<()>>>,
}

struct IndexProcessTaskArg<'a> {
	command: tg::Referent<tg::Either<Box<tg::process::data::Command>, tg::command::Id>>,
	command_data: CommandFuture,
	data: tg::process::Data,
	id: &'a tg::process::Id,
	location: &'a tg::Location,
	options: tg::referent::Options,
	parent: Option<&'a tg::process::Id>,
}

enum LogEvent {
	Chunk {
		bytes: Bytes,
		permit: tokio::sync::OwnedSemaphorePermit,
		stream: tg::process::stdio::Stream,
	},
	End,
}

#[derive(Clone)]
struct LogSender {
	buffer: Arc<tokio::sync::Semaphore>,
	sender: tokio::sync::mpsc::Sender<LogEvent>,
}

struct CollectProcessOutcomeArg<'a> {
	exit: u8,
	path: PathBuf,
	state: &'a tg::process::State,
}

pub(super) enum Event {
	Buffered,
	Connected(ConnectedEvent),
	Exited,
	Released,
}

#[derive(Clone, Debug)]
struct Outcome {
	checksum: Option<tg::Checksum>,
	error: Option<tg::Error>,
	exit: u8,
	output: Option<tg::Value>,
}

struct RunProcessArg {
	command: tg::process::data::Command,
	guest_url: tangram_uri::Uri,
	id: tg::process::Id,
	process_stopper: Stopper,
	processes: Arc<tangram_server_core::process::Processes>,
	progress_sender: tokio::sync::mpsc::UnboundedSender<Bytes>,
	sandbox: tangram_sandbox::Sandbox,
	sandbox_process_sender: tokio::sync::watch::Sender<Option<Arc<tangram_sandbox::Process>>>,
	state: tg::process::State,
	stopper: Stopper,
	token: String,
}

struct WriteProcessLogTaskArg {
	id: tg::process::Id,
	location: tg::Location,
	receiver: tokio::sync::mpsc::Receiver<LogEvent>,
	started_at: i64,
	streams: Vec<tg::process::stdio::Stream>,
}

struct RunProcessOutcome {
	exit: u8,
	path: PathBuf,
}

struct WaitForProcessArg<'a> {
	process_stopper: Stopper,
	sandbox: &'a tangram_sandbox::Sandbox,
	sandbox_process: &'a tangram_sandbox::Process,
	stopper: Stopper,
}

impl LogSender {
	async fn send(&self, event: tangram_sandbox::stdio::read::Event) -> tg::Result<()> {
		match event {
			tangram_sandbox::stdio::read::Event::Chunk(chunk) => {
				let mut offset = 0;
				while offset < chunk.bytes.len() {
					let end = (offset + tg::process::stdio::MAX_CHUNK_SIZE).min(chunk.bytes.len());
					let length = u32::try_from(end - offset).unwrap();
					let permit = self
						.buffer
						.clone()
						.acquire_many_owned(length)
						.await
						.map_err(|error| {
							tg::error!(!error, "failed to reserve space for the process logs")
						})?;
					let bytes = chunk.bytes.slice(offset..end);
					let event = LogEvent::Chunk {
						bytes,
						permit,
						stream: chunk.stream,
					};
					self.sender
						.send(event)
						.await
						.map_err(|_| tg::error!("failed to buffer the process logs"))?;
					offset = end;
				}
			},
			tangram_sandbox::stdio::read::Event::End => {
				self.sender
					.send(LogEvent::End)
					.await
					.map_err(|_| tg::error!("failed to buffer the process logs"))?;
			},
		}

		Ok(())
	}
}

impl Component {
	pub(super) fn spawn_process_task(
		&self,
		session: &Session,
		arg: SpawnProcessTaskArg<'_>,
	) -> SpawnProcessTaskOutput {
		let (event_sender, event_receiver) = tokio::sync::mpsc::unbounded_channel();
		let component = self.clone();
		let session = session.clone();
		let process = arg.process;
		let processes = arg.processes;
		let sandbox = arg.sandbox.clone();
		let sandbox_create = arg.sandbox_create;
		let guest_url = arg.guest_url.clone();
		let location = arg.location;
		let sandbox_ready_receiver = arg.sandbox_ready_receiver;
		let sandbox_stopper = arg.process_stopper.clone();
		let retention_stopper = arg.retention_stopper;
		arg.process_tasks.spawn(
			async move {
				let arg = ProcessTaskArg {
					event_sender: event_sender.clone(),
					guest_url,
					location,
					process,
					processes,
					retention_stopper,
					sandbox,
					sandbox_create,
					sandbox_ready_receiver,
					sandbox_stopper,
				};
				let result = if session.server.shutdown.borrow().is_some() {
					Err(tg::error!("the server is shutting down"))
				} else {
					component.process_task(&session, arg).boxed().await
				};
				if let Err(error) = &result {
					event_sender.send(Err(error.clone())).ok();
				}
				result
			}
			.in_current_span(),
		);
		SpawnProcessTaskOutput {
			events: event_receiver,
		}
	}

	#[tracing::instrument(name = "process.lifecycle", level = "info", skip_all, fields(parent = arg.process.parent.as_ref().map(ToString::to_string), process = arg.process.id.as_ref().map(ToString::to_string), runner = self.state.id().as_ref().map(ToString::to_string), sandbox = tracing::field::Empty), err(level = "debug"))]
	async fn process_task(&self, session: &Session, arg: ProcessTaskArg) -> tg::Result<()> {
		let ProcessTaskArg {
			event_sender,
			guest_url,
			location,
			process,
			processes,
			retention_stopper,
			sandbox,
			sandbox_create,
			mut sandbox_ready_receiver,
			sandbox_stopper,
		} = arg;
		let tg::runner::control::Process {
			data,
			id,
			open,
			options,
			parent,
			token,
		} = process;
		let mut state = tg::process::State::try_from_data(data)?;
		let mut command_options = options.clone();
		command_options
			.tokens
			.inherit(&state.command.options.tokens);
		let local = command_options
			.tokens
			.local_authorization()
			.iter()
			.any(|token| {
				session
					.server
					.components
					.authorization
					.verify_local_token(session, token)
			});
		if local {
			command_options.location = Some(tg::Location::Local(tg::location::Local::default()));
		} else {
			let trusted = match &location {
				tg::Location::Remote(remote) => session
					.server
					.components
					.remote
					.get_remote_session(session, &remote.name)
					.await
					.map_err(
						|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
					)?
					.trusted(),
				tg::Location::Local(_) => false,
			};
			session
				.server
				.components
				.authorization
				.update_tokens_and_location(
					session,
					&mut command_options.tokens,
					Some(&mut command_options.location),
					&location,
					trusted,
				)?;
		}
		state.command.options = command_options;
		let process_stopper = Stopper::new();
		let lease = tg::process::Lease::new();
		let (requests_sender, requests_receiver) = tokio::sync::oneshot::channel::<
			futures::stream::BoxStream<
				'static,
				tg::Result<tg::control::Event<tg::process::control::ServerMessage>>,
			>,
		>();
		let requests = futures::stream::once(async move {
			requests_receiver
				.await
				.map_err(|_| tg::error!("failed to receive the process control stream"))
		})
		.try_flatten()
		.boxed();
		let config = session.server.config.process.control;
		let control = tg::process::control::Connection::with_input(requests, config);
		let control_sender = control.sender();

		// Determine the process's identity. A process on the shortcut path derives its ID from the runner's ID, so that it starts without waiting for the server, and its start proves the derivation, its parent, and its sandbox.
		let (outcome_sync_sender, outcome_sync_receiver) = tokio::sync::oneshot::channel();
		let mut command_push = None;
		let (inner_token_sender, inner_token) = tokio::sync::watch::channel(None::<String>);
		let (id, command_session, shortcut) = match (id, token) {
			(Some(id), Some(token)) => {
				inner_token_sender.send_replace(Some(token));
				(id, None, None)
			},
			(None, None) => {
				let parent = parent.as_ref().ok_or_else(|| {
					tg::error!("a process on the shortcut path must have a parent")
				})?;
				let mut command_session = Self::try_get_process_session(session, parent)
					.ok_or_else(
						|| tg::error!(%parent, "failed to find the parent process session"),
					)?;
				command_session.context.stopper = None;
				if let Some(receiver) = sandbox_ready_receiver.take() {
					receiver.await.map_err(|error| {
						tg::error!(!error, "the sandbox failed before becoming ready")
					})?;
				}

				// Derive the ID.
				let runner = session
					.server
					.components
					.runner
					.state()
					.id()
					.ok_or_else(|| tg::error!("the runner has no id"))?;
				let (id, proof) = tg::process::Id::with_runner(&runner);

				// Prove the parent with its token, or, while the runner waits for the parent's token, with the parent's control service.
				let parent_token = Self::try_get_process_inner_token(session, parent);
				let parent = tg::process::control::Parent {
					id: parent.clone(),
					token: parent_token,
				};

				// Prove the sandbox with its token, or, while the runner waits for the sandbox's token, with the sandbox's control service.
				let sandbox_token = state
					.sandbox
					.as_ref()
					.and_then(|sandbox| self.state.sandboxes.get_by_id(sandbox))
					.map(|sandbox| sandbox.token.borrow().clone())
					.ok_or_else(|| tg::error!(%id, "missing the process's sandbox"))?;
				let sandbox = tg::process::control::Sandbox {
					create: sandbox_create,
					token: sandbox_token,
				};
				let shortcut = tg::process::control::Shortcut { parent, sandbox };

				// Push the command objects concurrently with the start, joining the process's command sync through its control service.
				if !Self::is_local_process_location(session, &location) {
					let component = self.clone();
					let session = command_session.clone();
					let data = state.to_data();
					let id = id.clone();
					let location = location.clone();
					command_push = Some(Task::spawn(move |_| {
						async move {
							component
								.push_process_command(&session, &id, &data, &location)
								.await
						}
						.in_current_span()
					}));
				}

				(id, Some(command_session), Some((proof, shortcut)))
			},
			_ => {
				return Err(tg::error!(
					"the process id and token must be provided together"
				));
			},
		};

		tracing::Span::current().record("process", tracing::field::display(&id));
		tracing::Span::current().record("sandbox", state.sandbox.as_ref().map(ToString::to_string));
		tracing::info!(parent = parent.as_ref().map(ToString::to_string), process = %id, runner = self.state.id().as_ref().map(ToString::to_string), sandbox = state.sandbox.as_ref().map(ToString::to_string), "initialized the process");
		let context = tangram_server_core::context::Context {
			principal: tg::Principal::Process(id.clone()),
			token: inner_token.borrow().clone(),
			..session.context.clone()
		};
		let session = session.server.session(&context);
		let command_session = command_session.unwrap_or_else(|| session.clone());
		let sandbox_index = sandbox.index();

		// Store the process state before starting the process.
		let mut data = state.to_data();
		let mut children = indexmap::IndexMap::default();
		for child in data.children.take().unwrap_or_default() {
			let id = child.process.node.clone();
			let child = tangram_server_core::process::Child {
				data: child,
				lease: None,
				location: None,
			};
			if children.insert(id, child).is_some() {
				return Err(tg::error!("the process children must be unique"));
			}
		}
		let (index_result_sender, index_result_receiver) = tokio::sync::oneshot::channel();
		let index_task = tangram_server_core::process::IndexTask::spawn(move |_| {
			async move {
				index_result_receiver.await.map_err(|error| {
					tg::error!(!error, "failed to receive the process index result")
				})?
			}
			.in_current_span()
		});
		let sandbox_id = state
			.sandbox
			.clone()
			.ok_or_else(|| tg::error!(%id, "the running process has no sandbox"))?;
		let capacity = session.server.config.process.local_control_channel_capacity;
		let (local_control_sender, control_receiver) =
			tangram_server_core::process::control::local::Local::new(capacity);
		let entry = tangram_server_core::process::State {
			changed: tokio::sync::watch::channel(()).0,
			children,
			control: control_sender.clone(),
			control_sender: local_control_sender.clone(),
			data,
			finish: None,
			index_task: index_task.clone(),
			inner_token: inner_token.clone(),
			leases: BTreeSet::from([lease]),
			parent: parent.clone(),
			process: None,
			stopper: process_stopper.clone(),
		};
		tangram_server_core::checkpoint!(session.server, "runner.process.state.insert", process = %id).await;
		match processes.entry(id.clone()) {
			dashmap::Entry::Occupied(_) => {
				return Err(tg::error!(%id, "the process ID is already in use"));
			},
			dashmap::Entry::Vacant(process) => {
				process.insert(entry);
			},
		}
		session
			.server
			.components
			.runner
			.state()
			.processes
			.insert(id.clone(), sandbox_id.clone());
		if let Some(mut sandbox) = session
			.server
			.components
			.runner
			.state()
			.sandboxes
			.get_mut_by_id(&sandbox_id)
		{
			// Keep the IDs after individual process state expires.
			if sandbox.process_ids.insert(id.clone(), ()).is_none() {
				sandbox.changed.send_replace(());
			}
		}
		let processes_for_cleanup = processes.clone();
		let id_for_cleanup = id.clone();
		let server = session.server.clone();
		scopeguard::defer! {
			processes_for_cleanup.remove(&id_for_cleanup);
			server.components.runner.state().processes.remove(&id_for_cleanup);

			// Let tests observe that the runner discarded the process state.
			if server.checkpoints.is_some() {
				let server = server.clone();
				let id = id_for_cleanup.clone();
				tokio::spawn(async move {
					tangram_server_core::checkpoint!(server, "runner.process.state.removed", process = %id).await;
				});
			}
		}
		tangram_server_core::checkpoint!(
			session.server,
			"runner.process.state.inserted",
			process = %id,
		)
		.await;

		// Register the token before starting the process.
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		let token = loop {
			let bytes = rand::random::<[u8; 32]>();
			let token = ENCODING.encode(&bytes);
			match session
				.server
				.components
				.runner
				.state()
				.process_for_token
				.entry(token.clone())
			{
				dashmap::mapref::entry::Entry::Occupied(_) => {},
				dashmap::mapref::entry::Entry::Vacant(entry) => {
					entry.insert((sandbox_index, id.clone()));
					break token;
				},
			}
		};
		let server_for_token_cleanup = session.server.clone();
		let token_for_cleanup = token.clone();
		scopeguard::defer! {
			server_for_token_cleanup
				.components
				.runner
				.state()
				.process_for_token
				.remove(&token_for_cleanup);
		}

		// Load the command concurrently with the control stream.
		let command: CommandFuture = {
			let command = state.command.clone();
			let command_session = command_session.clone();
			let session = session.clone();
			let server = session.server.clone();
			async move {
				let command = match command.node {
					tg::Either::Left(mut data) => {
						data.inherit_location_and_tokens(&command.options);
						return Ok(*data);
					},
					tg::Either::Right(id) => {
						tg::Command::with_referent(tg::Referent::new(id, command.options))
					},
				};
				// Check whether the command is available locally.
				let command_id: tg::object::Id = command.id().into();
				let local = server
					.components
					.object
					.try_get_object_local_with_server(&server, &command_id, false)
					.await
					.map_err(|error| tg::error!(!error, "failed to get the local command"))?
					.is_some();

				// Preserve the parent's local command authorization on the shortcut path.
				let session = if local { command_session } else { session };

				// Load the command.
				let data = command
					.data_with_instance(&session)
					.await
					.map_err(|error| tg::error!(!error, "failed to get the command data"))?;

				let options = command.to_referent().options;
				let command = tg::process::data::Command::with_command_data(data, &options);
				Ok(command)
			}
			.instrument(tracing::debug_span!("process.command", process = %id))
			.boxed()
			.shared()
		};

		// Create the progress and log streams.
		let (progress_sender, progress_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
		let progress = UnboundedReceiverStream::new(progress_receiver)
			.filter(|bytes| future::ready(!bytes.is_empty()))
			.map(Ok::<_, tg::Error>)
			.boxed();
		let (log_progress, stderr_progress) = match state.stderr {
			tg::process::Stdio::Log => (Some(progress), None),
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty => (None, Some(progress)),
			tg::process::Stdio::Blob(_)
			| tg::process::Stdio::Inherit
			| tg::process::Stdio::Null => (None, None),
		};
		let mut log_streams = Vec::new();
		if matches!(state.stdout, tg::process::Stdio::Log) {
			log_streams.push(tg::process::stdio::Stream::Stdout);
		}
		if matches!(state.stderr, tg::process::Stdio::Log) {
			log_streams.push(tg::process::stdio::Stream::Stderr);
		}
		let (log_sender, log_receiver) = if log_streams.is_empty() {
			(None, None)
		} else {
			let config = &session.server.config.runner;
			let buffer = Arc::new(tokio::sync::Semaphore::new(config.log_buffer_size));
			let (sender, receiver) = tokio::sync::mpsc::channel(config.log_channel_capacity);
			let sender = LogSender { buffer, sender };
			(Some(sender), Some(receiver))
		};

		// Start the process task concurrently with the control stream.
		let (sandbox_process_sender, sandbox_process_receiver) =
			tokio::sync::watch::channel::<Option<Arc<tangram_sandbox::Process>>>(None);
		let (log_buffered_sender, log_buffered_receiver) = tokio::sync::oneshot::channel();
		let log_task = match log_sender {
			None => {
				log_buffered_sender.send(Ok(())).ok();

				None
			},
			Some(log_sender) => Some(Task::spawn({
				let log_streams = log_streams.clone();
				let process_stopper = process_stopper.clone();
				let sandbox = sandbox.clone();
				let mut sandbox_process = sandbox_process_receiver.clone();
				move |_| {
					async move {
						let mut log_buffered_sender = Some(log_buffered_sender);
						let result = async {
							let sandbox_process = sandbox_process
								.wait_for(Option::is_some)
								.await
								.ok()
								.and_then(|process| process.as_ref().cloned());
							let input = match sandbox_process {
								None => {
									// A process that never spawned still needs to drain progress and finish its log.
									futures::stream::once(future::ok(
										tangram_sandbox::stdio::read::Event::End,
									))
									.boxed()
								},
								Some(sandbox_process) => sandbox
									.read_stdio(&sandbox_process, log_streams)
									.await
									.map_err(|error| {
										tg::error!(!error, "failed to read process stdio")
									})?
									.boxed(),
							};

							// Drain progress along with the process output.
							let input = match log_progress {
								Some(progress) => {
									let progress = progress
										.map_ok(|bytes| {
											tangram_sandbox::stdio::read::Event::Chunk(
												tangram_sandbox::stdio::Chunk {
													bytes,
													stream: tg::process::stdio::Stream::Stderr,
												},
											)
										})
										.boxed();
									futures::stream::select(input, progress).boxed()
								},
								None => input,
							};
							let mut input = std::pin::pin!(input);
							while let Some(event) = input.try_next().await? {
								if matches!(event, tangram_sandbox::stdio::read::Event::End) {
									if let Some(sender) = log_buffered_sender.take() {
										sender.send(Ok(())).ok();
									}

									continue;
								}
								log_sender.send(event).await?;
							}
							Ok::<_, tg::Error>(())
						}
						.await;
						if let Some(sender) = log_buffered_sender {
							let error = result.as_ref().err().cloned().unwrap_or_else(|| {
								tg::error!("the sandbox stdio stream ended unexpectedly")
							});
							sender.send(Err(error)).ok();
						}
						if result.is_err() {
							process_stopper.stop();
						}

						// Finish the received log even if the sandbox died while producing it.
						let end_result = log_sender
							.send(tangram_sandbox::stdio::read::Event::End)
							.await;

						result.and(end_result)
					}
					.in_current_span()
				}
			})),
		};
		let run_span = tracing::Span::current();
		let run_task = Task::spawn({
			let command = command.clone();
			let guest_url = guest_url.clone();
			let id = id.clone();
			let processes = processes.clone();
			let process_stopper = process_stopper.clone();
			let component = self.clone();
			let sandbox = sandbox.clone();
			let session = session.clone();
			let state = state.clone();
			let stopper = sandbox_stopper.clone();
			let token = token.clone();
			move |_| {
				async move {
					let command = command.await?;
					tangram_server_core::checkpoint!(session.server, "runner.process.run", process = %id).await;
					let arg = RunProcessArg {
						command,
						guest_url,
						id,
						process_stopper,
						processes,
						progress_sender,
						sandbox,
						sandbox_process_sender,
						state,
						stopper,
						token,
					};
					component.run_process(&session, arg).await
				}
				.instrument(run_span)
			}
		});

		let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
		let (index_sender, index_receiver) = tokio::sync::oneshot::channel();
		let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();

		// Write the log concurrently with execution and output collection.
		let log_write_task = log_receiver
			.map(|receiver| {
				let started_at = state
					.started_at
					.ok_or_else(|| tg::error!("expected the process to be started"))?;
				let arg = WriteProcessLogTaskArg {
					id: id.clone(),
					location: location.clone(),
					receiver,
					started_at,
					streams: log_streams.clone(),
				};
				let component = self.clone();
				let session = session.clone();
				let task = Task::spawn(move |_| {
					async move {
						component
							.write_process_log_task(&session, arg)
							.boxed()
							.await
					}
					.in_current_span()
				});
				Ok::<_, tg::Error>(task)
			})
			.transpose()?;

		// Collect and store the output concurrently with the control connection.
		let exited = Stopper::new();
		let finish_arg = FinishProcessRunArg {
			command_push,
			control: control.sender(),
			exited: exited.clone(),
			finish_sender,
			id: id.clone(),
			index_receiver,
			location: location.clone(),
			log_task: log_write_task,
			outcome_sync: outcome_sync_sender,
			processes: processes.clone(),
			ready_receiver,
			run_task,
			sandbox: sandbox.clone(),
			state: state.clone(),
		};
		let finish_span = tracing::Span::current();
		let finish_task = Task::spawn({
			let component = self.clone();
			let session = session.clone();
			move |_| {
				async move {
					component
						.finish_process_run(&session, finish_arg)
						.boxed()
						.await
				}
				.instrument(finish_span)
			}
		});

		// Spawn the process control task.
		let (stderr_buffered_sender, stderr_buffered_receiver) = tokio::sync::oneshot::channel();
		let (stdout_buffered_sender, stdout_buffered_receiver) = tokio::sync::oneshot::channel();
		let control_task = Task::spawn({
			let component = self.clone();
			let session = session.clone();
			let exited = exited.clone();
			let sandbox = sandbox.clone();
			let stdin = state.stdin.clone();
			let stdout = state.stdout.clone();
			let stderr = state.stderr.clone();
			let location = location.clone();
			|_| {
				async move {
					let arg = RunProcessControlTaskArg {
						control,
						exited,
						finish: finish_receiver,
						local: control_receiver,
						location,
						outcome_sync: outcome_sync_receiver,
						retention_stopper,
						sandbox,
						sandbox_process: sandbox_process_receiver,
						stderr,
						stderr_buffered: stderr_buffered_sender,
						stderr_progress,
						stdin,
						stdout,
						stdout_buffered: stdout_buffered_sender,
					};
					component
						.run_process_control_task(&session, arg)
						.boxed()
						.await
						.inspect_err(|error| {
							tracing::error!(error = %error.trace(), "the control task failed");
						})
				}
				.in_current_span()
			}
		});

		// Service the assignment's opens as if each arrived over process control.
		for request in open {
			local_control_sender.send(request).await?;
		}

		// Wait for sandbox control to be ready before connecting process control.
		if let Some(receiver) = sandbox_ready_receiver
			&& let Err(error) = receiver.await
		{
			process_stopper.stop();
			drop(index_sender);
			drop(ready_sender);
			finish_task.wait().await.ok();
			return Err(tg::error!(
				!error,
				"the sandbox failed before becoming ready"
			));
		}

		// Allow finish to proceed once the sandbox control connection is ready.
		ready_sender.send(()).ok();

		let data = state.to_data();

		// Connect process control while the process runs. A process on the shortcut path connects as the runner in the background, so that nothing waits for the server, and receives its process token in the header.
		let shortcut_start = shortcut.is_some();
		let (proof, shortcut) = shortcut.unzip();
		let start = tg::process::control::StartClientRequestArg {
			data: data.clone(),
			lease,
			options: options.clone(),
			shortcut,
		};
		let arg = tg::process::control::Arg {
			id: id.clone(),
			location: Some(location.clone().into()),
			mode: tg::process::control::Mode::Start(start),
			proof,
		};
		let connect = if shortcut_start {
			let component = self.clone();
			let session = Self::runner_session(&session);
			let process_stopper = process_stopper.clone();
			let server = session.server.clone();
			let process = id.clone();
			let mut task = Task::spawn(move |_| {
				async move {
					tangram_server_core::checkpoint!(server, "runner.process.control.start.sent", process = %process).await;
					let result = component
						.connect_process_control(&session, arg, control_sender)
						.await
						.and_then(|(header, requests)| {
							requests_sender.send(requests).map_err(|_| {
								tg::error!("the process control stream was dropped")
							})?;
							Ok(header)
						});
					match result {
						Ok(header) => {
							inner_token_sender.send_replace(header.token);
							tangram_server_core::checkpoint!(server, "runner.process.control.start.succeeded", process = %process).await;
						},
						Err(error) => {
							tracing::error!(error = %error.trace(), "failed to connect the process control stream");
							process_stopper.stop();
						},
					}
				}
				.in_current_span()
			});
			task.detach();
			Ok(())
		} else {
			self.connect_process_control(&session, arg, control_sender)
				.await
				.and_then(|(_, requests)| {
					requests_sender
						.send(requests)
						.map_err(|_| tg::error!("the process control stream was dropped"))
				})
		};
		if let Err(error) = connect {
			process_stopper.stop();
			drop(index_sender);
			finish_task.wait().await.ok();

			return Err(error);
		}
		session
			.server
			.notifications
			.publish_sandbox_process_spawned(&sandbox_id)
			.await?;

		// Index the remote process before reporting the connection.
		let arg = IndexProcessTaskArg {
			command: state.command.clone(),
			command_data: command.clone(),
			data,
			id: &id,
			location: &location,
			options,
			parent: parent.as_ref(),
		};
		let index_result = self.spawn_index_process_task(&session, arg).await;
		index_result_sender.send(index_result.clone()).ok();
		if let Err(error) = index_result {
			process_stopper.stop();
			drop(index_sender);
			finish_task.wait().await.ok();

			return Err(error);
		}
		tangram_server_core::checkpoint!(session.server, "runner.process.index.signal", process = %id)
			.await;
		index_sender.send(()).ok();

		if location.is_remote() {
			let entry = tangram_server_core::process::control::Connected { lease };
			let result = session
				.server
				.messenger
				.publish(
					tangram_server_core::process::control::connected_subject(&id),
					entry,
				)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to publish the process control connection"),
				);
			if let Err(error) = result {
				process_stopper.stop();
				finish_task.wait().await.ok();

				return Err(error);
			}
		}
		event_sender
			.send(Ok(Event::Connected(ConnectedEvent {
				lease,
				process: tg::Referent::with_node(id.clone()),
			})))
			.ok();

		let output = finish_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the process finish task panicked"))??;
		let FinishProcessRunOutput { index_task } = output;
		event_sender.send(Ok(Event::Exited)).ok();
		self.release_finished_process_children(&session, &id, &processes)
			.await?;

		let buffered_task = Task::spawn({
			let event_sender = event_sender.clone();
			let id = id.clone();
			let server = session.server.clone();
			move |_| {
				async move {
					let log_buffered = log_buffered_receiver
						.await
						.is_ok_and(|result| result.is_ok());
					let stderr_buffered = stderr_buffered_receiver
						.await
						.is_ok_and(|result| result.is_ok());
					let stdout_buffered = stdout_buffered_receiver
						.await
						.is_ok_and(|result| result.is_ok());
					let buffered = log_buffered && stderr_buffered && stdout_buffered;
					let event = if buffered {
						tangram_server_core::checkpoint!(
							server,
							"runner.process.buffered",
							process = %id,
						)
						.await;
						Event::Buffered
					} else {
						Event::Released
					};
					event_sender.send(Ok(event)).ok();

					Ok::<_, tg::Error>(())
				}
				.in_current_span()
			}
		});
		let arg = FinishProcessTaskArg {
			buffered_task,
			control_task,
			id: id.clone(),
			log_task,
		};

		let result = self.finish_process_task(arg).boxed().await;
		if let Some(index_task) = index_task {
			index_task.wait().await.map_err(|error| {
				tg::error!(!error, "the finished process index task panicked")
			})??;
		}
		result?;
		tracing::info!(process = %id, "completed the process cleanup");

		Ok(())
	}

	#[tracing::instrument(name = "process.connect", level = "debug", skip_all, fields(process = %arg.id), err(level = "debug"))]
	async fn connect_process_control(
		&self,
		session: &Session,
		arg: tg::process::control::Arg,
		sender: tg::process::control::Sender,
	) -> tg::Result<ControlConnection> {
		tangram_server_core::checkpoint!(
			session.server,
			"runner.process.control.connect",
			process = %arg.id
		)
		.await;

		// Tell the server when this runner controls a local process, whose outcome it stores directly.
		let location = session.server.location(arg.location.as_ref())?;
		let local_process_control = Self::is_local_process_location(session, &location);
		let context = tangram_server_core::context::Context {
			local_process_control,
			..session.context.clone()
		};
		let session = session.server.session(&context);
		let reconnect_server = session.server.clone();
		let id = arg.id.clone();
		let reconnect = move |header: &tg::process::control::Header| {
			let token = header.token.clone().or(context.token.clone());
			let context = tangram_server_core::context::Context {
				principal: tg::Principal::Process(id),
				token,
				..context
			};
			reconnect_server.session(&context)
		};
		let (header, requests) =
			tg::process::control::Connection::open_stream(&session, arg, sender, reconnect)
				.boxed()
				.await
				.map_err(|source| tg::error!(!source, "failed to create the control stream"))?
				.ok_or_else(|| tg::error!("expected a control stream"))?;
		Ok((header, requests.boxed()))
	}

	#[tracing::instrument(name = "process.finish", level = "debug", skip_all, fields(process = %arg.id, sandbox = arg.state.sandbox.as_ref().map(ToString::to_string)), err(level = "debug"))]
	async fn finish_process_run(
		&self,
		session: &Session,
		arg: FinishProcessRunArg,
	) -> tg::Result<FinishProcessRunOutput> {
		let FinishProcessRunArg {
			command_push,
			control,
			exited,
			finish_sender,
			id,
			index_receiver,
			location,
			log_task,
			outcome_sync,
			processes,
			ready_receiver,
			run_task,
			sandbox,
			state,
		} = arg;
		let result = run_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the process task panicked"))?;

		// The sandbox's connection is closed once the process exits, so stdin can no longer be written to.
		tangram_server_core::checkpoint!(session.server, "runner.process.exited", process = %id)
			.await;
		exited.stop();

		let finish = processes
			.get_mut(&id)
			.ok_or_else(|| tg::error!(?id, "failed to find the process"))?
			.finish
			.take()
			.filter(|_| match &result {
				Ok(_) => true,
				Err(error) => matches!(error.to_data_or_id(), tg::Either::Left(data) if matches!(data.code, Some(tg::error::Code::Cancellation))),
			});
		let result = match result {
			Ok(outcome) => {
				let context = tangram_server_core::context::Context {
					origin: tangram_server_core::context::Origin::Sandbox(sandbox.index()),
					..session.context.clone()
				};
				let outcome_session = session.server.session(&context);
				self.collect_process_outcome(
					&outcome_session,
					CollectProcessOutcomeArg {
						exit: outcome.exit,
						path: outcome.path,
						state: &state,
					},
				)
				.await
			},
			Err(error) => Err(error),
		};

		let outcome = if let Some(finish) = finish {
			let error = finish
				.error
				.map(tg::Error::try_from)
				.transpose()
				.map_err(|error| tg::error!(!error, "failed to deserialize the process error"))?;
			Outcome {
				checksum: None,
				error,
				exit: finish.exit,
				output: None,
			}
		} else {
			match result {
				Ok(outcome) => outcome,
				Err(error) => {
					let code = match error.to_data_or_id() {
						tg::Either::Left(data) => data.code.unwrap_or(tg::error::Code::Internal),
						tg::Either::Right(_) => tg::error::Code::Internal,
					};
					let error =
						tg::error!(!error, code = code, process = %id, "failed to run the process");
					Outcome {
						checksum: None,
						error: Some(error),
						exit: 1,
						output: None,
					}
				},
			}
		};

		// Store the output.
		if let Some(value) = &outcome.output {
			value
				.store_with_instance(session)
				.await
				.map_err(|error| tg::error!(!error, "failed to store the output"))?;
		}

		// Store the error.
		let (mut error, error_code) = if let Some(error) = &outcome.error {
			let error = error.to_data_or_id().map_right(|_| error.to_referent());
			let error_code = match &error {
				tg::Either::Left(data) => data.code,
				tg::Either::Right(_) => None,
			};
			let error = session
				.server
				.components
				.process
				.store_process_error(session, error)
				.await;
			(Some(error), error_code)
		} else {
			(None, None)
		};
		let mut exit = outcome.exit;

		let process_state = processes
			.get(&id)
			.ok_or_else(|| tg::error!(?id, "failed to find the process"))?;
		let mut data = process_state.data();
		drop(process_state);
		if matches!(
			error_code,
			Some(
				tg::error::Code::Cancellation
					| tg::error::Code::HeartbeatExpiration
					| tg::error::Code::Internal
			)
		) {
			data.cacheable = false;
		}
		if let Some(expected) = &data.checksum.expected
			&& exit == 0
		{
			if let Some(actual) = &outcome.checksum
				&& expected != actual
			{
				error = Some(tg::Either::Left(tg::error::Data {
					code: Some(tg::error::Code::ChecksumMismatch),
					message: Some("checksum mismatch".into()),
					values: [
						("expected".into(), expected.to_string()),
						("actual".into(), actual.to_string()),
					]
					.into(),
					..Default::default()
				}));
				exit = 1;
			} else if outcome.checksum.is_none() && !expected.is_any() {
				return Err(tg::error!(?id, "the actual checksum was not set"));
			}
		}
		data.checksum.actual = outcome.checksum.clone();
		data.set_error(error);
		data.exit = Some(exit);
		tangram_server_core::checkpoint!(session.server, "runner.process.output.stored", process = %id).await;
		let log = match log_task {
			Some(task) => match task.wait().await {
				Ok(result) => result.map(Some),
				Err(error) => Err(tg::error!(!error, "the process log write task panicked")),
			},
			None => Ok(None),
		};
		data.log = match log {
			Ok(log) => log,
			Err(error) => {
				let error = tg::error!(!error, "failed to store the process log");
				data.cacheable = false;
				let error = session
					.server
					.components
					.process
					.store_process_error(
						session,
						error.to_data_or_id().map_right(|_| error.to_referent()),
					)
					.await;
				data.set_error(Some(error));
				exit = 1;
				data.exit = Some(exit);
				None
			},
		};
		data.finished_at = Some(session.server.clock.unix_timestamp());
		data.set_output(outcome.output.as_ref().map(tg::Value::to_data));
		data.status = tg::process::Status::Finished;
		tangram_server_core::process::put::validate_process_data(&data)?;
		tracing::info!(exit, process = %id, sandbox = state.sandbox.as_ref().map(ToString::to_string), "collected the process output");

		let command_id = state.command.command_id()?;
		tangram_server_core::checkpoint!(session.server, "runner.process.finish", command = %command_id, process = %id).await;

		ready_receiver
			.await
			.map_err(|_| tg::error!("the process connection failed before initialization"))?;

		// Publish completion, which records the children that the finished data lists.
		self.publish_finished_process(session, &id, &processes, &mut data)
			.await?;

		// Index completion and capture permissions independently of the output transfer.
		let arg = IndexFinishedProcessTaskArg {
			data: data.clone(),
			id: id.clone(),
			location: location.clone(),
		};
		let index_task = tangram_server_core::process::IndexTask::spawn({
			let component = self.clone();
			let session = session.clone();
			move |_| {
				async move {
					index_receiver
						.await
						.map_err(|_| tg::error!("the process connection failed before indexing"))?;
					let inner = session.clone();
					session
						.server
						.index_tasks
						.spawn(move |_| {
							async move {
								component
									.index_finished_process_task(&inner, arg)
									.boxed()
									.await
							}
							.in_current_span()
						})
						.wait()
						.await
						.map_err(|_| tg::error!("the finished process index task panicked"))?
				}
				.in_current_span()
			}
		});
		let index_task = Some(index_task);

		// Retain the finish request across retries and reconnects without waiting for its response.
		tangram_server_core::checkpoint!(session.server, "runner.process.control.finish.request")
			.await;
		let mut finish_data = data.clone();
		Self::inherit_process_control_tokens(&mut finish_data, &location);
		let arg = tg::process::control::ClientRequestArg::Finish(
			tg::process::control::FinishClientRequestArg { data: finish_data },
		);
		let response = Self::send_process_control_client_request_inner(
			&control,
			arg,
			tg::process::control::Priority::High,
		)
		.await?;
		finish_sender
			.send(response)
			.map_err(|_| tg::error!("failed to send the process finish response receiver"))?;
		tangram_server_core::checkpoint!(session.server, "runner.process.control.finish.sent", process = %id).await;
		tracing::debug!(process = %id, "sent the process finish request");

		// Push the outcome objects concurrently with the finish request, and retain control until the pushes complete.
		let component = self.clone();
		let session = session.clone();
		let mut outcome_task = Task::spawn(move |_| {
			async move {
				if let Some(task) = command_push {
					let result = task
						.wait()
						.await
						.map_err(|error| tg::error!(!error, "the command push task panicked"))
						.and_then(|result| result);
					if let Err(error) = result {
						tracing::error!(error = %error.trace(), process = %id, "failed to push the process command");
					}
				}
				// Push the outcome objects to a location that this server does not store, joining the process's outcome sync through its control service.
				if !Self::is_local_process_location(&session, &location) {
					let result = component
						.push_process_outcome(&session, &id, &location, &data)
						.await;
					if let Err(error) = result {
						tracing::error!(error = %error.trace(), process = %id, "failed to push the process outcome");
					}
				}
				outcome_sync.send(()).ok();
			}
			.in_current_span()
		});
		outcome_task.detach();
		let output = FinishProcessRunOutput { index_task };

		Ok(output)
	}

	async fn push_process_command(
		&self,
		session: &Session,
		id: &tg::process::Id,
		data: &tg::process::Data,
		location: &tg::Location,
	) -> tg::Result<()> {
		let sync = tg::sync::Id::with_process(id, tg::sync::join::Kind::Command);
		tangram_server_core::checkpoint!(session.server, "runner.process.command.sync.started", sync = %sync).await;
		let mut data = data.clone();
		Self::inherit_process_control_tokens(&mut data, location);
		let nodes = data
			.command
			.objects()
			.into_iter()
			.map(|node| node.map(Into::into))
			.collect();
		let join = tg::sync::Join::Process(tg::sync::join::Process {
			id: id.clone(),
			kind: tg::sync::join::Kind::Command,
		});
		let arg = tangram_server_core::process::PushProcessObjectsArg {
			join: Some(join),
			location: location.clone(),
			nodes,
			process_command_objects: false,
			sync: None,
		};
		let output = session
			.server
			.components
			.process
			.push_process_objects(session, arg)
			.await?;
		output
			.task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the command push task panicked"))??;
		tangram_server_core::checkpoint!(session.server, "runner.process.command.sync.finished")
			.await;
		Ok(())
	}

	async fn push_process_outcome(
		&self,
		session: &Session,
		id: &tg::process::Id,
		location: &tg::Location,
		data: &tg::process::Data,
	) -> tg::Result<()> {
		let sync = tg::sync::Id::with_process(id, tg::sync::join::Kind::Outcome);
		tangram_server_core::checkpoint!(session.server, "runner.process.outcome.sync.started", process = %id, sync = %sync).await;
		let mut data = data.clone();
		Self::inherit_process_control_tokens(&mut data, location);
		let nodes = Self::process_control_outcome_objects(&data)
			.into_iter()
			.map(|node| node.map(Into::into))
			.collect::<Vec<_>>();
		if !nodes.is_empty() {
			let join = tg::sync::Join::Process(tg::sync::join::Process {
				id: id.clone(),
				kind: tg::sync::join::Kind::Outcome,
			});
			let arg = tangram_server_core::process::PushProcessObjectsArg {
				join: Some(join),
				location: location.clone(),
				nodes,
				process_command_objects: false,
				sync: None,
			};
			let output = session
				.server
				.components
				.process
				.push_process_objects(session, arg)
				.await?;
			output
				.task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the outcome push task panicked"))??;
		}
		tangram_server_core::checkpoint!(session.server, "runner.process.outcome.sync.finished", process = %id)
			.await;
		Ok(())
	}

	fn process_control_outcome_objects(
		data: &tg::process::Data,
	) -> Vec<tg::Referent<tg::object::Id>> {
		let mut objects = Vec::new();
		if let Some(output) = data.output_with_location_and_tokens() {
			output.children_with_tokens(&mut objects);
		}
		if let Some(tg::Either::Right(error)) = data.error_with_location_and_tokens() {
			objects.push(error.map(tg::object::Id::Error));
		}
		if let Some(log) = &data.log {
			objects.push(log.clone().map(tg::object::Id::from));
		}
		objects
	}

	fn inherit_process_control_tokens(data: &mut tg::process::Data, destination: &tg::Location) {
		if !destination.is_remote() {
			return;
		}
		let inherit = |tokens: &mut tg::authorization::Tokens| {
			let Some(entry) = tokens.get(destination).cloned() else {
				return;
			};
			let mut local = tokens.local_entry();
			local.inherit(&entry);
			tokens.set(tg::Location::Local(tg::location::Local::default()), local);
		};
		inherit(&mut data.command.options.tokens);
		if let tg::Either::Left(command) = &mut data.command.node {
			inherit(&mut command.executable.options.tokens);
			for value in command.args.iter_mut().chain(command.env.values_mut()) {
				let (tg::command::data::Value::String(value)
				| tg::command::data::Value::Value(value)) = value;
				tangram_server_core::process::update_process_value_tokens(
					value,
					&mut |tokens, _| {
						inherit(tokens);
					},
				);
			}
			if let Some(stdin) = &mut command.stdin {
				inherit(&mut stdin.options.tokens);
			}
		}
		if let Some(output) = &mut data.output {
			inherit(&mut output.options.tokens);
			tangram_server_core::process::update_process_value_tokens(
				&mut output.node,
				&mut |tokens, _| {
					inherit(tokens);
				},
			);
		}
		if let Some(error) = &mut data.error {
			inherit(&mut error.options.tokens);
		}
		if let Some(log) = &mut data.log {
			inherit(&mut log.options.tokens);
		}
	}

	#[tracing::instrument(name = "process.index", level = "debug", skip_all, fields(process = %arg.id), err(level = "debug"))]
	async fn index_finished_process_task(
		&self,
		session: &Session,
		arg: IndexFinishedProcessTaskArg,
	) -> tg::Result<()> {
		let IndexFinishedProcessTaskArg { data, id, location } = arg;
		let options = tangram_server_core::process::put::Options {
			command_sync: None,
			defer_index: false,
			location: Some(location.clone()),
			outcome_sync: None,
			store_data: location.is_remote(),
		};
		let arg = tg::process::put::Arg {
			data,
			location: None,
		};
		tangram_server_core::checkpoint!(
			session.server,
			"index.batch",
			command_object_permission = false,
			finished_process = true,
			runner = true
		)
		.await;
		let result = session
			.server
			.components
			.process
			.put_process_local_inner(
				session,
				&id,
				arg,
				tg::Principal::Process(id.clone()),
				options,
			)
			.await;
		if let Err(error) = &result {
			tracing::error!(error = %error.trace(), process = %id, "failed to index the finished process");
		}
		result?;
		tracing::debug!(process = %id, "indexed the finished process");

		Ok(())
	}

	async fn publish_finished_process(
		&self,
		session: &Session,
		id: &tg::process::Id,
		processes: &tangram_server_core::process::Processes,
		data: &mut tg::process::Data,
	) -> tg::Result<()> {
		let mut process_state = processes
			.get_mut(id)
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
		process_state.data.checksum = data.checksum.clone();
		process_state.data.cacheable = data.cacheable;
		process_state.data.error = data.error.clone();
		process_state.data.exit = data.exit;
		process_state.data.finished_at = data.finished_at;
		process_state.data.log = data.log.clone();
		process_state.data.output = data.output.clone();
		process_state.data.status = tg::process::Status::Finished;

		// List the children while holding the lock that finishes the process, so that every child that was added before the process finished is listed.
		data.children = process_state.data().children;
		process_state.changed.send_replace(());
		drop(process_state);
		tangram_server_core::checkpoint!(session.server, "runner.process.finished", process = %id)
			.await;

		Ok(())
	}

	async fn release_finished_process_children(
		&self,
		session: &Session,
		id: &tg::process::Id,
		processes: &tangram_server_core::process::Processes,
	) -> tg::Result<()> {
		let mut process_state = processes
			.get_mut(id)
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
		let child_leases = process_state
			.children
			.iter_mut()
			.filter_map(|(id, child)| {
				let lease = child.lease.take()?;
				let location = child.location.take();
				Some((id.clone(), lease, location))
			})
			.collect::<Vec<_>>();
		drop(process_state);

		child_leases
			.into_iter()
			.map(|(child, lease, location)| {
				let parent = id.clone();
				let session = session.clone();
				async move {
					tangram_server_core::checkpoint!(
						session.server,
						"runner.process.child_lease.release",
						child = %child,
						parent = %parent,
					)
					.await;
					let arg = tg::process::cancel::Arg {
						lease,
						location,
					};
					if let Err(error) = session.cancel_process(&tg::Either::Right(child.clone()), arg).await {
						tracing::error!(error = %error.trace(), process = %child, "failed to release a child process lease");
					}
				}
			})
			.collect::<futures::stream::FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		Ok(())
	}

	async fn write_process_log_task(
		&self,
		session: &Session,
		arg: WriteProcessLogTaskArg,
	) -> tg::Result<tg::Referent<tg::blob::Id>> {
		let WriteProcessLogTaskArg {
			id,
			location,
			mut receiver,
			started_at,
			streams,
		} = arg;
		let mut writer = tangram_server_core::log::Writer::new(session);
		let clock = session.server.clock.clone();

		// Stream the log chunks to the process's location as one stdio write.
		let capacity = session.server.config.runner.log_channel_capacity;
		let (sender, chunks) = tokio::sync::mpsc::channel(capacity);
		let write = async {
			let session = Self::try_get_process_session(session, &id)
				.ok_or_else(|| tg::error!(%id, "failed to get the process session"))?;
			let arg = tg::process::stdio::write::stream::Arg {
				id: None,
				location: Some(location.into()),
				streams,
				tokens: tg::authorization::Tokens::default(),
			};
			let chunks = tokio_stream::wrappers::ReceiverStream::new(chunks).boxed();
			session
				.write_process_stdio_all(&tg::Either::Right(id.clone()), arg, chunks)
				.await
		};
		let produce = async {
			let sender = sender;
			let result = async {
				let mut position = 0_u64;
				let mut stderr_position = 0_u64;
				let mut stdout_position = 0_u64;
				while let Some(event) = receiver.recv().await {
					let LogEvent::Chunk {
						bytes,
						permit,
						stream,
					} = event
					else {
						return Ok(());
					};
					let length = u64::try_from(bytes.len()).unwrap();
					let stream_position = match stream {
						tg::process::stdio::Stream::Stderr => &mut stderr_position,
						tg::process::stdio::Stream::Stdin => {
							return Err(tg::error!("invalid stdio stream"));
						},
						tg::process::stdio::Stream::Stdout => &mut stdout_position,
					};
					let timestamp = clock
						.unix_timestamp()
						.checked_sub(started_at)
						.ok_or_else(|| tg::error!("the log timestamp is too small"))?;
					let chunk = tg::process::stdio::Chunk {
						bytes,
						combined_position: position,
						stream,
						stream_position: *stream_position,
						timestamp: Some(timestamp),
					};
					position = position
						.checked_add(length)
						.ok_or_else(|| tg::error!("the log position is too large"))?;
					*stream_position = stream_position
						.checked_add(length)
						.ok_or_else(|| tg::error!("the stream log position is too large"))?;
					writer.write(&chunk).await?;
					if sender.send(Ok(chunk)).await.is_err() {
						return Err(tg::error!("the process log write ended"));
					}
					drop(permit);
				}
				Err(tg::error!("the process log stream ended unexpectedly"))
			}
			.await;
			if let Err(error) = &result {
				sender.send(Err(error.clone())).await.ok();
			}
			result
		};
		let (write_result, produce_result) = futures::future::join(write, produce).await;
		drop(receiver);

		// Commit the end only after every chunk has been written, reporting the write's error before the producer's.
		write_result?;
		produce_result?;
		tangram_server_core::checkpoint!(session.server, "runner.process.log.finish").await;
		let blob = writer.end().await?;

		Ok(blob)
	}

	#[tracing::instrument(name = "process.retain", level = "debug", skip_all, fields(process = %arg.id), err(level = "debug"))]
	async fn finish_process_task(&self, arg: FinishProcessTaskArg) -> tg::Result<()> {
		let FinishProcessTaskArg {
			buffered_task,
			control_task,
			id,
			log_task,
		} = arg;
		let log_result = if let Some(log_task) = log_task {
			match log_task.wait().await {
				Ok(result) => {
					result.map_err(|error| tg::error!(!error, "failed to read the process logs"))
				},
				Err(error) => Err(tg::error!(!error, "the log read task panicked")),
			}
		} else {
			Ok(())
		};
		let buffered_result = match buffered_task.wait().await {
			Ok(result) => result,
			Err(error) => Err(tg::error!(!error, %id, "the process buffered task panicked")),
		};

		let control_result = match control_task.wait().await {
			Ok(result) => result,
			Err(error) => Err(tg::error!(!error, %id, "the process control task panicked")),
		};
		log_result?;
		buffered_result?;
		control_result?;

		Ok::<_, tg::Error>(())
	}

	/// Get a process's token, if the runner has received it.
	pub(crate) fn try_get_process_inner_token(
		session: &Session,
		id: &tg::process::Id,
	) -> Option<String> {
		let state = session.server.components.runner.state();
		let sandbox_id = state.processes().get(id)?.value().clone();
		let sandbox = state.sandboxes().get_by_id(&sandbox_id)?;
		let process = sandbox.processes.get(id)?;
		process.inner_token.borrow().clone()
	}

	/// Determine whether a process's location is this server's region, whose processes this runner controls and stores directly.
	fn is_local_process_location(session: &Session, location: &tg::Location) -> bool {
		matches!(
			location,
			tg::Location::Local(tg::location::Local { region })
				if region.as_deref().is_none_or(|region| Some(region) == session.server.config.region.as_deref())
		)
	}

	/// Get a session that authenticates as the runner.
	fn runner_session(session: &Session) -> Session {
		let principal = session
			.server
			.components
			.runner
			.state()
			.id()
			.map_or(tg::Principal::Root, tg::Principal::Runner);
		let context = Context {
			principal,
			token: session.server.config.runner.token.clone(),
			..session.context.clone()
		};
		session.server.session(&context)
	}

	fn try_get_process_session(session: &Session, id: &tg::process::Id) -> Option<Session> {
		let state = session.server.components.runner.state();
		let sandbox_id = state.processes().get(id)?.value().clone();
		let sandbox = state.sandboxes().get_by_id(&sandbox_id)?;
		let origin = Origin::Sandbox(*sandbox.key());
		let process = sandbox.processes.get(id)?;
		let token = process.inner_token.borrow().clone();
		drop(process);
		drop(sandbox);
		let context = Context {
			origin,
			principal: tg::Principal::Process(id.clone()),
			token,
			..session.context.clone()
		};
		let session = session.server.session(&context);

		Some(session)
	}

	async fn spawn_index_process_task(
		&self,
		session: &Session,
		arg: IndexProcessTaskArg<'_>,
	) -> tg::Result<()> {
		let IndexProcessTaskArg {
			command,
			command_data,
			data,
			id,
			location,
			mut options,
			parent,
		} = arg;
		if !location.is_remote() {
			return Ok(());
		}
		tangram_server_core::checkpoint!(
			session.server,
			"runner.process.index.started",
			process = %id,
		)
		.await;

		// Resolve the command before a finish can register its tracked write.
		command_data.await?;

		let data = data.without_location_and_tokens_except_sync();
		options.clear_location_and_tokens();
		let command_id = data.command.command_id()?;
		let sandbox = data
			.sandbox
			.as_ref()
			.ok_or_else(|| tg::error!(%id, "the running process has no sandbox"))?;
		let now = session.server.clock.unix_timestamp();
		let put_process_arg = tangram_index::process::put::Arg {
			cached: false,
			children: None,
			command: Some(
				data.command
					.objects()
					.into_iter()
					.map(|object| object.node)
					.collect(),
			),
			command_id: command_id.into(),
			data: Some(data.clone()),
			error: None,
			id: id.clone(),
			location: Some(location.clone()),
			log: None,
			metadata: tg::process::Metadata::default(),
			options,
			output: None,
			parent: parent.cloned(),
			permissions: Vec::new(),
			principal: tg::Principal::Process(id.clone()),
			sandbox: Some(sandbox.clone()),
			storage: tg::process::storage::Set::NODE,
			time_to_touch: session.server.config.process.time_to_touch,
			touched_at: now,
		};
		let mut items = vec![tangram_index::batch::Item::PutProcess(put_process_arg)];
		let permission_arg = session
			.server
			.components
			.process
			.create_process_sandbox_permission_arg(session, id, sandbox, now)?;
		items.push(tangram_index::batch::Item::PutPermission(permission_arg));
		let destination = id.clone().into();
		// Loading an inline command does not prove permissions on the objects it references.
		let permissions = match &command.node {
			tg::Either::Left(_) => tg::authorization::permission::object::Set::empty(),
			tg::Either::Right(_) => tg::authorization::permission::object::Set::NODE,
		};
		let permissions = tg::authorization::permission::Set::Object(permissions);
		let roots = command
			.objects()
			.into_iter()
			.map(|resource| (resource.map(Into::into), permissions));
		let source = tg::Principal::Process(parent.unwrap_or(id).clone());
		items.extend(
			session.create_capture_permissions_batch_items_with_permissions(
				destination,
				None,
				roots,
				source,
				now,
			)?,
		);

		// Apply the initial data before the finished data can be written.
		let arg = tangram_index::batch::Arg { items };
		session
			.server
			.index
			.batch(arg)
			.await
			.and_then(std::convert::identity)
			.map_err(|error| tg::error!(!error, "failed to index the remote process"))?;

		Ok(())
	}

	#[tracing::instrument(name = "process.run", level = "debug", skip_all, fields(command = tracing::field::Empty, process = %arg.id, sandbox = arg.state.sandbox.as_ref().map(ToString::to_string)), err(level = "debug"))]
	async fn run_process(
		&self,
		session: &Session,
		arg: RunProcessArg,
	) -> tg::Result<RunProcessOutcome> {
		let RunProcessArg {
			command,
			guest_url,
			id,
			process_stopper,
			processes,
			progress_sender,
			sandbox,
			sandbox_process_sender,
			state,
			stopper,
			token,
		} = arg;
		let command = &command;
		let state = &state;
		let command_id = state.command.command_id()?;
		tracing::Span::current().record("command", tracing::field::display(&command_id));
		tracing::debug!(process = %id, sandbox = state.sandbox.as_ref().map(ToString::to_string), "checking out the process artifacts");

		// Run the process.
		let result = async {
			// Validate the host.
			let host = command.host.as_str();
			match host {
				#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
				"aarch64-darwin" => (),

				#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
				"x86_64-darwin" => (),

				#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
				"aarch64-linux" => (),

				#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
				"x86_64-linux" => (),

				_ => {
					return Err(tg::error!(%host, "cannot run process with host"));
				},
			}

			// Check out the process's children.
			self.checkout_process_artifacts(session,
				command,
				state
					.sandbox
					.as_ref()
					.ok_or_else(|| tg::error!("the running process has no sandbox"))?,
				progress_sender.clone(),
				&state.stderr,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to check out the children"))?;

			tracing::debug!(process = %id, sandbox = state.sandbox.as_ref().map(ToString::to_string), "checked out the process artifacts");
			let sandbox_process = sandbox.create_process();
			let guest_store_path = sandbox.guest_store_path();
			let guest_output_path = sandbox.guest_output_path_for_process(&sandbox_process);
			let host_output_path = sandbox.host_output_path_for_process(&sandbox_process);

			// Render the args.
			let args = render_args(&command.args, &guest_store_path, &guest_output_path)?;

			// Get the working directory. On macOS there is no chroot, so "/" is the host root and not writable. Default to the scratch directory instead.
			let cwd = if let Some(cwd) = &command.cwd {
				cwd.clone()
			} else if cfg!(target_os = "macos") {
				sandbox.host_scratch_path()
			} else {
				"/".into()
			};

			// Render the env.
			let mut env = render_env(&command.env, &guest_store_path, &guest_output_path)?;
			let engine = match session.server.config.runner.javascript.engine {
				tangram_server_core::config::JavaScriptEngine::Auto => "auto",
				tangram_server_core::config::JavaScriptEngine::QuickJs => "quickjs",
				tangram_server_core::config::JavaScriptEngine::V8 => "v8",
			};
			env.insert("TANGRAM_JAVASCRIPT_ENGINE".to_owned(), engine.to_owned());
			for key in [
				"TANGRAM_JAVASCRIPT_DEBUG",
				"TANGRAM_JAVASCRIPT_DEBUG_ADDR",
				"TANGRAM_JAVASCRIPT_DEBUG_MODE",
			] {
				env.remove(key);
			}
			if let Some(debug) = state.debug.as_ref() {
				env.insert("TANGRAM_JAVASCRIPT_DEBUG".to_owned(), "true".to_owned());
				if let Some(addr) = debug.addr {
					env.insert("TANGRAM_JAVASCRIPT_DEBUG_ADDR".to_owned(), addr.to_string());
				}
				if debug.mode != tg::process::debug::Mode::Normal {
					env.insert(
						"TANGRAM_JAVASCRIPT_DEBUG_MODE".to_owned(),
						debug.mode.to_string(),
					);
				}
			}

			#[cfg(target_os = "macos")]
			env.entry("TMPDIR".to_owned())
				.or_insert_with(|| sandbox.host_scratch_path().to_string_lossy().into_owned());

			// Render the executable.
			let executable = if let Some(artifact) = &command.executable.node.artifact {
				let mut path = guest_store_path.join(artifact.to_string());
				if let Some(executable_path) = &command.executable.node.path {
					path.push(executable_path);
				}
				path
			} else if let Some(path) = &command.executable.node.path {
				path.clone()
			} else {
				return Err(tg::error!("invalid executable"));
			};
			let stdin = match state.stdin {
				tg::process::Stdio::Null => tangram_sandbox::Stdio::Null,
				tg::process::Stdio::Pipe => tangram_sandbox::Stdio::Pipe,
				tg::process::Stdio::Tty => tangram_sandbox::Stdio::Tty,
				_ => {
					return Err(tg::error!("invalid stdin"));
				},
			};
			let stdout = match state.stdout {
				tg::process::Stdio::Log | tg::process::Stdio::Pipe => tangram_sandbox::Stdio::Pipe,
				tg::process::Stdio::Null => tangram_sandbox::Stdio::Null,
				tg::process::Stdio::Tty => tangram_sandbox::Stdio::Tty,
				_ => {
					return Err(tg::error!("invalid stdout"));
				},
			};
			let stderr = match state.stderr {
				tg::process::Stdio::Log | tg::process::Stdio::Pipe => tangram_sandbox::Stdio::Pipe,
				tg::process::Stdio::Null => tangram_sandbox::Stdio::Null,
				tg::process::Stdio::Tty => tangram_sandbox::Stdio::Tty,
				_ => {
					return Err(tg::error!("invalid stderr"));
				},
			};

			// Spawn.
			let sandbox_command = tangram_sandbox::Command {
				args,
				cwd,
				env,
				executable,
				stderr,
				stdin,
				stdout,
			};
			tangram_server_core::checkpoint!(
				session.server,
				"runner.process.start",
				command = %command_id,
				process = %id,
			)
			.await;
			let entry = tangram_sandbox::SpawnArg {
				command: sandbox_command,
				token: token.clone(),
				tty: state.tty,
				url: guest_url.clone(),
			};
			sandbox
				.spawn(&sandbox_process, entry)
				.await
				.map_err(|error| {
					tg::error!(!error, "failed to spawn the process in the sandbox")
				})?;
			tracing::info!(command = %command_id, process = %id, sandbox = state.sandbox.as_ref().map(ToString::to_string), "started the process");
			let sandbox_process = Arc::new(sandbox_process);

			// Provide the sandbox process to the control task.
			sandbox_process_sender.send_replace(Some(sandbox_process.clone()));
			processes
				.get_mut(&id)
				.expect("the process state was not found")
				.process = Some(sandbox_process.as_ref().clone());

			let arg = WaitForProcessArg {
				process_stopper: process_stopper.clone(),
				sandbox: &sandbox,
				sandbox_process: sandbox_process.as_ref(),
				stopper,
			};
			let stdin = async {
				let result = self
					.write_process_stdin_blob(session, command.stdin.as_ref(), &sandbox, &sandbox_process)
					.await;
				if result.is_err() {
					process_stopper.stop();
				}
				result
			};
			let (exit, stdin) = future::join(self.wait_for_process(arg).boxed(), stdin).await;
			stdin?;
			let exit = exit?;
			tracing::info!(exit, process = %id, sandbox = state.sandbox.as_ref().map(ToString::to_string), "exited the process");
			tangram_server_core::checkpoint!(
				session.server,
				"runner.process.exit",
				command = %command_id,
			)
			.await;

			let outcome = RunProcessOutcome {
				exit,
				path: host_output_path,
			};

			Ok(outcome)
		}
		.boxed()
		.await;

		// Drop the sender so that the i/o tasks observe that the sandbox process will never be spawned if it has not been.
		drop(sandbox_process_sender);

		result
	}

	async fn write_process_stdin_blob(
		&self,
		session: &Session,
		blob: Option<&tg::Referent<tg::blob::Id>>,
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
	) -> tg::Result<()> {
		let Some(blob) = blob else {
			return Ok(());
		};
		let blob = tg::Blob::with_referent(blob.clone());
		let reader = blob
			.read_with_instance(session, tg::read::Options::default())
			.await
			.map_err(|error| tg::error!(!error, "failed to read process stdin blob"))?;
		let stream = tokio_util::io::ReaderStream::new(reader)
			.map_ok(|bytes| {
				tangram_sandbox::stdio::read::Event::Chunk(tangram_sandbox::stdio::Chunk {
					bytes,
					stream: tg::process::stdio::Stream::Stdin,
				})
			})
			.map_err(|error| tg::error!(!error, "failed to read from the blob"))
			.chain(futures::stream::once(future::ok(
				tangram_sandbox::stdio::read::Event::End,
			)))
			.boxed();
		let output = sandbox
			.write_stdio(
				sandbox_process,
				vec![tg::process::stdio::Stream::Stdin],
				stream,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to write stdin"))?;
		let mut output = std::pin::pin!(output);
		while let Some(event) = output.try_next().await? {
			if matches!(event, tangram_sandbox::stdio::write::Event::End) {
				break;
			}
		}

		Ok(())
	}

	#[tracing::instrument(name = "process.wait", level = "debug", skip_all, fields(sandbox_index = arg.sandbox.index()), err(level = "debug"))]
	async fn wait_for_process(&self, arg: WaitForProcessArg<'_>) -> tg::Result<u8> {
		let WaitForProcessArg {
			process_stopper,
			sandbox,
			sandbox_process,
			stopper,
		} = arg;
		let wait_future = sandbox
			.wait(sandbox_process)
			.await
			.map_err(|error| tg::error!(!error, "failed to start waiting for the process"))?;
		let mut wait_future = std::pin::pin!(wait_future);
		let (exit, stopped) = tokio::select! {
				result = &mut wait_future => {
					let exit = result.map_err(
						|error| tg::error!(!error, "failed to wait for the process"),
					)?;
					(exit, false)
				},
				() = stopper.wait() => {
					sandbox.kill(sandbox_process, tg::process::Signal::SIGKILL).await.ok();
					let exit = wait_future.await.map_err(
						|error| tg::error!(!error, "failed to wait for the process"),
					)?;
					(exit, true)
				},
				() = process_stopper.wait() => {
					sandbox.kill(sandbox_process, tg::process::Signal::SIGKILL).await.ok();
					let exit = wait_future.await.map_err(
						|error| tg::error!(!error, "failed to wait for the process"),
					)?;
					(exit, true)
				},
		};
		if stopped {
			return Err(tg::error!(
				code = tg::error::Code::Cancellation,
				"the process was canceled"
			));
		}

		Ok(exit)
	}

	#[tracing::instrument(name = "process.collect", level = "debug", skip_all, fields(exit = arg.exit, sandbox = arg.state.sandbox.as_ref().map(ToString::to_string)), err(level = "debug"))]
	async fn collect_process_outcome(
		&self,
		session: &Session,
		arg: CollectProcessOutcomeArg<'_>,
	) -> tg::Result<Outcome> {
		let CollectProcessOutcomeArg { exit, path, state } = arg;
		let mut outcome = Outcome {
			checksum: None,
			error: None,
			exit,
			output: None,
		};
		let exists = tokio::fs::try_exists(&path)
			.await
			.map_err(|error| tg::error!(!error, "failed to determine if the output path exists"))?;

		// Read the combined outcome before the separate output and error attributes.
		let outcome_bytes = if exists {
			tg::file::xattrs::read_outcome(&path)?
		} else {
			None
		};
		let has_outcome = outcome_bytes.is_some();
		if let Some(bytes) = outcome_bytes {
			let data = serde_json::from_slice::<tg::process::outcome::Data>(&bytes)
				.map_err(|error| tg::error!(!error, "failed to parse the outcome xattr"))?;
			let value = tg::process::Outcome::try_from(data)?;
			outcome.checksum = value.checksum;
			outcome.error = value.error;
			outcome.output = value.output;
		} else if exists {
			// Try to read the user.tangram.output xattr.
			if let Some(bytes) = tg::file::xattrs::read_output(&path)? {
				let tgon = String::from_utf8(bytes)
					.map_err(|error| tg::error!(!error, "failed to decode the output xattr"))?;
				outcome.output = Some(
					tgon.parse::<tg::Value>()
						.map_err(|error| tg::error!(!error, "failed to parse the output xattr"))?,
				);
			}

			// Try to read the user.tangram.error xattr.
			if let Some(bytes) = tg::file::xattrs::read_error(&path)? {
				let error = if let Ok(data) = serde_json::from_slice::<tg::error::Data>(&bytes) {
					tg::Error::try_from(data)
						.map_err(|error| tg::error!(!error, "failed to convert the error data"))?
				} else {
					let string = String::from_utf8(bytes)
						.map_err(|error| tg::error!(!error, "failed to decode the error xattr"))?;
					let referent = string
						.parse()
						.map_err(|error| tg::error!(!error, "failed to parse the error xattr"))?;
					tg::Error::with_referent(referent)
				};
				outcome.error = Some(error);
			}
		}

		// Try to read the user.tangram.checksum xattr.
		if outcome.checksum.is_none()
			&& let Ok(Some(bytes)) = tg::file::xattrs::read_checksum(&path)
		{
			let checksum = String::from_utf8(bytes)
				.map_err(|error| tg::error!(!error, "failed to parse the checksum xattr"))
				.and_then(|string| string.parse::<tg::Checksum>())
				.map_err(|error| tg::error!(!error, "failed to parse the checksum string"))?;
			outcome.checksum = Some(checksum);
		}

		// Check in the output.
		if !has_outcome && outcome.error.is_none() && outcome.output.is_none() && exists {
			let path = session.guest_path_for_host_path(&path)?;
			let arg = tg::checkin::Arg {
				options: tg::checkin::Options {
					destructive: true,
					deterministic: true,
					ignore: false,
					lock: None,
					locked: true,
					root: true,
					..Default::default()
				},
				path,
				updates: Vec::new(),
			};
			let checkin_output = session
				.checkin(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to check in the output"))?
				.try_last()
				.await?
				.and_then(|event| event.try_unwrap_output().ok())
				.ok_or_else(|| tg::error!("stream ended without output"))?;
			let artifact = tg::Artifact::with_referent(checkin_output.artifact);
			let value = artifact.into();
			outcome.output = Some(value);
		}

		// Compute the checksum if necessary.
		if let (Some(checksum), None, Some(value)) =
			(&state.checksum.expected, &outcome.checksum, &outcome.output)
		{
			let algorithm = checksum.algorithm();
			let checksum = self
				.compute_checksum(session, value, algorithm)
				.await
				.map_err(|error| tg::error!(!error, "failed to compute the checksum"))?;
			outcome.checksum = Some(checksum);
		}

		Ok(outcome)
	}

	async fn compute_checksum(
		&self,
		session: &Session,
		value: &tg::Value,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		if let Ok(blob) = value.clone().try_into() {
			session
				.server
				.components
				.checksum
				.checksum_blob(session, &blob, algorithm)
				.await
		} else if let Ok(artifact) = value.clone().try_into() {
			session
				.server
				.components
				.checksum
				.checksum_artifact(session, &artifact, algorithm)
				.await
		} else {
			Err(tg::error!(
				"cannot checksum a value that is not a blob or an artifact"
			))
		}
	}

	#[tracing::instrument(name = "process.checkout", level = "debug", skip_all, fields(sandbox = %sandbox), err(level = "debug"))]
	async fn checkout_process_artifacts(
		&self,
		session: &Session,
		command: &tg::process::data::Command,
		sandbox: &tg::sandbox::Id,
		progress: tokio::sync::mpsc::UnboundedSender<Bytes>,
		stderr: &tg::process::Stdio,
	) -> tg::Result<()> {
		// Get the process's command's children that are artifacts.
		let artifacts = command
			.objects()
			.into_iter()
			.filter_map(|object| {
				let id = object.node.try_into().ok()?;
				let artifact = tg::Referent::new(id, object.options);
				Some(artifact)
			})
			.collect::<Vec<tg::Referent<tg::artifact::Id>>>();

		// Seed the VFS with the tokens already carried by the command's artifact inputs.
		let vfs = session
			.server
			.components
			.runner
			.state()
			.sandboxes
			.get_by_id(sandbox)
			.and_then(|state| state.vfs.clone());
		if let Some(vfs) = vfs {
			for artifact in &artifacts {
				vfs.seed_tokens(session, &artifact.options.tokens);
			}
		}

		// Track each artifact's verified subtree token for store path checkin and the VFS.
		let permissions =
			tg::authorization::permission::Set::from(tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			));
		let tokens = artifacts.iter().flat_map(|artifact| {
			artifact
				.options
				.tokens
				.local_authorization()
				.iter()
				.filter_map(move |token| {
					let resource =
						tg::Selector::Id(tg::object::Id::from(artifact.node.clone()).into());
					session
						.server
						.components
						.authorization
						.authorize_token(session, &resource, permissions, token)
						.then(|| (artifact.node.clone(), token.clone()))
				})
		});
		if let Some(mut state) = session
			.server
			.components
			.runner
			.state()
			.sandboxes
			.get_mut_by_id(sandbox)
		{
			state.tokens.extend(tokens);
		}
		if session.server.components.vfs.mounted() {
			return Ok(());
		}

		// Check out the artifacts.
		let stream = session
			.server
			.components
			.checkout
			.checkout_internal(session, artifacts)
			.await
			.map_err(|error| tg::error!(!error, "failed to check out the artifacts"))?;

		// Write progress.
		self.write_progress_stream(session, progress, stderr, stream)
			.await
			.map_err(|error| tg::error!(!error, "failed to log the progress stream"))?;

		Ok(())
	}
}

fn render_args(
	args: &[tg::command::data::Value],
	store_path: &Path,
	output_path: &Path,
) -> tg::Result<Vec<String>> {
	args.iter()
		.map(|arg| match arg {
			tg::command::data::Value::String(value) => {
				render_value_string(value, store_path, output_path)
			},
			tg::command::data::Value::Value(value) => {
				let value = tg::Value::try_from_data(value.clone())?;
				Ok(render_value(&value))
			},
		})
		.collect::<tg::Result<Vec<_>>>()
}

fn render_value(value: &tg::Value) -> String {
	let options = tg::value::print::Options {
		tokens: true,
		..Default::default()
	};
	value.print(options)
}

fn render_env(
	env: &BTreeMap<String, tg::command::data::Value>,
	store_path: &Path,
	output_path: &Path,
) -> tg::Result<BTreeMap<String, String>> {
	for key in env.keys() {
		if key.starts_with(tg::process::env::PREFIX) {
			return Err(tg::error!(
				key = %key,
				"env vars prefixed with TANGRAM_ENV_ are reserved"
			));
		}
	}
	let mut output = env
		.iter()
		.map(|(key, value)| {
			let key = key.clone();
			let value = match value {
				tg::command::data::Value::String(value) => {
					render_value_string(value, store_path, output_path)?
				},
				tg::command::data::Value::Value(value) => {
					let value = tg::Value::try_from_data(value.clone())?;
					render_value(&value)
				},
			};
			Ok::<_, tg::Error>((key, value))
		})
		.collect::<tg::Result<BTreeMap<_, _>>>()?;
	for (key, value) in env {
		let value = match value {
			tg::command::data::Value::String(tg::value::Data::String(_)) => continue,
			tg::command::data::Value::String(value) | tg::command::data::Value::Value(value) => {
				value
			},
		};
		let value = tg::Value::try_from_data(value.clone())?;
		let value = render_value(&value);
		output.insert(format!("{}{key}", tg::process::env::PREFIX), value);
	}
	Ok(output)
}

fn render_value_string(
	value: &tg::value::Data,
	store_path: &Path,
	output_path: &Path,
) -> tg::Result<String> {
	match value {
		tg::value::Data::String(string) => Ok(string.clone()),
		tg::value::Data::Object(object) if object.node.is_artifact() => {
			let artifact: tg::artifact::Id = object.node.clone().try_into().unwrap();
			Ok(store_path
				.join(artifact.to_string())
				.to_string_lossy()
				.into_owned())
		},
		tg::value::Data::Template(template) => template.try_render(|component| match component {
			tg::template::data::Component::String(string) => Ok(string.clone().into()),
			tg::template::data::Component::Artifact(artifact) => Ok(store_path
				.join(artifact.node.to_string())
				.to_str()
				.unwrap()
				.to_owned()
				.into()),
			tg::template::data::Component::Placeholder(placeholder) => {
				if placeholder.name == "output" {
					Ok(output_path.to_str().unwrap().to_owned().into())
				} else {
					Err(tg::error!(
						name = %placeholder.name,
						"invalid placeholder"
					))
				}
			},
		}),
		tg::value::Data::Placeholder(placeholder) => {
			if placeholder.name == "output" {
				Ok(output_path.to_str().unwrap().to_owned())
			} else {
				Err(tg::error!(
					name = %placeholder.name,
					"invalid placeholder"
				))
			}
		},
		_ => Ok(tg::Value::try_from_data(value.clone()).unwrap().to_string()),
	}
}
