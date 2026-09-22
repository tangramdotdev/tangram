use {
	self::control::RunProcessControlTaskArg,
	crate::{Context, Origin, Session},
	bytes::Bytes,
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::{self, BoxFuture, Shared},
		stream::{BoxStream, FuturesOrdered},
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
	tangram_index::Index as _,
	tangram_messenger::Messenger as _,
	tokio::task::JoinSet,
	tokio_stream::wrappers::UnboundedReceiverStream,
};

mod control;
mod progress;

#[cfg(test)]
mod tests;

type CommandFuture = Shared<BoxFuture<'static, tg::Result<tg::process::data::Command>>>;
type ControlConnection = (
	tg::process::control::Output,
	BoxStream<'static, tg::Result<tg::control::Event<tg::process::control::ServerMessage>>>,
);

pub(super) struct ProcessControlConnection {
	control: crate::control::Stream<
		tg::process::control::ServerMessage,
		tg::process::control::ClientMessage,
	>,
	input: tokio::sync::mpsc::Sender<tg::process::control::ClientMessage>,
	output: tg::process::control::Output,
}

const LOG_BUFFER_SIZE: usize = 16 * 1024 * 1024;
const LOG_CHANNEL_CAPACITY: usize = 256;
const LOG_CHUNK_SIZE: usize = tg::process::stdio::flow::CHUNK_SIZE;
const LOG_REQUEST_CONCURRENCY: usize = tg::process::stdio::flow::MAX_CHUNKS;

pub(super) struct SpawnProcessTaskArg<'a> {
	pub guest_url: &'a tangram_uri::Uri,
	pub location: tg::Location,
	pub process: tg::runner::control::Process,
	pub process_stopper: &'a Stopper,
	pub process_tasks: &'a mut JoinSet<tg::Result<()>>,
	pub processes: Arc<crate::process::Processes>,
	pub retention_stopper: Stopper,
	pub sandbox: &'a tangram_sandbox::Sandbox,
	pub sandbox_initialization: Option<tg::process::control::Sandbox>,
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
	processes: Arc<crate::process::Processes>,
	retention_stopper: Stopper,
	sandbox: tangram_sandbox::Sandbox,
	sandbox_initialization: Option<tg::process::control::Sandbox>,
	sandbox_ready_receiver: Option<tokio::sync::oneshot::Receiver<()>>,
	sandbox_stopper: Stopper,
}

struct FinishProcessRunArg {
	control: control::ProcessControlSender,
	exited: Stopper,
	finish_sender: tokio::sync::oneshot::Sender<control::ProcessControlResponseReceiver>,
	id: tg::process::Id,
	index_receiver: tokio::sync::oneshot::Receiver<()>,
	initialization: Option<tokio::sync::oneshot::Receiver<()>>,
	location: tg::Location,
	processes: Arc<crate::process::Processes>,
	ready_receiver: tokio::sync::oneshot::Receiver<()>,
	run_task: Task<tg::Result<RunProcessOutput>>,
	sandbox: tangram_sandbox::Sandbox,
	state: tg::process::State,
}

struct FinishProcessRunOutput {
	data: tg::process::Data,
	index_task: Option<crate::process::IndexTask>,
}

struct IndexFinishedProcessTaskArg {
	authorization: crate::process::put::Authorization,
	data: tg::process::Data,
	id: tg::process::Id,
	location: tg::Location,
}

struct RecordFinishedProcessArg<'a> {
	data: &'a tg::process::Data,
	id: &'a tg::process::Id,
	location: &'a tg::Location,
	processes: Arc<crate::process::Processes>,
}

struct FinishProcessTaskArg {
	buffered_task: Task<tg::Result<()>>,
	control_task: Task<tg::Result<()>>,
	data: tg::process::Data,
	id: tg::process::Id,
	log_task: Option<Task<tg::Result<()>>>,
	process: tg::Process,
	processes: Arc<crate::process::Processes>,
	push: tokio::sync::oneshot::Sender<()>,
}

struct IndexProcessTaskArg<'a> {
	command: tg::Referent<tg::Either<Box<tg::process::data::Command>, tg::command::Id>>,
	command_data: CommandFuture,
	command_roots: Vec<tangram_index::process::object::grant::Root>,
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

struct CollectProcessOutputArg<'a> {
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
pub(crate) struct ConnectedEvent {
	pub lease: String,
	pub process: tg::Referent<tg::process::Id>,
}

#[derive(Clone, Debug)]
struct Output {
	checksum: Option<tg::Checksum>,
	error: Option<tg::Error>,
	exit: u8,
	value: Option<tg::Value>,
}

struct RunProcessArg {
	command: tg::process::data::Command,
	guest_url: tangram_uri::Uri,
	id: tg::process::Id,
	process_stopper: Stopper,
	processes: Arc<crate::process::Processes>,
	progress_sender: tokio::sync::mpsc::UnboundedSender<Bytes>,
	sandbox: tangram_sandbox::Sandbox,
	sandbox_process_sender: tokio::sync::watch::Sender<Option<Arc<tangram_sandbox::Process>>>,
	state: tg::process::State,
	stopper: Stopper,
	token: String,
}

pub(super) struct WriteProcessLogTaskArg {
	receiver: tokio::sync::mpsc::Receiver<LogEvent>,
	started_at: i64,
}

struct RunProcessOutput {
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
					let end = (offset + LOG_CHUNK_SIZE).min(chunk.bytes.len());
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

impl Session {
	#[must_use]
	pub(super) fn create_process_lease() -> String {
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
	}

	pub(super) fn spawn_process_task(
		&self,
		arg: SpawnProcessTaskArg<'_>,
	) -> SpawnProcessTaskOutput {
		let (event_sender, event_receiver) = tokio::sync::mpsc::unbounded_channel();
		let session = self.clone();
		let process = arg.process;
		let processes = arg.processes;
		let sandbox = arg.sandbox.clone();
		let sandbox_initialization = arg.sandbox_initialization;
		let guest_url = arg.guest_url.clone();
		let location = arg.location;
		let sandbox_ready_receiver = arg.sandbox_ready_receiver;
		let sandbox_stopper = arg.process_stopper.clone();
		let retention_stopper = arg.retention_stopper;
		arg.process_tasks.spawn(async move {
			let arg = ProcessTaskArg {
				event_sender: event_sender.clone(),
				guest_url,
				location,
				process,
				processes,
				retention_stopper,
				sandbox,
				sandbox_initialization,
				sandbox_ready_receiver,
				sandbox_stopper,
			};
			let result = if session.server.shutdown.borrow().is_some() {
				Err(tg::error!("the server is shutting down"))
			} else {
				session.process_task(arg).boxed().await
			};
			if let Err(error) = &result {
				event_sender.send(Err(error.clone())).ok();
			}
			result
		});
		SpawnProcessTaskOutput {
			events: event_receiver,
		}
	}

	async fn process_task(&self, arg: ProcessTaskArg) -> tg::Result<()> {
		let ProcessTaskArg {
			event_sender,
			guest_url,
			location,
			process,
			processes,
			retention_stopper,
			sandbox,
			sandbox_initialization,
			mut sandbox_ready_receiver,
			sandbox_stopper,
		} = arg;
		let tg::runner::control::Process {
			data,
			id,
			options,
			parent,
			token: inner_token,
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
			.any(|token| self.verify_local_token(token));
		if local {
			command_options.location = Some(tg::Location::Local(tg::location::Local::default()));
		} else {
			self.update_tokens_and_location(
				&mut command_options.tokens,
				Some(&mut command_options.location),
				&location,
				false,
			)?;
		}
		state.command.options = command_options;
		let process_stopper = Stopper::new();
		let lease = Self::create_process_lease();
		let (mut control_sender_high, control_responses_high) = tokio::sync::mpsc::channel(512);
		let (control_sender_low, control_responses_low) = tokio::sync::mpsc::channel(512);
		let mut control_responses = Some(
			crate::control::priority_stream(control_responses_high, control_responses_low)
				.map(Ok)
				.boxed(),
		);
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
		let mut control = crate::control::Stream::new_reconnecting_with_priorities(
			requests,
			control_sender_high.clone(),
			control_sender_low,
			crate::control::stream_options(),
		);

		// Obtain the shortcut process identity before starting execution.
		let mut initialization = None;
		let (id, inner_token, command_session, connection_output) = match (id, inner_token) {
			(Some(id), Some(token)) => (id, token, None, None),
			(None, None) => {
				let parent = parent.as_ref().ok_or_else(|| {
					tg::error!("a process on the shortcut path must have a parent")
				})?;
				let mut command_session = self.try_get_process_session(parent).ok_or_else(
					|| tg::error!(%parent, "failed to find the parent process session"),
				)?;
				command_session.context.stopper = None;
				if let Some(receiver) = sandbox_ready_receiver.take() {
					receiver.await.map_err(|error| {
						tg::error!(!error, "the sandbox failed before becoming ready")
					})?;
				}
				crate::checkpoint!(self.server, "runner.process.control.acquire").await;
				let connection = self
					.server
					.runner
					.process_control_connection_pool()
					.take()
					.await?;
				let id = connection.output.process.node.clone();
				let token =
					connection.output.token.clone().ok_or_else(
						|| tg::error!(%id, "missing the process authentication token"),
					)?;
				let start = tg::process::control::StartClientRequestArg {
					data: state.to_data(),
					lease: lease.clone(),
					options: options.clone(),
					parent: parent.clone(),
					sandbox: sandbox_initialization,
				};
				let (sender, receiver) = tokio::sync::oneshot::channel();
				initialization = Some(receiver);
				let control_sender = connection.control.sender();
				let command = state.command.clone();
				let location = location.clone();
				let push_session = command_session.clone();
				let process_stopper = process_stopper.clone();
				let server = self.server.clone();
				let process_id = id.clone();
				let mut start_task = Task::spawn(move |_| async move {
					let result = async {
						Self::push_process_command(&push_session, &command, &location).await?;
						let response = Self::send_process_control_client_request_inner(
							&control_sender,
							tg::process::control::ClientRequestArg::Start(start),
							crate::control::Priority::High,
						)
						.await?;
						sender.send(()).ok();
						crate::checkpoint!(server, "runner.process.control.start.sent", process = %process_id).await;
						Self::receive_process_control_client_response(response)
							.await
							.and_then(|output| {
								output.try_unwrap_start().map_err(|_| {
									tg::error!("expected a process control start response")
								})
							})
					}
					.boxed()
					.await;
					if result.is_ok() {
						crate::checkpoint!(server, "runner.process.control.start.succeeded", process = %process_id).await;
					}
					if let Err(error) = result {
						tracing::error!(error = %error.trace(), "failed to start the process control connection");
						process_stopper.stop();
					}
				});
				start_task.detach();
				control_sender_high = connection.input;
				control = connection.control;
				(id, token, Some(command_session), Some(connection.output))
			},
			_ => {
				return Err(tg::error!(
					"the process id and token must be provided together"
				));
			},
		};
		let context = crate::Context {
			principal: tg::Principal::Process(id.clone()),
			token: Some(inner_token.clone()),
			..self.context.clone()
		};
		let session = self.server.session(&context);
		let command_session = command_session.unwrap_or_else(|| session.clone());
		let sandbox_index = sandbox.index();

		// Store the process state before starting the process.
		let mut data = state.to_data();
		let mut children = indexmap::IndexMap::default();
		for child in data.children.take().unwrap_or_default() {
			let id = child.process.node.clone();
			let child = crate::process::Child {
				data: child,
				lease: None,
				location: None,
			};
			if children.insert(id, child).is_some() {
				return Err(tg::error!("the process children must be unique"));
			}
		}
		let (index_result_sender, index_result_receiver) = tokio::sync::oneshot::channel();
		let index_task = crate::process::IndexTask::spawn(move |_| async move {
			index_result_receiver
				.await
				.map_err(|error| tg::error!(!error, "failed to receive the process index result"))?
		});
		let sandbox_id = state.sandbox.clone();
		let sync = connection_output
			.as_ref()
			.and_then(|output| output.sync.clone().filter(|_| location.is_remote()));
		let (control_sender, control_receiver) = crate::process::control::local::Local::new();
		let entry = crate::process::State {
			changed: tokio::sync::watch::channel(()).0,
			children,
			control: control_sender_high.clone(),
			control_sender,
			data,
			finish: None,
			index_task: index_task.clone(),
			inner_token: inner_token.clone(),
			leases: BTreeSet::from([lease.clone()]),
			process: None,
			stopper: process_stopper.clone(),
			sync,
		};
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
			.runner
			.state
			.processes
			.insert(id.clone(), sandbox_id.clone());
		let processes_for_cleanup = processes.clone();
		let id_for_cleanup = id.clone();
		let server = session.server.clone();
		scopeguard::defer! {
			processes_for_cleanup.remove(&id_for_cleanup);
			server.runner.state.processes.remove(&id_for_cleanup);
		}
		crate::checkpoint!(
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
			match self
				.server
				.runner
				.state
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
		let server_for_token_cleanup = self.server.clone();
		let token_for_cleanup = token.clone();
		scopeguard::defer! {
			server_for_token_cleanup
				.runner
				.state
				.process_for_token
				.remove(&token_for_cleanup);
		}

		// Load the command concurrently with the control stream.
		let command: CommandFuture = {
			// Ignore the source-relative location when loading the command on the runner.
			let mut command = state.command.clone();
			command.options.location = None;
			let command_session = command_session.clone();
			let session = session.clone();
			let server = self.server.clone();
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
					.try_get_object_local(&command_id, false)
					.await
					.map_err(|error| tg::error!(!error, "failed to get the local command"))?
					.is_some();

				// Preserve the parent's local command authorization on the shortcut path.
				let session = if local { command_session } else { session };

				// Load the command.
				let data = if local {
					session.try_load_process_command_local(&command).await?
				} else {
					None
				};
				let data = match data {
					Some(data) => data,
					None => command
						.data_with_handle(&session)
						.await
						.map_err(|error| tg::error!(!error, "failed to get the command data"))?,
				};

				let options = command.to_referent().options;
				let command = tg::process::data::Command::with_command_data(data, &options);
				Ok(command)
			}
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
			let buffer = Arc::new(tokio::sync::Semaphore::new(LOG_BUFFER_SIZE));
			let (sender, receiver) = tokio::sync::mpsc::channel(LOG_CHANNEL_CAPACITY);
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
				move |_| async move {
					let mut log_buffered_sender = Some(log_buffered_sender);
					let result = async {
						let sandbox_process = loop {
							if let Some(process) = sandbox_process.borrow().clone() {
								break process;
							}
							if sandbox_process.changed().await.is_err() {
								if let Some(sender) = log_buffered_sender.take() {
									sender.send(Ok(())).ok();
								}

								return Ok(());
							}
						};
						let input = sandbox
							.read_stdio(&sandbox_process, log_streams)
							.await
							.map_err(|error| tg::error!(!error, "failed to read process stdio"))?
							.boxed();

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
						log_sender
							.send(tangram_sandbox::stdio::read::Event::End)
							.await?;

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

					result
				}
			})),
		};
		let run_task = Task::spawn({
			let command = command.clone();
			let guest_url = guest_url.clone();
			let id = id.clone();
			let processes = processes.clone();
			let process_stopper = process_stopper.clone();
			let sandbox = sandbox.clone();
			let session = session.clone();
			let state = state.clone();
			let stopper = sandbox_stopper.clone();
			let token = token.clone();
			move |_| async move {
				let command = command.await?;
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
				session.run_process(arg).await
			}
		});

		let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
		let (index_sender, index_receiver) = tokio::sync::oneshot::channel();
		let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();

		// Collect and store the output concurrently with the control connection.
		let exited = Stopper::new();
		let finish_arg = FinishProcessRunArg {
			control: control.sender(),
			exited: exited.clone(),
			finish_sender,
			id: id.clone(),
			index_receiver,
			initialization,
			location: location.clone(),
			processes: processes.clone(),
			ready_receiver,
			run_task,
			sandbox: sandbox.clone(),
			state: state.clone(),
		};
		let finish_task = Task::spawn({
			let session = session.clone();
			move |_| async move { session.finish_process_run(finish_arg).boxed().await }
		});

		// Spawn the process control task.
		let (stderr_buffered_sender, stderr_buffered_receiver) = tokio::sync::oneshot::channel();
		let (stdout_buffered_sender, stdout_buffered_receiver) = tokio::sync::oneshot::channel();
		let log = log_receiver
			.map(|receiver| {
				let started_at = state
					.started_at
					.ok_or_else(|| tg::error!("expected the process to be started"))?;
				let arg = WriteProcessLogTaskArg {
					receiver,
					started_at,
				};

				Ok::<_, tg::Error>(arg)
			})
			.transpose()?;
		let (push_sender, push_receiver) = tokio::sync::oneshot::channel();
		let control_task = Task::spawn({
			let session = session.clone();
			let exited = exited.clone();
			let sandbox = sandbox.clone();
			let stdin = state.stdin.clone();
			let stdout = state.stdout.clone();
			let stderr = state.stderr.clone();
			|_| async move {
				let arg = RunProcessControlTaskArg {
					control,
					exited,
					finish: finish_receiver,
					local: control_receiver,
					log,
					push: push_receiver,
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
				session
					.run_process_control_task(arg)
					.boxed()
					.await
					.inspect_err(|error| {
						tracing::error!(error = %error.trace(), "the control task failed");
					})
			}
		});

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

		// Prepare command authorization before tracked finish writes can wait for initialization.
		let command_roots = session
			.prepare_process_command_grants(&state.command, &location, parent.as_ref())
			.await;
		let command_roots = match command_roots {
			Ok(roots) => roots,
			Err(error) => {
				process_stopper.stop();
				drop(index_sender);
				drop(ready_sender);
				finish_task.wait().await.ok();
				return Err(error);
			},
		};

		// Complete command grant preparation before a tracked finish write can wait for indexing.
		ready_sender.send(()).ok();

		let data = state.to_data();

		// Reuse the shortcut connection or connect the assigned process while it runs.
		let output = if let Some(output) = connection_output {
			Ok(output)
		} else {
			let arg = tg::process::control::Arg {
				data: Some(data.clone()),
				id: Some(id.clone()),
				lease: Some(lease.clone()),
				location: Some(location.clone().into()),
				options: options.clone(),
				parent: parent.clone(),
				start: true,
				sync: None,
			};
			session
				.connect_process_control(arg, control_responses.take().unwrap())
				.await
				.and_then(|(output, requests)| {
					requests_sender
						.send(requests)
						.map_err(|_| tg::error!("the process control stream was dropped"))?;
					Ok(output)
				})
		};
		let output = match output {
			Ok(output) => output,
			Err(error) => {
				process_stopper.stop();
				drop(index_sender);
				finish_task.wait().await.ok();

				return Err(error);
			},
		};
		let sync = output.sync.filter(|_| location.is_remote());
		processes
			.get_mut(&id)
			.expect("the process state was not found")
			.sync = sync;
		let entry = tg::process::Options {
			location: Some(location.clone().into()),
			state: Some(state.clone()),
			..Default::default()
		};
		let process = tg::Process::new(id.clone(), entry);
		session
			.server
			.messenger
			.publish(format!("sandboxes.{sandbox_id}.processes"), ())
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					"failed to publish the sandbox process spawned notification"
				)
			})?;

		// Index the remote process before reporting the connection.
		let arg = IndexProcessTaskArg {
			command: state.command.clone(),
			command_data: command.clone(),
			command_roots,
			data,
			id: &id,
			location: &location,
			options,
			parent: parent.as_ref(),
		};
		let index_result = session.spawn_index_process_task(arg).await;
		index_result_sender.send(index_result.clone()).ok();
		if let Err(error) = index_result {
			process_stopper.stop();
			drop(index_sender);
			finish_task.wait().await.ok();

			return Err(error);
		}
		crate::checkpoint!(self.server, "runner.process.index.signal", process = %id).await;
		index_sender.send(()).ok();

		if location.is_remote() {
			let entry = crate::process::control::Connected {
				lease: lease.clone(),
			};
			let result = session
				.server
				.messenger
				.publish(crate::process::control::connected_subject(&id), entry)
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
				lease: lease.clone(),
				process: output.process,
			})))
			.ok();

		let output = finish_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the process finish task panicked"))??;
		let FinishProcessRunOutput { data, index_task } = output;
		event_sender.send(Ok(Event::Exited)).ok();
		session
			.release_finished_process_children(&id, &processes)
			.await?;

		let buffered_task = Task::spawn({
			let event_sender = event_sender.clone();
			let id = id.clone();
			let server = session.server.clone();
			move |_| async move {
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
					crate::checkpoint!(
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
		});
		let arg = FinishProcessTaskArg {
			buffered_task,
			control_task,
			data,
			id,
			log_task,
			process,
			processes: processes.clone(),
			push: push_sender,
		};

		let result = session.finish_process_task(arg).boxed().await;
		if let Some(index_task) = index_task {
			index_task.wait().await.map_err(|error| {
				tg::error!(!error, "the finished process index task panicked")
			})??;
		}
		result?;

		Ok(())
	}

	pub(super) async fn create_process_control_connection(
		&self,
	) -> tg::Result<ProcessControlConnection> {
		let location = self.server.config.runner.remote.as_ref().map_or_else(
			|| tg::Location::Local(tg::location::Local::default()),
			|name| {
				tg::Location::Remote(tg::location::Remote {
					name: name.clone(),
					region: None,
				})
			},
		);
		let (input_high, receiver_high) = tokio::sync::mpsc::channel(512);
		let (input_low, receiver_low) = tokio::sync::mpsc::channel(512);
		let responses = crate::control::priority_stream(receiver_high, receiver_low)
			.map(Ok)
			.boxed();
		let arg = tg::process::control::Arg {
			data: None,
			id: None,
			lease: None,
			location: Some(location.into()),
			options: tg::referent::Options::default(),
			parent: None,
			start: false,
			sync: None,
		};
		let (output, requests) = self.connect_process_control(arg, responses).await?;
		let control = crate::control::Stream::new_reconnecting_with_priorities(
			requests,
			input_high.clone(),
			input_low,
			crate::control::stream_options(),
		);
		let connection = ProcessControlConnection {
			control,
			input: input_high,
			output,
		};

		Ok(connection)
	}

	async fn connect_process_control(
		&self,
		arg: tg::process::control::Arg,
		responses: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> tg::Result<ControlConnection> {
		crate::checkpoint!(self.server, "runner.process.control.connect", process = ?arg.id).await;
		let reconnect_context = self.context.clone();
		let reconnect_server = self.server.clone();
		let reconnect = move |output: &tg::process::control::Output| {
			let token = output.token.clone().or(reconnect_context.token.clone());
			let context = crate::Context {
				principal: tg::Principal::Process(output.process.node.clone()),
				token,
				..reconnect_context
			};
			reconnect_server.session(&context)
		};
		let (output, requests) = self
			.try_get_process_control_stream_all(arg, responses, reconnect)
			.boxed()
			.await
			.map_err(|source| tg::error!(!source, "failed to create the control stream"))?
			.ok_or_else(|| tg::error!("expected a control stream"))?;
		Ok((output, requests.boxed()))
	}

	async fn finish_process_run(
		&self,
		arg: FinishProcessRunArg,
	) -> tg::Result<FinishProcessRunOutput> {
		let FinishProcessRunArg {
			control,
			exited,
			finish_sender,
			id,
			index_receiver,
			initialization,
			location,
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
			Ok(output) => {
				let context = crate::Context {
					origin: crate::Origin::Sandbox(sandbox.index()),
					..self.context.clone()
				};
				let output_session = self.server.session(&context);
				output_session
					.collect_process_output(CollectProcessOutputArg {
						exit: output.exit,
						path: output.path,
						state: &state,
					})
					.await
			},
			Err(error) => Err(error),
		};

		let output = if let Some(finish) = finish {
			let error = finish
				.error
				.map(tg::Error::try_from)
				.transpose()
				.map_err(|error| tg::error!(!error, "failed to deserialize the process error"))?;
			Output {
				checksum: None,
				error,
				exit: finish.exit,
				value: None,
			}
		} else {
			match result {
				Ok(output) => output,
				Err(error) => {
					let code = match error.to_data_or_id() {
						tg::Either::Left(data) => data.code.unwrap_or(tg::error::Code::Internal),
						tg::Either::Right(_) => tg::error::Code::Internal,
					};
					let error =
						tg::error!(!error, code = code, process = %id, "failed to run the process");
					Output {
						checksum: None,
						error: Some(error),
						exit: 1,
						value: None,
					}
				},
			}
		};

		// Store the output.
		if let Some(value) = &output.value {
			value
				.store_with_handle(self)
				.await
				.map_err(|error| tg::error!(!error, "failed to store the output"))?;
		}

		// Store the error.
		let (mut error, error_code) = if let Some(error) = &output.error {
			let error = error.to_data_or_id();
			let error_code = match &error {
				tg::Either::Left(data) => data.code,
				tg::Either::Right(_) => None,
			};
			let error = self.store_process_error(error).await;
			(Some(error.map_right(tg::Referent::with_node)), error_code)
		} else {
			(None, None)
		};
		let mut exit = output.exit;

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
		if let Some(expected) = &data.expected_checksum
			&& exit == 0
		{
			if let Some(actual) = &output.checksum
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
			} else if output.checksum.is_none() && !expected.is_any() {
				return Err(tg::error!(?id, "the actual checksum was not set"));
			}
		}
		data.actual_checksum = output.checksum.clone();
		data.error = error;
		data.exit = Some(exit);
		data.finished_at = Some(self.server.clock.unix_timestamp()?);
		data.output = output.value.as_ref().map(tg::Value::to_data);
		data.status = tg::process::Status::Finished;
		Self::validate_process_data(&data)?;

		crate::checkpoint!(self.server, "runner.process.output.stored", process = %id).await;
		let command_id = state.command.command_id()?;
		crate::checkpoint!(self.server, "runner.process.finish", command = %command_id, process = %id).await;

		ready_receiver
			.await
			.map_err(|_| tg::error!("the process connection failed before initialization"))?;

		// The initial write establishes command grants; the finished write only needs proofs for the result objects.
		let index_task =
			if let Some(authorization) = self.try_prepare_finished_process_authorization(&data) {
				let arg = IndexFinishedProcessTaskArg {
					authorization,
					data: data.clone(),
					id: id.clone(),
					location,
				};
				let session = self.clone();
				let index_task = crate::process::IndexTask::spawn(move |_| async move {
					index_receiver
						.await
						.map_err(|_| tg::error!("the process connection failed before indexing"))?;
					let inner = session.clone();
					session
						.server
						.index_tasks
						.spawn(move |_| async move { inner.index_finished_process_task(arg).await })
						.wait()
						.await
						.map_err(|_| tg::error!("the finished process index task panicked"))?
				});
				self.publish_finished_process(&id, &processes, &data)
					.await?;
				Some(index_task)
			} else {
				index_receiver.await.map_err(|_| {
					tg::error!("the process connection failed before grant preparation")
				})?;
				let arg = RecordFinishedProcessArg {
					data: &data,
					id: &id,
					location: &location,
					processes: processes.clone(),
				};
				self.record_finished_process_local(arg).await?;
				None
			};

		// Publish local completion before waiting for command transfer and Start submission.
		if let Some(initialization) = initialization {
			initialization
				.await
				.map_err(|error| tg::error!(!error, "the process initialization failed"))?;
		}

		// Retain the finish request across retries and reconnects without waiting for its response.
		crate::checkpoint!(self.server, "runner.process.control.finish.request").await;
		let arg = tg::process::control::ClientRequestArg::Finish(
			tg::process::control::FinishClientRequestArg { data: data.clone() },
		);
		let response = Self::send_process_control_client_request_inner(
			&control,
			arg,
			crate::control::Priority::High,
		)
		.await?;
		finish_sender
			.send(response)
			.map_err(|_| tg::error!("failed to send the process finish response receiver"))?;
		crate::checkpoint!(self.server, "runner.process.control.finish.sent", process = %id).await;
		let output = FinishProcessRunOutput { data, index_task };

		Ok(output)
	}

	async fn index_finished_process_task(
		&self,
		arg: IndexFinishedProcessTaskArg,
	) -> tg::Result<()> {
		let IndexFinishedProcessTaskArg {
			authorization,
			data,
			id,
			location,
		} = arg;
		let options = crate::process::put::Options {
			defer_index: false,
			enqueue_log_compaction: false,
			location: Some(location.clone()),
			store_data: location.is_remote(),
		};
		let arg = tg::process::put::Arg {
			data,
			location: None,
		};
		crate::checkpoint!(
			self.server,
			"index.batch",
			command_object_grant = false,
			finished_process = true
		)
		.await;
		let result = self
			.put_process_local_inner(
				&id,
				arg,
				crate::process::put::ObjectGrants::Authorized(authorization),
				options,
			)
			.await;
		if let Err(error) = &result {
			tracing::error!(error = %error.trace(), process = %id, "failed to index the finished process");
		}
		result?;

		Ok(())
	}

	async fn record_finished_process_local(
		&self,
		arg: RecordFinishedProcessArg<'_>,
	) -> tg::Result<()> {
		let RecordFinishedProcessArg {
			data,
			id,
			location,
			processes,
		} = arg;

		// Queue the process grants before publishing completion so authorization can wait for indexing.
		// Grant preparation can call index(), so this work must remain outside server.index_tasks.
		// Leave the local process data to the control finish handler and log compaction to EOF handling.
		let remote = location.is_remote();
		let options = crate::process::put::Options {
			defer_index: self.server.config.advanced.single_process,
			enqueue_log_compaction: false,
			location: Some(location.clone()),
			store_data: remote,
		};
		self.put_finished_process_local(id, data.clone(), options)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to index the finished process"))?;
		self.publish_finished_process(id, &processes, data).await?;

		Ok(())
	}

	async fn publish_finished_process(
		&self,
		id: &tg::process::Id,
		processes: &crate::process::Processes,
		data: &tg::process::Data,
	) -> tg::Result<()> {
		let mut process_state = processes
			.get_mut(id)
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
		process_state.data.actual_checksum = data.actual_checksum.clone();
		process_state.data.cacheable = data.cacheable;
		process_state.data.error = data.error.clone();
		process_state.data.exit = data.exit;
		process_state.data.finished_at = data.finished_at;
		process_state.data.output = data.output.clone();
		process_state.data.status = tg::process::Status::Finished;
		process_state.changed.send_replace(());
		drop(process_state);
		crate::checkpoint!(self.server, "runner.process.finished", process = %id).await;

		Ok(())
	}

	async fn release_finished_process_children(
		&self,
		id: &tg::process::Id,
		processes: &crate::process::Processes,
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
				let session = self.clone();
				async move {
					crate::checkpoint!(
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
					if let Err(error) = session.cancel_process(&child, arg).await {
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
		arg: WriteProcessLogTaskArg,
		finished: tokio::sync::oneshot::Receiver<()>,
		sender: control::ProcessControlSender,
	) -> tg::Result<()> {
		let WriteProcessLogTaskArg {
			mut receiver,
			started_at,
		} = arg;
		let clock = self.server.clock.clone();
		let mut position = 0_u64;
		let mut stderr_position = 0_u64;
		let mut stdout_position = 0_u64;
		let mut requests = FuturesOrdered::new();
		let mut result = Ok(());
		let mut stream_ended = false;

		// Write the log chunks.
		while let Some(event) = receiver.recv().await {
			let LogEvent::Chunk {
				bytes,
				permit,
				stream,
			} = event
			else {
				stream_ended = true;
				break;
			};
			let length = u64::try_from(bytes.len()).unwrap();
			let prepared = (|| {
				let stream_position = match stream {
					tg::process::stdio::Stream::Stderr => stderr_position,
					tg::process::stdio::Stream::Stdin => {
						return Err(tg::error!("invalid stdio stream"));
					},
					tg::process::stdio::Stream::Stdout => stdout_position,
				};
				let timestamp = clock
					.unix_timestamp()?
					.checked_sub(started_at)
					.ok_or_else(|| tg::error!("the log timestamp is too small"))?;
				let next_position = position
					.checked_add(length)
					.ok_or_else(|| tg::error!("the log position is too large"))?;
				let (next_stderr_position, next_stdout_position) = match stream {
					tg::process::stdio::Stream::Stderr => {
						let next_stderr_position = stderr_position
							.checked_add(length)
							.ok_or_else(|| tg::error!("the stderr log position is too large"))?;
						(next_stderr_position, stdout_position)
					},
					tg::process::stdio::Stream::Stdin => unreachable!(),
					tg::process::stdio::Stream::Stdout => {
						let next_stdout_position = stdout_position
							.checked_add(length)
							.ok_or_else(|| tg::error!("the stdout log position is too large"))?;
						(stderr_position, next_stdout_position)
					},
				};
				let chunk = tg::process::stdio::Chunk {
					bytes,
					combined_position: position,
					stream,
					stream_position,
					timestamp: Some(timestamp),
				};
				let arg = tg::process::control::ClientRequestArg::Write(
					tg::process::control::WriteClientRequestArg::Chunk(chunk),
				);

				Ok((
					arg,
					next_position,
					next_stderr_position,
					next_stdout_position,
				))
			})();
			let (arg, next_position, next_stderr_position, next_stdout_position) = match prepared {
				Ok(prepared) => prepared,
				Err(error) => {
					result = Err(error);
					break;
				},
			};
			position = next_position;
			stderr_position = next_stderr_position;
			stdout_position = next_stdout_position;
			let priority = crate::control::Priority::Low;
			let response =
				Self::send_process_control_client_request_inner(&sender, arg, priority).await;
			let response = match response {
				Ok(response) => response,
				Err(error) => {
					result = Err(error);
					break;
				},
			};
			let request = Self::receive_process_log_response(response, permit, length);
			requests.push_back(request);
			if requests.len() >= LOG_REQUEST_CONCURRENCY
				&& let Some(request_result) = requests.next().await
				&& let Err(error) = request_result
			{
				result = Err(error);
				break;
			}
		}
		if result.is_ok() && !stream_ended {
			result = Err(tg::error!("the process log stream ended unexpectedly"));
		}
		if result.is_ok() {
			while let Some(request_result) = requests.next().await {
				if let Err(error) = request_result {
					result = Err(error);
					break;
				}
			}
		}
		drop(receiver);
		drop(requests);

		// Commit the end only after every chunk has a successful response.
		result?;
		finished
			.await
			.map_err(|_| tg::error!("failed to receive the process finish notification"))?;
		let end = tg::process::stdio::End {
			combined_position: position,
			stream_positions: [
				(tg::process::stdio::Stream::Stderr, stderr_position),
				(tg::process::stdio::Stream::Stdout, stdout_position),
			]
			.into(),
		};
		Self::send_process_log_end(&sender, end).await?;

		Ok(())
	}

	async fn send_process_log_end(
		sender: &control::ProcessControlSender,
		end: tg::process::stdio::End,
	) -> tg::Result<()> {
		let arg = tg::process::control::ClientRequestArg::Write(
			tg::process::control::WriteClientRequestArg::End(end),
		);
		let output =
			Self::send_process_control_client_request(sender, arg, crate::control::Priority::Low)
				.boxed()
				.await?
				.try_unwrap_write()
				.map_err(|_| tg::error!("expected a write process response"))?;
		if !output.closed || output.length != 0 {
			return Err(tg::error!("the log end was not confirmed"));
		}
		Ok(())
	}

	fn receive_process_log_response(
		response: control::ProcessControlResponseReceiver,
		permit: tokio::sync::OwnedSemaphorePermit,
		length: u64,
	) -> BoxFuture<'static, tg::Result<()>> {
		async move {
			let output = Self::receive_process_control_client_response(response)
				.await?
				.try_unwrap_write()
				.map_err(|_| tg::error!("expected a write process response"))?;
			if output.closed || output.length != length {
				return Err(tg::error!("the log write did not complete"));
			}
			drop(permit);
			Ok(())
		}
		.boxed()
	}

	async fn finish_process_task(&self, arg: FinishProcessTaskArg) -> tg::Result<()> {
		let FinishProcessTaskArg {
			buffered_task,
			control_task,
			data,
			id,
			log_task,
			process,
			processes,
			push,
		} = arg;
		let session = self;
		let sync = processes.get(&id).and_then(|state| state.sync.clone());
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

		// Push the output and error. The process has finished, so a failure is logged.
		if let Err(error) = session.push_process_output(&process, &data, sync).await {
			tracing::error!(error = %error.trace(), process = %id, "failed to push the process output");
		}
		push.send(()).ok();
		let control_result = match control_task.wait().await {
			Ok(result) => result,
			Err(error) => Err(tg::error!(!error, %id, "the process control task panicked")),
		};
		log_result?;
		buffered_result?;
		control_result?;

		Ok::<_, tg::Error>(())
	}

	async fn push_process_output(
		&self,
		process: &tg::Process,
		data: &tg::process::Data,
		sync: Option<tg::sync::Token>,
	) -> tg::Result<()> {
		let Some(tg::Location::Remote(remote)) = process
			.location()
			.and_then(|location| location.to_location())
		else {
			return Ok(());
		};

		// Collect the objects.
		let mut objects = Vec::new();
		if let Some(value) = &data.output {
			value.children_with_tokens(&mut objects);
		}
		if let Some(tg::Either::Right(id)) = &data.error {
			let id = tg::object::Id::Error(id.node.clone());
			objects.push(tg::Referent::with_node(id));
		}
		if objects.is_empty() {
			return Ok(());
		}

		// Push the objects.
		crate::checkpoint!(
			self.server,
			"runner.process.output.push.started",
			process = %process.id(),
		)
		.await;
		let destination = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: remote.region.clone(),
		});
		let arg = tg::push::Arg {
			destination: Some(destination),
			nodes: objects
				.into_iter()
				.map(|object| object.map(Into::into))
				.collect(),
			sync,
			..Default::default()
		};
		let stream = self
			.push_for_process(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to push the output"))?;
		let mut stream = std::pin::pin!(stream);
		while stream.try_next().await?.is_some() {}

		Ok(())
	}

	fn try_get_process_session(&self, id: &tg::process::Id) -> Option<Session> {
		let state = self.server.runner.state();
		let sandbox_id = state.processes().get(id)?.value().clone();
		let sandbox = state.sandboxes().get_by_id(&sandbox_id)?;
		let origin = Origin::Sandbox(*sandbox.key());
		let process = sandbox.processes.get(id)?;
		let token = process.inner_token.clone();
		drop(process);
		drop(sandbox);
		let context = Context {
			origin,
			principal: tg::Principal::Process(id.clone()),
			token: Some(token),
			..self.context.clone()
		};
		let session = self.server.session(&context);

		Some(session)
	}

	async fn try_load_process_command_local(
		&self,
		command: &tg::Command,
	) -> tg::Result<Option<tg::command::Data>> {
		let id = command.id();
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		let resource = tg::Referent::with_node_and_local_tokens(
			tg::object::Id::from(id.clone()),
			command.state().tokens().local_authorization().to_vec(),
		);
		let authorized = self.authorize(resource, permission).await?;
		if !authorized.is_some_and(|permissions| permissions.contains(permission)) {
			return Ok(None);
		}
		let id = tg::object::Id::from(id);
		let Some(output) = self.server.try_get_object_local(&id, false).await? else {
			return Ok(None);
		};
		let data = tg::command::Data::deserialize(output.bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the command"))?;
		let object = tg::command::Object::try_from_data(data.clone())?;
		command.state().set_object(Arc::new(object));

		Ok(Some(data))
	}

	async fn push_process_command(
		session: &Session,
		command: &tg::Referent<tg::Either<Box<tg::process::data::Command>, tg::command::Id>>,
		location: &tg::Location,
	) -> tg::Result<()> {
		let id = command.command_id()?;
		crate::checkpoint!(session.server, "runner.process.command.push.started", command = %id)
			.await;
		let result = Self::push_process_command_inner(session, command, location).await;
		if let Err(error) = &result {
			tracing::error!(error = %error.trace(), "failed to push the command");
		}
		crate::checkpoint!(session.server, "runner.process.command.push.finished", command = %id)
			.await;
		result?;
		Ok(())
	}

	async fn push_process_command_inner(
		session: &Session,
		command: &tg::Referent<tg::Either<Box<tg::process::data::Command>, tg::command::Id>>,
		location: &tg::Location,
	) -> tg::Result<()> {
		let nodes = command
			.objects()
			.into_iter()
			.map(|object| object.map(Into::into))
			.collect::<Vec<_>>();
		if nodes.is_empty() {
			return Ok(());
		}
		let arg = tg::push::Arg {
			destination: Some(location.clone()),
			nodes,
			process_commands: true,
			..Default::default()
		};
		let stream = session.push_for_process(arg).await?;
		let mut stream = std::pin::pin!(stream);
		while let Some(event) = stream.try_next().await? {
			if event.is_output() {
				return Ok(());
			}
		}

		Err(tg::error!(
			command = %command.command_id()?,
			"failed to push the command: expected an output"
		))
	}

	async fn prepare_process_command_grants(
		&self,
		command: &tg::Referent<tg::Either<Box<tg::process::data::Command>, tg::command::Id>>,
		location: &tg::Location,
		parent: Option<&tg::process::Id>,
	) -> tg::Result<Vec<tangram_index::process::object::grant::Root>> {
		if !location.is_remote() {
			return Ok(Vec::new());
		}
		let Some(parent) = parent else {
			return Ok(Vec::new());
		};
		let context = crate::Context {
			principal: tg::Principal::Process(parent.clone()),
			token: None,
			..self.context.clone()
		};
		let session = self.server.session(&context);
		let roots = session
			.prepare_process_object_grant_roots(
				command.objects(),
				tg::authorization::permission::object::Set::NODE,
			)
			.await?;
		Ok(roots)
	}

	async fn spawn_index_process_task(&self, arg: IndexProcessTaskArg<'_>) -> tg::Result<()> {
		let IndexProcessTaskArg {
			command,
			command_data,
			command_roots,
			data,
			id,
			location,
			mut options,
			parent,
		} = arg;
		if !location.is_remote() {
			return Ok(());
		}
		crate::checkpoint!(
			self.server,
			"runner.process.index.started",
			process = %id,
		)
		.await;

		// Resolve the command before a finish can register its tracked write.
		command_data.await?;

		let data = data.without_location_and_tokens();
		options.clear_location_and_tokens();
		let command_id = data.command.command_id()?;
		let sandbox = data.sandbox.clone();
		let now = self.server.clock.unix_timestamp()?;
		let time_to_live = i64::try_from(self.server.config.object.grant_time_to_live.as_secs())
			.map_err(|error| tg::error!(!error, "failed to convert the grant time to live"))?;
		let expires_at = now + time_to_live;
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
			sandbox: Some(sandbox),
			storage: tangram_index::process::Storage::default(),
			subtree_objects: std::collections::BTreeSet::new(),
			time_to_touch: self.server.config.process.time_to_touch,
			touched_at: now,
		};
		let mut items = vec![tangram_index::batch::Item::PutProcess(put_process_arg)];
		if let Some(parent) = parent {
			let grant_arg = tangram_index::process::object::grant::Arg {
				authorize: crate::authorization_search_config(
					&self.server.config.authorization.final_,
				),
				created_at: now,
				expires_at: Some(expires_at),
				principal: tg::Principal::Process(parent.clone()),
				process: id.clone(),
				roots: command_roots,
				time_to_touch: Some(self.server.config.object.grant_time_to_touch),
			};
			items.push(tangram_index::batch::Item::PutProcessObjectGrants(
				grant_arg,
			));
		} else {
			for command in command.objects() {
				let permission = tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Node,
				);
				let grant_arg = tangram_index::grant::put::Arg {
					created_at: now,
					creator: Some(self.context.principal.clone()),
					implicit: Some(Some(expires_at)),
					permissions: permission.into(),
					resource: command.node.into(),
					subject: tg::authorization::Subject::Process(id.clone()),
					time_to_touch: Some(self.server.config.object.grant_time_to_touch),
				};
				items.push(tangram_index::batch::Item::PutGrant(grant_arg));
			}
		}

		// Apply the initial data before the finished data can be written.
		let arg = tangram_index::batch::Arg { items };
		self.server
			.index
			.batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to index the remote process"))?;

		Ok(())
	}

	async fn run_process(&self, arg: RunProcessArg) -> tg::Result<RunProcessOutput> {
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
			self.checkout_process_artifacts(
				command,
				&state.sandbox,
				progress_sender.clone(),
				&state.stderr,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to check out the children"))?;

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
			let engine = match self.server.config.runner.js.engine {
				crate::config::JsEngine::Auto => "auto",
				crate::config::JsEngine::QuickJs => "quickjs",
				crate::config::JsEngine::V8 => "v8",
			};
			env.insert("TANGRAM_JS_ENGINE".to_owned(), engine.to_owned());
			for key in [
				"TANGRAM_JS_DEBUG",
				"TANGRAM_JS_DEBUG_ADDR",
				"TANGRAM_JS_DEBUG_MODE",
			] {
				env.remove(key);
			}
			if let Some(debug) = state.debug.as_ref() {
				env.insert("TANGRAM_JS_DEBUG".to_owned(), "true".to_owned());
				if let Some(addr) = debug.addr {
					env.insert("TANGRAM_JS_DEBUG_ADDR".to_owned(), addr.to_string());
				}
				if debug.mode != tg::process::debug::Mode::Normal {
					env.insert("TANGRAM_JS_DEBUG_MODE".to_owned(), debug.mode.to_string());
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
			crate::checkpoint!(
				self.server,
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
					.write_process_stdin_blob(command.stdin.as_ref(), &sandbox, &sandbox_process)
					.await;
				if result.is_err() {
					process_stopper.stop();
				}
				result
			};
			let (exit, stdin) = future::join(self.wait_for_process(arg).boxed(), stdin).await;
			stdin?;
			let exit = exit?;
			crate::checkpoint!(
				self.server,
				"runner.process.exit",
				command = %command_id,
			)
			.await;

			let output = RunProcessOutput {
				exit,
				path: host_output_path,
			};

			Ok(output)
		}
		.boxed()
		.await;

		// Drop the sender so that the i/o tasks observe that the sandbox process will never be spawned if it has not been.
		drop(sandbox_process_sender);

		result
	}

	async fn write_process_stdin_blob(
		&self,
		blob: Option<&tg::Referent<tg::blob::Id>>,
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
	) -> tg::Result<()> {
		let Some(blob) = blob else {
			return Ok(());
		};
		let blob = tg::Blob::with_referent(blob.clone());
		let reader = blob
			.read_with_handle(self, tg::read::Options::default())
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

	async fn wait_for_process(&self, arg: WaitForProcessArg<'_>) -> tg::Result<u8> {
		let WaitForProcessArg {
			process_stopper,
			sandbox,
			sandbox_process,
			stopper,
		} = arg;
		let wait = sandbox
			.wait(sandbox_process)
			.await
			.map_err(|error| tg::error!(!error, "failed to start waiting for the process"))?;
		let mut wait = std::pin::pin!(wait);
		let (exit, stopped) = tokio::select! {
				result = &mut wait => {
					let exit = result.map_err(
						|error| tg::error!(!error, "failed to wait for the process"),
					)?;
					(exit, false)
				},
				() = stopper.wait() => {
					sandbox.kill(sandbox_process, tg::process::Signal::SIGKILL).await.ok();
					let exit = wait.await.map_err(
						|error| tg::error!(!error, "failed to wait for the process"),
					)?;
					(exit, true)
				},
				() = process_stopper.wait() => {
					sandbox.kill(sandbox_process, tg::process::Signal::SIGKILL).await.ok();
					let exit = wait.await.map_err(
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

	async fn collect_process_output(&self, arg: CollectProcessOutputArg<'_>) -> tg::Result<Output> {
		let CollectProcessOutputArg { exit, path, state } = arg;
		let mut output = Output {
			checksum: None,
			error: None,
			exit,
			value: None,
		};
		let exists = tokio::fs::try_exists(&path)
			.await
			.map_err(|error| tg::error!(!error, "failed to determine if the output path exists"))?;

		// Try to read the user.tangram.checksum xattr.
		if let Ok(Some(bytes)) = tg::file::xattrs::read_checksum(&path) {
			let checksum = String::from_utf8(bytes)
				.map_err(|error| tg::error!(!error, "failed to parse the checksum xattr"))
				.and_then(|string| string.parse::<tg::Checksum>())
				.map_err(|error| tg::error!(!error, "failed to parse the checksum string"))?;
			output.checksum = Some(checksum);
		}

		// Try to read the user.tangram.output xattr.
		if let Ok(Some(bytes)) = tg::file::xattrs::read_output(&path) {
			let tgon = String::from_utf8(bytes)
				.map_err(|error| tg::error!(!error, "failed to decode the output xattr"))?;
			output.value = Some(
				tgon.parse::<tg::Value>()
					.map_err(|error| tg::error!(!error, "failed to parse the output xattr"))?,
			);
		}

		// Try to read the user.tangram.error xattr.
		if let Ok(Some(bytes)) = tg::file::xattrs::read_error(&path) {
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
			output.error = Some(error);
		}

		// Check in the output.
		if output.value.is_none() && exists {
			let path = self.guest_path_for_host_path(&path)?;
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
			let checkin_output = self
				.checkin(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to check in the output"))?
				.try_last()
				.await?
				.and_then(|event| event.try_unwrap_output().ok())
				.ok_or_else(|| tg::error!("stream ended without output"))?;
			let artifact = tg::Artifact::with_referent(checkin_output.artifact);
			let value = artifact.into();
			output.value = Some(value);
		}

		// Compute the checksum if necessary.
		if let (Some(checksum), None, Some(value)) =
			(&state.expected_checksum, &output.checksum, &output.value)
		{
			let algorithm = checksum.algorithm();
			let checksum = self
				.compute_checksum(value, algorithm)
				.await
				.map_err(|error| tg::error!(!error, "failed to compute the checksum"))?;
			output.checksum = Some(checksum);
		}

		Ok(output)
	}

	async fn compute_checksum(
		&self,
		value: &tg::Value,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		if let Ok(blob) = value.clone().try_into() {
			self.checksum_blob(&blob, algorithm).await
		} else if let Ok(artifact) = value.clone().try_into() {
			self.checksum_artifact(&artifact, algorithm).await
		} else {
			Err(tg::error!(
				"cannot checksum a value that is not a blob or an artifact"
			))
		}
	}

	async fn checkout_process_artifacts(
		&self,
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
					self.authorize_token(&resource, permissions, token)
						.then(|| (artifact.node.clone(), token.clone()))
				})
		});
		if let Some(mut state) = self.server.runner.state.sandboxes.get_mut_by_id(sandbox) {
			state.tokens.extend(tokens);
		}
		if self.server.vfs.lock().unwrap().is_some() {
			return Ok(());
		}

		// Check out the artifacts.
		let stream = self
			.checkout_internal(artifacts)
			.await
			.map_err(|error| tg::error!(!error, "failed to check out the artifacts"))?;

		// Write progress.
		self.write_progress_stream(progress, stderr, stream)
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
