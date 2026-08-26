use {
	self::control::RunProcessControlTaskArg,
	crate::{Context, Origin, Session},
	bytes::Bytes,
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::{self, BoxFuture, Shared},
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
	tangram_messenger::Messenger as _,
	tokio::task::JoinSet,
	tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream},
};

mod control;
mod progress;

type CommandFuture = Shared<BoxFuture<'static, tg::Result<(tg::command::Data, Session)>>>;

pub(super) struct SpawnProcessTaskArg<'a> {
	pub guest_url: &'a tangram_uri::Uri,
	pub location: tg::Location,
	pub process: tg::runner::control::Process,
	pub process_stopper: &'a Stopper,
	pub process_tasks: &'a mut JoinSet<tg::Result<()>>,
	pub processes: Arc<crate::process::Processes>,
	pub retention_stopper: Stopper,
	pub sandbox: &'a tangram_sandbox::Sandbox,
	pub sandbox_id_receiver: Option<tokio::sync::oneshot::Receiver<tg::sandbox::Id>>,
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
	sandbox_id_receiver: Option<tokio::sync::oneshot::Receiver<tg::sandbox::Id>>,
	sandbox_stopper: Stopper,
}

struct FinishProcessTaskArg {
	buffered_task: Task<tg::Result<()>>,
	control_task: Task<tg::Result<()>>,
	finish_sender: tokio::sync::oneshot::Sender<tg::process::Data>,
	id: tg::process::Id,
	log_task: Option<Task<tg::Result<()>>>,
	log_write_task: Option<Task<tg::Result<()>>>,
	output: tg::Result<Output>,
	process: tg::Process,
	process_index: u64,
	processes: Arc<crate::process::Processes>,
	progress_sender: tokio::sync::mpsc::UnboundedSender<Bytes>,
}

struct IndexProcessTaskArg<'a> {
	command: tg::Referent<tg::command::Id>,
	command_data: CommandFuture,
	data: tg::process::Data,
	id: &'a tg::process::Id,
	location: &'a tg::Location,
	options: tg::referent::Options,
	parent: Option<&'a tg::process::Id>,
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
	pub grant: Option<tg::authorization::Token>,
	pub lease: String,
	pub process: tg::process::Id,
}

#[derive(Clone, Debug)]
struct Output {
	checksum: Option<tg::Checksum>,
	error: Option<tg::Error>,
	exit: u8,
	value: Option<tg::Value>,
}

struct RunProcessArg {
	command: tg::command::Data,
	guest_url: tangram_uri::Uri,
	process_index: u64,
	process_stopper: Stopper,
	processes: Arc<crate::process::Processes>,
	progress_sender: tokio::sync::mpsc::UnboundedSender<Bytes>,
	sandbox: tangram_sandbox::Sandbox,
	sandbox_process_sender: tokio::sync::watch::Sender<Option<Arc<tangram_sandbox::Process>>>,
	state: tg::process::State,
	stopper: Stopper,
	token: String,
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
		let guest_url = arg.guest_url.clone();
		let location = arg.location;
		let sandbox_id_receiver = arg.sandbox_id_receiver;
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
				sandbox_id_receiver,
				sandbox_stopper,
			};
			let result = session.process_task(arg).boxed().await;
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
			sandbox_id_receiver,
			sandbox_stopper,
		} = arg;
		let tg::runner::control::Process {
			data,
			id,
			options,
			parent,
			token: inner_token,
		} = process;
		let assigned = id.is_some();
		let mut state = tg::process::State::try_from_data(data)?;
		let mut command_options = options.clone();
		let local = command_options
			.tokens
			.local()
			.is_some_and(|token| self.verify_local_token(token));
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
		state
			.command
			.state()
			.inherit_location(command_options.location.as_ref());
		state
			.command
			.state()
			.inherit_tokens(&command_options.tokens);
		let process_stopper = Stopper::new();
		let lease = Self::create_process_lease();
		let (control_sender, control_responses) = tokio::sync::mpsc::channel(512);
		let context = match (&id, &inner_token) {
			(Some(id), Some(inner_token)) => crate::Context {
				principal: tg::Principal::Process(id.clone()),
				token: Some(inner_token.clone()),
				..self.context.clone()
			},
			(None, None) => {
				let runner = self
					.server
					.runner
					.state
					.id()
					.ok_or_else(|| tg::error!("missing the runner id"))?;
				let token = self.server.config.runner.token.clone();
				crate::Context {
					principal: tg::Principal::Runner(runner),
					token,
					..self.context.clone()
				}
			},
			_ => {
				return Err(tg::error!(
					"the process id and token must be provided together"
				));
			},
		};
		let session = self.server.session(&context);
		let command_session = if assigned {
			session.clone()
		} else {
			let parent = parent
				.as_ref()
				.ok_or_else(|| tg::error!("a process on the shortcut path must have a parent"))?;
			let mut session = self
				.try_get_process_session(parent)
				.ok_or_else(|| tg::error!(%parent, "failed to find the parent process session"))?;
			session.context.stopper = None;
			session
		};
		let sandbox_index = sandbox.index();

		// Store the process state before starting the process or connecting control.
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
		let (process_id_sender, process_id_receiver) = tokio::sync::watch::channel(id.clone());
		let process_index = session.server.runner.state.create_process_index();
		let sandbox_id = state.sandbox.clone();
		let entry = crate::process::State {
			children,
			control: control_sender.clone(),
			data,
			finish: None,
			id: id.clone(),
			id_receiver: process_id_receiver.clone(),
			index_task: index_task.clone(),
			inner_token: inner_token.clone(),
			leases: BTreeSet::from([lease.clone()]),
			process: None,
			stopper: process_stopper.clone(),
		};
		processes.insert(process_index, entry);
		if let Some(id) = &id {
			session
				.server
				.runner
				.state
				.processes
				.insert(id.clone(), sandbox_id);
		}
		let processes_for_cleanup = processes.clone();
		let server = session.server.clone();
		scopeguard::defer! {
			if let Some(process) = processes_for_cleanup.remove(process_index)
				&& let Some(id) = process.id
			{
				server.runner.state.processes.remove(&id);
			}
		}
		crate::checkpoint!(
			session.server,
			"runner.process.state.inserted",
			process = ?id,
			process_index,
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
					entry.insert((sandbox_index, process_index));
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
			let mut command = state.command.to_referent();
			command.options.location = None;
			let command = tg::Command::with_referent(command);
			let context = self.context.clone();
			let mut process_id_receiver = process_id_receiver.clone();
			let process_stopper = process_stopper.clone();
			let command_session = command_session.clone();
			let server = self.server.clone();
			async move {
				// Check whether the command is available locally.
				let command_id: tg::object::Id = command.id().into();
				let local = server
					.try_get_object_local(&command_id, false)
					.await
					.map_err(|error| tg::error!(!error, "failed to get the local command"))?
					.is_some();

				// Select the execution session.
				let session = if local {
					command_session
				} else {
					let id = loop {
						if let Some(id) = process_id_receiver.borrow().clone() {
							break id;
						}
						tokio::select! {
							result = process_id_receiver.changed() => {
								result.map_err(|error| tg::error!(!error, "failed to receive the process ID"))?;
							},
							() = process_stopper.wait() => {
								return Err(tg::error!("the process was stopped before its ID was assigned"));
							},
						}
					};
					let context = crate::Context {
						principal: tg::Principal::Process(id),
						token: None,
						..context
					};
					server.session(&context)
				};

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

				Ok((data, session))
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
			let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
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
										tg::process::stdio::read::Event::Chunk(
											tg::process::stdio::Chunk {
												bytes,
												position: None,
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
							if matches!(event, tg::process::stdio::read::Event::End) {
								if let Some(sender) = log_buffered_sender.take() {
									sender.send(Ok(())).ok();
								}

								continue;
							}
							log_sender
								.send(event)
								.map_err(|_| tg::error!("failed to buffer the process logs"))?;
						}
						log_sender
							.send(tg::process::stdio::read::Event::End)
							.map_err(|_| tg::error!("failed to buffer the process logs"))?;

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
		let mut run_task = Some(Task::spawn({
			let command = command.clone();
			let guest_url = guest_url.clone();
			let processes = processes.clone();
			let process_stopper = process_stopper.clone();
			let progress_sender = progress_sender.clone();
			let sandbox = sandbox.clone();
			let state = state.clone();
			let stopper = sandbox_stopper.clone();
			let token = token.clone();
			move |_| async move {
				let (command, session) = command.await?;
				session
					.run_process(RunProcessArg {
						command,
						guest_url,
						process_index,
						process_stopper,
						processes,
						progress_sender,
						sandbox,
						sandbox_process_sender,
						state,
						stopper,
						token,
					})
					.await
			}
		}));
		// Wait for the sandbox ID before connecting the control stream.
		if let Some(sandbox_id_receiver) = sandbox_id_receiver {
			let sandbox_id = match sandbox_id_receiver.await {
				Ok(sandbox_id) => sandbox_id,
				Err(error) => {
					process_stopper.stop();
					run_task.take().unwrap().wait().await.ok();

					return Err(tg::error!(!error, "failed to receive the sandbox ID"));
				},
			};
			if id.is_some() && state.sandbox != sandbox_id {
				let error = tg::error!(
					process = ?id,
					sandbox = %sandbox_id,
					"the process is not in the sandbox"
				);
				process_stopper.stop();
				run_task.take().unwrap().wait().await.ok();

				return Err(error);
			}
			state.sandbox = sandbox_id;
		}
		let sandbox_id = state.sandbox.clone();
		processes
			.get_mut(process_index)
			.expect("the process state was not found")
			.data
			.sandbox = sandbox_id.clone();

		// Push the command before connecting process control.
		if id.is_none() {
			let parent = parent
				.as_ref()
				.ok_or_else(|| tg::error!("a process on the shortcut path must have a parent"))?;
			let mut parent_session = session
				.try_get_process_session(parent)
				.ok_or_else(|| tg::error!(%parent, "failed to find the parent process session"))?;
			parent_session.context.stopper = None;
			let command = state.command.to_referent();
			crate::checkpoint!(
				parent_session.server,
				"runner.process.command.push.started",
				command = %command.node,
			)
			.await;
			let result = Self::push_process_command(&parent_session, &command, &location).await;
			if let Err(error) = &result {
				tracing::error!(error = %error.trace(), "failed to push the command");
			}
			crate::checkpoint!(
				parent_session.server,
				"runner.process.command.push.finished",
				command = %command.node,
			)
			.await;
			if let Err(error) = result {
				process_stopper.stop();
				run_task.take().unwrap().wait().await.ok();

				return Err(tg::error!(!error, "failed to push the process command"));
			}
		}

		crate::checkpoint!(
			session.server,
			"runner.process.control.connect",
			process = ?id,
		)
		.await;

		// Create the control stream.
		let control_responses = ReceiverStream::new(control_responses).map(Ok).boxed();
		let arg = tg::process::control::Arg {
			data: Some(state.to_data()),
			id,
			lease: lease.clone(),
			location: Some(location.clone().into()),
			options: options.clone(),
			parent: parent.clone(),
		};
		let connection =
			Box::pin(session.try_get_process_control_stream_all(arg, control_responses))
				.await
				.map_err(|source| tg::error!(!source, "failed to create the control stream"))
				.and_then(|connection| {
					connection.ok_or_else(|| tg::error!("expected a control stream"))
				});
		let (output, requests) = match connection {
			Ok(connection) => connection,
			Err(error) => {
				process_stopper.stop();
				run_task.take().unwrap().wait().await.ok();

				return Err(error);
			},
		};
		let requests = requests.boxed();
		let id = output.id;
		let Some(inner_token) = output.token.or(inner_token) else {
			process_stopper.stop();
			run_task.take().unwrap().wait().await.ok();

			return Err(tg::error!(%id, "missing the process authentication token"));
		};
		processes
			.get_mut(process_index)
			.expect("the process state was not found")
			.inner_token = Some(inner_token.clone());
		if !assigned {
			processes.set_id(process_index, id.clone());
			session
				.server
				.runner
				.state
				.processes
				.insert(id.clone(), sandbox_id.clone());
			process_id_sender.send_replace(Some(id.clone()));
		}
		let entry = tg::process::Options {
			location: Some(location.clone().into()),
			state: Some(state.clone()),
			..Default::default()
		};
		let process = tg::Process::new(id.clone(), entry);
		let context = crate::Context {
			principal: tg::Principal::Process(id.clone()),
			token: Some(inner_token.clone()),
			..self.context.clone()
		};
		let session = self.server.session(&context);
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
		let index_result = session
			.spawn_index_process_task(IndexProcessTaskArg {
				command: state.command.to_referent(),
				command_data: command.clone(),
				data: state.to_data(),
				id: &id,
				location: &location,
				options,
				parent: parent.as_ref(),
			})
			.await;
		index_result_sender.send(index_result.clone()).ok();
		if let Err(error) = index_result {
			process_stopper.stop();
			run_task.take().unwrap().wait().await.ok();

			return Err(error);
		}
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
				run_task.take().unwrap().wait().await.ok();

				return Err(error);
			}
		}
		event_sender
			.send(Ok(Event::Connected(ConnectedEvent {
				grant: output.grant,
				lease: lease.clone(),
				process: id.clone(),
			})))
			.ok();

		// Spawn the process control task.
		let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
		let (stderr_buffered_sender, stderr_buffered_receiver) = tokio::sync::oneshot::channel();
		let (stdout_buffered_sender, stdout_buffered_receiver) = tokio::sync::oneshot::channel();
		let stdin_blob = command
			.clone()
			.await
			.ok()
			.and_then(|(command, _)| command.stdin.map(tg::Blob::with_id));
		let control_task = Task::spawn({
			let session = session.clone();
			let sandbox = sandbox.clone();
			let stdin = state.stdin.clone();
			let stdout = state.stdout.clone();
			let stderr = state.stderr.clone();
			|_| async move {
				session
					.run_process_control_task(RunProcessControlTaskArg {
						finish: finish_receiver,
						requests,
						retention_stopper,
						sandbox,
						sandbox_process: sandbox_process_receiver,
						sender: control_sender,
						stderr,
						stderr_buffered: stderr_buffered_sender,
						stderr_progress,
						stdin,
						stdin_blob,
						stdout,
						stdout_buffered: stdout_buffered_sender,
					})
					.await
					.inspect_err(|error| {
						tracing::error!(error = %error.trace(), "the control task failed");
					})
			}
		});

		// Write logs while the process runs, buffering any output produced before the control stream connected.
		let log_write_task = log_receiver.map(|receiver| {
			Task::spawn({
				let id = id.clone();
				let location = location.clone();
				let session = session.clone();
				move |_| async move {
					let input = UnboundedReceiverStream::new(receiver).map(Ok).boxed();
					let arg = tg::process::stdio::write::Arg {
						location: Some(location.into()),
						streams: log_streams,
						tokens: tg::authorization::Tokens::default(),
					};
					if let Some(output) = session.try_write_process_stdio(&id, arg, input).await? {
						let mut output = std::pin::pin!(output);
						while output.try_next().await?.is_some() {}
					}

					Ok::<_, tg::Error>(())
				}
			})
		});
		let result = run_task
			.take()
			.unwrap()
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the process task panicked"))?;
		let result = match result {
			Ok(output) => {
				let context = crate::Context {
					origin: crate::Origin::Sandbox(sandbox.index()),
					..session.context.clone()
				};
				let output_session = session.server.session(&context);
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
		crate::checkpoint!(
			session.server,
			"runner.process.finish",
			command = %state.command,
			process = %id,
		)
		.await;
		event_sender.send(Ok(Event::Exited)).ok();
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
			finish_sender,
			id,
			log_task,
			log_write_task,
			output: result,
			process,
			process_index,
			processes: processes.clone(),
			progress_sender,
		};

		session.finish_process_task(arg).boxed().await
	}

	async fn finish_process_task(&self, arg: FinishProcessTaskArg) -> tg::Result<()> {
		let FinishProcessTaskArg {
			buffered_task,
			control_task,
			finish_sender,
			id,
			log_task,
			log_write_task,
			output: result,
			process,
			process_index,
			processes,
			progress_sender,
		} = arg;
		let session = self;
		let remote = process
			.location()
			.and_then(|location| location.to_location())
			.is_some_and(|location| location.is_remote());

		let finish = processes
			.get_mut(process_index)
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?
			.finish
			.take();
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
					let error = tg::error!(
						!error,
						code = code,
						process = %process.id(),
						"failed to run the process"
					);
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
		let mut value = if let Some(value) = &output.value {
			value
				.store_with_handle(session)
				.await
				.map_err(|error| tg::error!(!error, "failed to store the output"))?;
			let data = value.to_data();
			Some(data)
		} else {
			None
		};

		// Store the error.
		let (mut error, mut error_code) = if let Some(error) = &output.error {
			let error = error.to_data_or_id();
			let error_code = match &error {
				tg::Either::Left(data) => data.code,
				tg::Either::Right(_) => None,
			};
			let error = session.store_process_error(error).await;
			(Some(error.map_right(tg::Referent::with_node)), error_code)
		} else {
			(None, None)
		};
		let mut exit = output.exit;

		// Push the output and error.
		let push_result = async {
			let Some(tg::Location::Remote(remote)) = process
				.location()
				.and_then(|location| location.to_location())
			else {
				return Ok::<_, tg::Error>(());
			};

			let mut objects = Vec::new();
			if let Some(value) = &value {
				value.children_with_tokens(&mut objects);
			}
			if let Some(tg::Either::Right(id)) = &error {
				let id = id.node.clone();
				let object = tg::Referent::with_node(tg::object::Id::Error(id));
				objects.push(object);
			}
			if objects.is_empty() {
				return Ok(());
			}
			let arg = tg::push::Arg {
				destination: Some(tg::Location::Remote(tg::location::Remote {
					name: remote.name.clone(),
					region: remote.region.clone(),
				})),
				nodes: objects
					.into_iter()
					.map(|object| object.map(Into::into))
					.collect(),
				..Default::default()
			};
			let stream = session
				.push_for_process(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to push the output"))?;
			let state = process
				.load_with_handle(session)
				.await
				.map_err(|error| tg::error!(!error, "failed to load the process"))?;
			session
				.write_progress_stream(progress_sender.clone(), &state.stderr, stream)
				.await
				.map_err(|error| tg::error!(!error, "failed to log the progress stream"))?;

			Ok(())
		}
		.await;
		if let Err(push_error) = push_result {
			let push_error = tg::error!(
				!push_error,
				code = tg::error::Code::Internal,
				process = %process.id(),
				"failed to push the process output"
			);
			error = Some(
				push_error
					.to_data_or_id()
					.map_right(tg::Referent::with_node),
			);
			error_code = Some(tg::error::Code::Internal);
			exit = 1;
			value = None;
		}

		// Finish draining and writing the logs.
		drop(progress_sender);
		let log_read_result = if let Some(log_task) = log_task {
			log_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the log read task panicked"))
				.and_then(|result| {
					result.map_err(|error| tg::error!(!error, "failed to read the process logs"))
				})
		} else {
			Ok(())
		};
		let log_write_result = if let Some(log_write_task) = log_write_task {
			log_write_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the log write task panicked"))
				.and_then(|result| {
					result.map_err(|error| tg::error!(!error, "failed to write the process logs"))
				})
		} else {
			Ok(())
		};
		let log_result = log_write_result
			.and(log_read_result)
			.map_err(|error| tg::error!(!error, "failed to drain the process logs"));
		if let Err(log_error) = log_result {
			let log_error = tg::error!(
				!log_error,
				code = tg::error::Code::Internal,
				process = %process.id(),
				"failed to handle the process logs"
			);
			let log_error = session.store_process_error(log_error.to_data_or_id()).await;
			error = Some(log_error.map_right(tg::Referent::with_node));
			error_code = Some(tg::error::Code::Internal);
			exit = 1;
		}

		let id = process.id().unwrap_right();
		let mut process_state = processes
			.get_mut(process_index)
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
		if matches!(
			error_code,
			Some(
				tg::error::Code::Cancellation
					| tg::error::Code::HeartbeatExpiration
					| tg::error::Code::Internal
			)
		) {
			process_state.data.cacheable = false;
		}
		if let Some(expected) = &process_state.data.expected_checksum
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
				return Err(tg::error!(%id, "the actual checksum was not set"));
			}
		}
		process_state.data.actual_checksum = output.checksum;
		process_state.data.error = error;
		process_state.data.exit = Some(exit);
		process_state.data.finished_at = Some(self.server.clock.unix_timestamp()?);
		process_state.data.output = value;
		process_state.data.status = tg::process::Status::Finished;
		let child_leases = process_state
			.children
			.iter_mut()
			.filter_map(|(id, child)| {
				let lease = child.lease.take()?;
				let location = child.location.take();
				Some((id.clone(), lease, location))
			})
			.collect::<Vec<_>>();
		let data = process_state.data();
		drop(process_state);

		// Store the finished remote process in the runner's local index.
		if remote {
			session
				.put_finished_process_local(id, data.clone())
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to index the finished remote process"),
				)?;
		}

		child_leases
			.into_iter()
			.map(|(child, lease, location)| {
				let parent = id.clone();
				let session = session.clone();
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

		finish_sender
			.send(data)
			.map_err(|_| tg::error!(%id, "failed to send the finished process data"))?;
		buffered_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, %id, "the process buffered task panicked"))??;
		control_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, %id, "the process control task panicked"))??;

		Ok::<_, tg::Error>(())
	}

	fn try_get_process_session(&self, id: &tg::process::Id) -> Option<Session> {
		let state = self.server.runner.state();
		let sandbox_id = state.processes().get(id)?.value().clone();
		let sandbox = state.sandboxes().get_by_id(&sandbox_id)?;
		let origin = Origin::Sandbox(*sandbox.key());
		let process = sandbox.processes.get_by_id(id)?;
		let token = process.inner_token.clone()?;
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
		let resource = tg::Referent::with_node_and_token(
			tg::object::Id::from(id.clone()),
			command.state().tokens().local().cloned(),
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

		Ok(Some(data))
	}

	async fn push_process_command(
		session: &Session,
		command: &tg::Referent<tg::command::Id>,
		location: &tg::Location,
	) -> tg::Result<()> {
		let arg = tg::push::Arg {
			destination: Some(location.clone()),
			nodes: vec![tg::Referent::with_node_and_tokens(
				command.node.clone().into(),
				command.options.tokens.clone(),
			)],
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
			command = %command.node,
			"failed to push the command: expected an output"
		))
	}

	async fn spawn_index_process_task(&self, arg: IndexProcessTaskArg<'_>) -> tg::Result<()> {
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
		crate::checkpoint!(
			self.server,
			"runner.process.index.started",
			process = %id,
		)
		.await;

		// A successful direct read proves node permission on the command.
		command_data.await?;

		let data = data.without_location_and_tokens();
		options.clear_location_and_tokens();
		let command_id = data.command.node.clone();
		let sandbox = data.sandbox.clone();
		let now = self.server.clock.unix_timestamp()?;
		let time_to_live = i64::try_from(self.server.config.object.grant_time_to_live.as_secs())
			.map_err(|error| tg::error!(!error, "failed to convert the grant time to live"))?;
		let expires_at = now + time_to_live;
		let put_process_arg = tangram_index::process::put::Arg {
			cached: false,
			children: None,
			command: command_id.into(),
			data: Some(data.clone()),
			error: None,
			id: id.clone(),
			log: None,
			metadata: tg::process::Metadata::default(),
			options,
			output: None,
			parent: parent.cloned(),
			sandbox: Some(sandbox),
			storage: tangram_index::process::Storage::default(),
			time_to_touch: self.server.config.process.time_to_touch,
			touched_at: now,
		};
		let mut items = vec![tangram_index::batch::Item::PutProcess(put_process_arg)];
		let grant_item = if let Some(parent) = parent {
			let context = crate::Context {
				principal: tg::Principal::Process(parent.clone()),
				token: None,
				..self.context.clone()
			};
			let session = self.server.session(&context);
			let command = command.map(tg::object::Id::from);
			let grant_arg = session
				.create_process_object_grant_arg_with_root_permissions(
					id,
					[command],
					now,
					Some(expires_at),
					tg::authorization::permission::object::Set::NODE,
				)
				.await?;
			tangram_index::batch::Item::PutProcessObjectGrants(grant_arg)
		} else {
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
			tangram_index::batch::Item::PutGrant(grant_arg)
		};
		items.push(grant_item);
		self.server
			.index_batch(tangram_index::batch::Arg { items })
			.await
			.map_err(|error| tg::error!(!error, "failed to index the remote process"))?;

		Ok(())
	}

	async fn run_process(&self, arg: RunProcessArg) -> tg::Result<RunProcessOutput> {
		let RunProcessArg {
			command,
			guest_url,
			process_index,
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

			// Cache the process's children.
			self.checkout_process_artifacts(
				&state.command,
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
			let executable = if let Some(artifact) = &command.executable.artifact {
				let mut path = guest_store_path.join(artifact.to_string());
				if let Some(executable_path) = &command.executable.path {
					path.push(executable_path);
				}
				path
			} else if let Some(path) = &command.executable.path {
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
				command = %state.command,
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
				.get_mut(process_index)
				.expect("the process state was not found")
				.process = Some(sandbox_process.as_ref().clone());

			let arg = WaitForProcessArg {
				process_stopper,
				sandbox: &sandbox,
				sandbox_process: sandbox_process.as_ref(),
				stopper,
			};
			let exit = self.wait_for_process(arg).boxed().await?;

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
		if let Ok(Some(bytes)) = xattr::get(&path, "user.tangram.checksum") {
			let checksum = String::from_utf8(bytes)
				.map_err(|error| tg::error!(!error, "failed to parse the checksum xattr"))
				.and_then(|string| string.parse::<tg::Checksum>())
				.map_err(|error| tg::error!(!error, "failed to parse the checksum string"))?;
			output.checksum = Some(checksum);
		}

		// Try to read the user.tangram.output xattr.
		if let Ok(Some(bytes)) = xattr::get(&path, "user.tangram.output") {
			let tgon = String::from_utf8(bytes)
				.map_err(|error| tg::error!(!error, "failed to decode the output xattr"))?;
			output.value = Some(
				tgon.parse::<tg::Value>()
					.map_err(|error| tg::error!(!error, "failed to parse the output xattr"))?,
			);
		}

		// Try to read the user.tangram.error xattr.
		if let Ok(Some(bytes)) = xattr::get(&path, "user.tangram.error") {
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
		command: &tg::Command,
		sandbox: &tg::sandbox::Id,
		progress: tokio::sync::mpsc::UnboundedSender<Bytes>,
		stderr: &tg::process::Stdio,
	) -> tg::Result<()> {
		// Get the process's command's children that are artifacts.
		let artifacts = command
			.children_with_handle(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the command's children"))?
			.into_iter()
			.filter_map(|object| {
				let id = object.id().try_into().ok()?;
				let artifact = tg::Referent::with_node_and_tokens(id, object.state().tokens());
				Some(artifact)
			})
			.collect::<Vec<tg::Referent<tg::artifact::Id>>>();

		// Track each artifact's verified subtree token for the per-sandbox VFS.
		if self.server.vfs.lock().unwrap().is_some() {
			let permissions =
				tg::authorization::permission::Set::from(tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				));
			let tokens = artifacts.iter().filter_map(|artifact| {
				let token = artifact.options.tokens.local()?.clone();
				let resource = tg::Selector::Id(tg::object::Id::from(artifact.node.clone()).into());
				self.authorize_token(&resource, permissions, &token)
					.then(|| (artifact.node.clone(), token))
			});
			if let Some(mut state) = self.server.runner.state.sandboxes.get_mut_by_id(sandbox) {
				state.tokens.extend(tokens);
			}
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
				Ok(value.to_string())
			},
		})
		.collect::<tg::Result<Vec<_>>>()
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
					tg::Value::try_from_data(value.clone())?.to_string()
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
		let value = tg::Value::try_from_data(value.clone())?.to_string();
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
