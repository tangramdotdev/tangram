use {
	self::control::RunProcessControlTaskArg,
	crate::Session,
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

type CommandFuture = Shared<BoxFuture<'static, tg::Result<tg::command::Data>>>;

pub(super) struct SpawnProcessTaskArg<'a> {
	pub guest_url: &'a tangram_uri::Uri,
	pub location: tg::Location,
	pub process: tg::runner::control::Process,
	pub process_stopper: &'a Stopper,
	pub process_tasks: &'a mut JoinSet<tg::Result<()>>,
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
	retention_stopper: Stopper,
	sandbox: tangram_sandbox::Sandbox,
	sandbox_id_receiver: Option<tokio::sync::oneshot::Receiver<tg::sandbox::Id>>,
	sandbox_stopper: Stopper,
}

struct FinishProcessTaskArg {
	control_task: Task<tg::Result<()>>,
	finish_sender: tokio::sync::oneshot::Sender<tg::process::Data>,
	id: tg::process::Id,
	log_task: Option<Task<tg::Result<()>>>,
	log_write_task: Option<Task<tg::Result<()>>>,
	output: tg::Result<Output>,
	process: tg::Process,
	progress_sender: tokio::sync::mpsc::UnboundedSender<Bytes>,
	sandbox: tg::sandbox::Id,
}

struct CollectProcessOutputArg<'a> {
	exit: u8,
	path: PathBuf,
	state: &'a tg::process::State,
}

pub(super) enum Event {
	Connected(ConnectedEvent),
	Exited,
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
	command: CommandFuture,
	guest_url: tangram_uri::Uri,
	id_receiver: tokio::sync::watch::Receiver<Option<tg::process::Id>>,
	process_stopper: Stopper,
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
			retention_stopper,
			sandbox,
			sandbox_id_receiver,
			sandbox_stopper,
		} = arg;
		let tg::runner::control::Process {
			data,
			id: expected_id,
			parent,
			token: inner_token,
		} = process;
		let mut state = tg::process::State::try_from_data(data)?;
		let process_stopper = Stopper::new();
		let lease = Self::create_process_lease();
		let (control_sender, control_responses) = tokio::sync::mpsc::channel(512);
		let context = match (&expected_id, &inner_token) {
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

		// Register the token before starting the process.
		let (process_id_sender, process_id_receiver) = tokio::sync::watch::channel(None);
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		let token = loop {
			let bytes = rand::random::<[u8; 32]>();
			let token = ENCODING.encode(&bytes);
			match self.server.runner.state.process_tokens.entry(token.clone()) {
				dashmap::mapref::entry::Entry::Occupied(_) => {},
				dashmap::mapref::entry::Entry::Vacant(entry) => {
					entry.insert(process_id_receiver.clone());
					break token;
				},
			}
		};
		let server_for_token_cleanup = self.server.clone();
		let token_for_cleanup = token.clone();
		scopeguard::defer! {
			server_for_token_cleanup.runner.state.process_tokens.remove(&token_for_cleanup);
		}

		// Create a process-principal session after the process is assigned.
		let process_session = {
			let context = self.context.clone();
			let mut process_id_receiver = process_id_receiver.clone();
			let process_stopper = process_stopper.clone();
			let server = self.server.clone();
			async move {
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
				let session = server.session(&context);

				Ok::<_, tg::Error>(session)
			}
			.boxed()
			.shared()
		};

		// Load the command concurrently with the control stream.
		let command = {
			let command = state.command.clone();
			let process_session = process_session.clone();
			async move {
				let session = process_session.await?;
				command
					.data_with_handle(&session)
					.await
					.map_err(|error| tg::error!(!error, "failed to get the command data"))
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
		let log_task = log_sender.map(|log_sender| {
			Task::spawn({
				let log_streams = log_streams.clone();
				let process_stopper = process_stopper.clone();
				let sandbox = sandbox.clone();
				let mut sandbox_process = sandbox_process_receiver.clone();
				move |_| async move {
					let result = async {
						let sandbox_process = loop {
							if let Some(process) = sandbox_process.borrow().clone() {
								break process;
							}
							if sandbox_process.changed().await.is_err() {
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
					if result.is_err() {
						process_stopper.stop();
					}

					result
				}
			})
		});
		let mut run_task = Some(Task::spawn({
			let command = command.clone();
			let guest_url = guest_url.clone();
			let process_id_receiver = process_id_receiver.clone();
			let process_stopper = process_stopper.clone();
			let progress_sender = progress_sender.clone();
			let process_session = process_session.clone();
			let sandbox = sandbox.clone();
			let state = state.clone();
			let stopper = sandbox_stopper.clone();
			let token = token.clone();
			move |_| async move {
				let session = process_session.await?;
				session
					.run_process(RunProcessArg {
						command,
						guest_url,
						id_receiver: process_id_receiver,
						process_stopper,
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
			if expected_id.is_some() && state.sandbox != sandbox_id {
				let error = tg::error!(
					process = ?expected_id,
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

		// Create the control stream.
		let control_responses = ReceiverStream::new(control_responses).map(Ok).boxed();
		let arg = tg::process::control::Arg {
			data: Some(state.to_data()),
			id: expected_id.clone(),
			lease: lease.clone(),
			location: Some(location.clone().into()),
			parent,
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
		if let Some(expected_id) = &expected_id
			&& expected_id != &output.id
		{
			let error = tg::error!(
				actual = %output.id,
				expected = %expected_id,
				"the server returned an invalid process"
			);
			process_stopper.stop();
			run_task.take().unwrap().wait().await.ok();

			return Err(error);
		}
		let id = output.id;
		let Some(inner_token) = output.token.or(inner_token) else {
			process_stopper.stop();
			run_task.take().unwrap().wait().await.ok();

			return Err(tg::error!(%id, "missing the process authentication token"));
		};
		let process = tg::Process::new(
			id.clone(),
			tg::process::Options {
				location: Some(location.clone().into()),
				state: Some(state.clone()),
				..Default::default()
			},
		);
		let context = crate::Context {
			principal: tg::Principal::Process(id.clone()),
			token: Some(inner_token.clone()),
			..self.context.clone()
		};
		let session = self.server.session(&context);
		let Some(mut sandbox_state) = session
			.server
			.runner
			.state
			.sandboxes
			.get_mut_by_id(&sandbox_id)
		else {
			process_stopper.stop();
			run_task.take().unwrap().wait().await.ok();

			return Err(tg::error!(%sandbox_id, "failed to find the sandbox"));
		};
		sandbox_state.processes.insert(
			id.clone(),
			crate::process::State {
				child_leases: Vec::new(),
				control: control_sender.clone(),
				data: state.to_data(),
				finish: None,
				inner_token: inner_token.clone(),
				leases: BTreeSet::from([lease.clone()]),
				process: sandbox_process_receiver.borrow().as_deref().cloned(),
				stopper: process_stopper.clone(),
			},
		);
		drop(sandbox_state);
		session
			.server
			.runner
			.state
			.processes
			.insert(id.clone(), sandbox_id.clone());
		process_id_sender.send_replace(Some(id.clone()));
		if let Some(process) = sandbox_process_receiver.borrow().as_deref().cloned() {
			session
				.server
				.runner
				.state
				.try_update_process(&id, |state| {
					state.process = Some(process);
				});
		}
		let server_for_cleanup = session.server.clone();
		let id_for_cleanup = id.clone();
		scopeguard::defer! {
			server_for_cleanup.runner.state.processes.remove(&id_for_cleanup);
			if let Some(mut sandbox) = server_for_cleanup
				.runner
				.state
				.sandboxes
				.get_mut_by_id(&sandbox_id)
			{
				sandbox.processes.remove(&id_for_cleanup);
			}
		}
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
		if let Err(error) = session
			.spawn_grant_process_command_task(&process, &id, &location)
			.await
		{
			process_stopper.stop();
			run_task.take().unwrap().wait().await.ok();

			return Err(error);
		}
		if location.is_remote() {
			let result = session
				.server
				.messenger
				.publish(
					crate::process::control::connected_subject(&id),
					crate::process::control::Connected {
						lease: lease.clone(),
					},
				)
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
		let stdin_blob = command
			.clone()
			.await
			.ok()
			.and_then(|command| command.stdin.map(tg::Blob::with_id));
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
						stderr_progress,
						stdin,
						stdin_blob,
						stdout,
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
				session
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
		let arg = FinishProcessTaskArg {
			control_task,
			finish_sender,
			id,
			log_task,
			log_write_task,
			output: result,
			process,
			progress_sender,
			sandbox: sandbox_id.clone(),
		};

		session.finish_process_task(arg).boxed().await
	}

	async fn finish_process_task(&self, arg: FinishProcessTaskArg) -> tg::Result<()> {
		let FinishProcessTaskArg {
			control_task,
			finish_sender,
			id,
			log_task,
			log_write_task,
			output: result,
			process,
			progress_sender,
			sandbox: sandbox_id,
		} = arg;
		let session = self;

		let finish = {
			let mut sandbox = session
				.server
				.runner
				.state
				.sandboxes
				.get_mut_by_id(&sandbox_id)
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
			let process = sandbox
				.processes
				.get_mut(&id)
				.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
			process.finish.take()
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

		// Push the output and error if the process is remote.
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
				.write_progress_stream(
					&state.command,
					progress_sender.clone(),
					&state.stderr,
					stream,
				)
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
		let sandbox = session
			.server
			.runner
			.state
			.try_get_process_sandbox(id)
			.ok_or_else(|| tg::error!(%id, "failed to find the process sandbox"))?;
		let mut sandbox = session
			.server
			.runner
			.state
			.sandboxes
			.get_mut_by_id(&sandbox)
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
		let process_state = sandbox
			.processes
			.get_mut(id)
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
		process_state.data.children.get_or_insert_default();
		process_state.data.error = error;
		process_state.data.exit = Some(exit);
		process_state.data.finished_at = Some(self.server.clock.unix_timestamp()?);
		process_state.data.output = value;
		process_state.data.status = tg::process::Status::Finished;
		let child_leases = std::mem::take(&mut process_state.child_leases);
		let data = process_state.data.clone();
		drop(sandbox);

		child_leases
			.into_iter()
			.map(|child_lease| {
				let parent = id.clone();
				let session = session.clone();
				async move {
					let id = child_lease.process;
					crate::checkpoint!(
						session.server,
						"runner.process.child_lease.release",
						child = %id,
						parent = %parent,
					)
					.await;
					let arg = tg::process::cancel::Arg {
						lease: child_lease.lease,
						location: child_lease.location,
					};
					if let Err(error) = session.cancel_process(&id, arg).await {
						tracing::error!(error = %error.trace(), process = %id, "failed to release a child process lease");
					}
				}
			})
			.collect::<futures::stream::FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		finish_sender
			.send(data)
			.map_err(|_| tg::error!(%id, "failed to send the finished process data"))?;
		control_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, %id, "the process control task panicked"))??;

		Ok::<_, tg::Error>(())
	}

	async fn spawn_grant_process_command_task(
		&self,
		process: &tg::Process,
		id: &tg::process::Id,
		location: &tg::Location,
	) -> tg::Result<()> {
		if !location.is_remote() {
			return Ok(());
		}

		let command = process
			.command_with_handle(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the command"))?;

		let now = self.server.clock.unix_timestamp()?;
		let time_to_live = i64::try_from(self.server.config.object.grant_time_to_live.as_secs())
			.map_err(|error| tg::error!(!error, "failed to convert the grant time to live"))?;
		let expires_at = now + time_to_live;
		let subject = tg::authorization::Subject::Process(id.clone());
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let permissions = tg::authorization::permission::Set::from_permission(permission);
		let put_grant = tangram_index::grant::put::Arg {
			created_at: now,
			creator: Some(tg::Principal::Process(id.clone())),
			expires_at: Some(expires_at),
			permissions,
			resource: command.id().into(),
			subject,
			time_to_touch: Some(self.server.config.object.grant_time_to_touch),
		};

		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutGrant(put_grant)],
		};
		self.server
			.index_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to index the process command grant"))?;

		Ok(())
	}

	async fn run_process(&self, arg: RunProcessArg) -> tg::Result<RunProcessOutput> {
		let RunProcessArg {
			command,
			guest_url,
			id_receiver,
			process_stopper,
			progress_sender,
			sandbox,
			sandbox_process_sender,
			state,
			stopper,
			token,
		} = arg;
		let command = command.await?;
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
			.map_err(|error| tg::error!(!error, "failed to cache the children"))?;

			let sandbox_process = sandbox.create_process();
			let guest_artifacts_path = sandbox.guest_artifacts_path();
			let guest_output_path = sandbox.guest_output_path_for_process(&sandbox_process);
			let host_output_path = sandbox.host_output_path_for_process(&sandbox_process);

			// Render the args.
			let args = render_args(&command.args, &guest_artifacts_path, &guest_output_path)?;

			// Get the working directory. On macOS there is no chroot, so "/" is the host root and not writable. Default to the scratch directory instead.
			let cwd = if let Some(cwd) = &command.cwd {
				cwd.clone()
			} else if cfg!(target_os = "macos") {
				sandbox.host_scratch_path()
			} else {
				"/".into()
			};

			// Render the env.
			let mut env = render_env(&command.env, &guest_artifacts_path, &guest_output_path)?;
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
				let mut path = guest_artifacts_path.join(artifact.to_string());
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
			sandbox
				.spawn(
					&sandbox_process,
					tangram_sandbox::SpawnArg {
						command: sandbox_command,
						token: token.clone(),
						tty: state.tty,
						url: guest_url.clone(),
					},
				)
				.await
				.map_err(|error| {
					tg::error!(!error, "failed to spawn the process in the sandbox")
				})?;
			let sandbox_process = Arc::new(sandbox_process);

			// Provide the sandbox process to the control task.
			sandbox_process_sender.send_replace(Some(sandbox_process.clone()));
			if let Some(id) = id_receiver.borrow().clone() {
				self.server.runner.state.try_update_process(&id, |state| {
					state.process = Some(sandbox_process.as_ref().clone());
				});
			}

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
		let arg = tg::cache::Arg { artifacts };
		let stream = self
			.cache(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to cache the artifacts"))?;

		// Write progress.
		self.write_progress_stream(command, progress, stderr, stream)
			.await
			.map_err(|error| tg::error!(!error, "failed to log the progress stream"))?;

		Ok(())
	}
}

fn render_args(
	args: &[tg::command::data::Value],
	artifacts_path: &Path,
	output_path: &Path,
) -> tg::Result<Vec<String>> {
	args.iter()
		.map(|arg| match arg {
			tg::command::data::Value::String(value) => {
				render_value_string(value, artifacts_path, output_path)
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
	artifacts_path: &Path,
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
					render_value_string(value, artifacts_path, output_path)?
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
	artifacts_path: &Path,
	output_path: &Path,
) -> tg::Result<String> {
	match value {
		tg::value::Data::String(string) => Ok(string.clone()),
		tg::value::Data::Object(object) if object.node.is_artifact() => {
			let artifact: tg::artifact::Id = object.node.clone().try_into().unwrap();
			Ok(artifacts_path
				.join(artifact.to_string())
				.to_string_lossy()
				.into_owned())
		},
		tg::value::Data::Template(template) => template.try_render(|component| match component {
			tg::template::data::Component::String(string) => Ok(string.clone().into()),
			tg::template::data::Component::Artifact(artifact) => Ok(artifacts_path
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
