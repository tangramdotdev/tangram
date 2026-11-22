use {
	self::pool::CreateSandboxOutput,
	super::process::{Event as ProcessEvent, SpawnProcessTaskArg, SpawnProcessTaskOutput},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::{self, BoxFuture},
		stream::{BoxStream, FuturesUnordered},
	},
	std::{
		collections::{BTreeMap, BTreeSet},
		pin::pin,
		sync::{
			Arc,
			atomic::{AtomicBool, Ordering},
		},
	},
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
	tangram_server_runner_core::{
		process::ConnectedEvent,
		sandbox::{Boot, Event, ReadyEvent, SpawnSandboxTaskArg, SpawnSandboxTaskOutput, Teardown},
	},
	tangram_server_sandbox_core::control::local::{Message, Reply},
	tokio::task::JoinSet,
	tokio_stream::{StreamMap, wrappers::UnboundedReceiverStream},
	tracing::Instrument as _,
	{
		crate::Component,
		tangram_server_core::{
			Server, Session,
			context::{Context, Origin},
			temp::Temp,
		},
	},
};

mod control;
mod get;
mod listener;
mod path;
#[cfg(test)]
mod tests;

pub(crate) mod pool;

type ConnectionReady = futures::future::Shared<BoxFuture<'static, tg::Result<()>>>;

type PendingProcess = BoxFuture<
	'static,
	(
		tg::Result<ConnectedEvent>,
		tokio::sync::mpsc::UnboundedReceiver<tg::Result<ProcessEvent>>,
		tg::runner::control::Process,
		Option<Reply>,
	),
>;

type SandboxControlSender = tangram_server_core::control::Sender<
	tg::sandbox::control::ServerMessage,
	tg::sandbox::control::ClientMessage,
>;

struct SpawnSandboxArg {
	arg: SpawnSandboxTaskArg,
	boot: Arc<Boot>,
	event_sender: tokio::sync::mpsc::UnboundedSender<tg::Result<Event>>,
	stopper: Stopper,
}

struct SandboxTaskArg {
	allocation: tangram_server_runner_core::capacity::Allocation,
	arg: tg::sandbox::create::Arg,
	boot: Arc<Boot>,
	connected: Arc<AtomicBool>,
	created_at: i64,
	creator: tg::Principal,
	event_sender: tokio::sync::mpsc::UnboundedSender<tg::Result<Event>>,
	id: tg::sandbox::Id,
	kept: Arc<AtomicBool>,
	location: tg::Location,
	owner: tg::Principal,
	process: Option<tg::runner::control::Process>,
	public: bool,
	stopper: Stopper,
	token: Option<String>,
}

/// The output of a boot's task once the boot ended locally, with the boot's control stream, on which the end of the boot is written, and the state that the runner retains after the end.
struct SandboxTaskOutput {
	connected: ConnectionReady,
	control: control::Control,
	ending: Ending,

	/// The guard that discards the boot's state when it is dropped.
	guard: Box<dyn Send>,

	process_tasks: JoinSet<tg::Result<()>>,
	session: Session,
}

/// A boot's task that failed, with the boot's control stream if it is connected, on which the end of the boot records the failure.
struct Failed {
	control: Option<control::Control>,
	error: tg::Error,
}

/// What the end of a boot needs, which the boot's task gets before it runs.
struct EndSandboxBootArg {
	boot: Arc<Boot>,

	/// Whether the boot's control connection was established, after which an index has a record of the sandbox.
	connected: Arc<AtomicBool>,

	create: tg::sandbox::create::Arg,
	created_at: i64,
	creator: tg::Principal,
	event_sender: tokio::sync::mpsc::UnboundedSender<tg::Result<Event>>,

	/// Whether a failed boot's task kept the sandbox's directory, so that the sandbox can start again.
	kept: Arc<AtomicBool>,

	location: tg::Location,
	owner: tg::Principal,
	stopper: Stopper,
	storage: tg::sandbox::Resource<u64>,
}

/// How a boot ended locally, which the end of the boot records: the sandbox's data at the end, the boot's processes, and whether the end stops the sandbox or destroys it.
struct Ending {
	data: tg::sandbox::get::Output,
	processes: Vec<tg::process::Id>,
	teardown: Teardown,
}

/// The end of a boot that the runner wrote: the end's number, whether the boot's control stream recorded it, and the processes that the end finished from the runner's memory.
struct WrittenEnd {
	recorded: bool,
	sequence: u64,
	unfinished: Vec<tg::runner::control::Process>,
}

/// What the data of a sandbox whose boot failed is made from.
struct EndedSandboxDataArg {
	boot: u64,
	create: tg::sandbox::create::Arg,
	creator: tg::Principal,
	error: tg::Either<tg::error::Data, tg::error::Id>,
	id: tg::sandbox::Id,
	location: tg::Location,
	owner: tg::Principal,
	status: tg::sandbox::Status,
}

struct GetSandboxControlStreamArg<'a> {
	boot: u64,
	create: tg::sandbox::create::Arg,
	created_at: i64,
	creator: tg::Principal,
	id: &'a tg::sandbox::Id,
	input_stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	location: &'a tg::Location,
	owner: tg::Principal,
	proof: Option<tg::sandbox::id::Proof>,
	sequence: u64,
}

struct ConnectedSandboxControl {
	requests:
		BoxStream<'static, tg::Result<tg::control::Event<tg::sandbox::control::ServerMessage>>>,
	token: String,
}

struct SandboxTaskInnerArg {
	boot: Arc<Boot>,
	connected: ConnectionReady,
	control: control::Control,
	create_output: CreateSandboxOutput,
	event_sender: tokio::sync::mpsc::UnboundedSender<tg::Result<Event>>,
	id: tg::sandbox::Id,
	kept: Arc<AtomicBool>,
	location: tg::Location,
	process_stopper: Stopper,
	process_task_output: Option<SpawnProcessTaskOutput>,
	process_tasks: JoinSet<tg::Result<()>>,
	processes: Arc<tangram_server_process_core::Processes>,
	state: tg::sandbox::get::Output,
	stopper: Stopper,
}

struct RunSandboxTaskArg {
	boot: Arc<Boot>,
	connected: ConnectionReady,
	control: control::Control,
	event_sender: tokio::sync::mpsc::UnboundedSender<tg::Result<Event>>,
	guest_url: tangram_uri::Uri,
	id: tg::sandbox::Id,
	location: tg::Location,
	process_stopper: Stopper,
	process_task_output: Option<SpawnProcessTaskOutput>,
	process_tasks: JoinSet<tg::Result<()>>,
	processes: Arc<tangram_server_process_core::Processes>,
	sandbox: tangram_sandbox::Sandbox,
	serve_task: Task<()>,
	state: tg::sandbox::get::Output,
	stopper: Stopper,
}

/// The end of a boot's run, with the boot's control stream, how the boot ended, and the process tasks that the runner retains.
struct RunSandboxTaskOutput {
	control: control::Control,
	ending: Ending,
	process_tasks: JoinSet<tg::Result<()>>,
}

struct RetainSandboxTaskArg {
	boot: Arc<Boot>,
	control: Option<control::Control>,
	process_tasks: JoinSet<tg::Result<()>>,
	stopper: Stopper,
}

/// A destroy that arrived while a sandbox was stopping.
struct DestroyRequest {
	error: Option<tg::error::Data>,
}

/// The phase of a boot in which the boot's control stream serves a request.
#[derive(Clone, Copy)]
enum Phase<'a> {
	/// The boot's sandbox is being destroyed, or the boot ended locally.
	Ended,

	/// The boot runs. A boot on the shortcut path is reported once its scheduler acknowledged it.
	Running { reported: bool },

	/// The boot stops, and a destroy that arrives turns the stop into a destroy.
	Stopping {
		destroy: &'a std::sync::Mutex<Option<DestroyRequest>>,
	},
}

/// What serving a request of a boot's control stream leaves for the boot's run loop.
enum Handled {
	/// The request was answered.
	Done,

	/// The request ends the boot's run, which stops the sandbox or destroys it.
	End(Teardown),

	/// The request spawns a process that the boot accepted into its run.
	Spawn(Spawn),
}

/// A spawn that a running boot accepted, which the run loop starts.
struct Spawn {
	process: tg::runner::control::Process,
	public: bool,
	reply: Reply,
}

enum Directory {
	/// The directory of a stopped sandbox, which starts again on its files.
	Existing {
		#[cfg(target_os = "linux")]
		filesystem_project_id: Option<u32>,
		path: std::path::PathBuf,
	},

	/// A new directory for a sandbox's first boot.
	New { path: std::path::PathBuf },
}

impl Component {
	pub(crate) fn resolve_sandbox_backend(
		&self,
		server: &Server,
	) -> tg::Result<tangram_sandbox::Backend> {
		let backend = match &server.config.runner.backend {
			tangram_server_core::config::RunnerBackend::Builtin(_) => {
				tangram_sandbox::Backend::Builtin
			},
			tangram_server_core::config::RunnerBackend::Bubblewrap(config) => {
				tangram_sandbox::Backend::Bubblewrap {
					path: config.path.clone(),
				}
			},
			tangram_server_core::config::RunnerBackend::CloudHypervisor(config) => {
				let image_path = self
					.sandbox_assets
					.image
					.clone()
					.ok_or_else(|| tg::error!("the cloud-hypervisor image is unavailable"))?;
				let snapshot = self.sandbox_assets.snapshot.clone();
				tangram_sandbox::Backend::CloudHypervisor(tangram_sandbox::CloudHypervisor {
					cloud_hypervisor_path: config.cloud_hypervisor_path.clone(),
					dax: config.dax.map(|dax| dax.window_size as u64),
					image_path,
					kernel_path: config.kernel_path.clone(),
					max_cpu: config.max_cpu,
					max_memory: config.max_memory,
					network_interface_timeout: config.network_interface_timeout,
					ready_timeout: config.ready_timeout,
					serial_connect_interval: config.serial_connect_interval,
					serial_connect_timeout: config.serial_connect_timeout,
					snapshot,
					snapshot_cpu: config.snapshot_cpu,
					snapshot_memory: config.snapshot_memory,
				})
			},
			tangram_server_core::config::RunnerBackend::Gvisor(config) => {
				tangram_sandbox::Backend::Gvisor {
					path: config.path.clone(),
					platform: config.platform,
				}
			},
			tangram_server_core::config::RunnerBackend::Seatbelt => {
				tangram_sandbox::Backend::Seatbelt
			},
		};
		if cfg!(target_os = "linux") && matches!(backend, tangram_sandbox::Backend::Seatbelt) {
			return Err(tg::error!("the seatbelt backend requires macos"));
		}
		if cfg!(target_os = "macos") && !matches!(backend, tangram_sandbox::Backend::Seatbelt) {
			return Err(tg::error!("the selected backend requires linux"));
		}
		Ok(backend)
	}

	pub fn spawn_sandbox_task(
		&self,
		server: &Server,
		arg: SpawnSandboxTaskArg,
	) -> SpawnSandboxTaskOutput {
		let (event_sender, event_receiver) = tokio::sync::mpsc::unbounded_channel();

		// Use the sandbox's ID and token from its assignment, or, on the shortcut path, derive the ID and receive the token in the control connection's header.
		let mut arg = arg;
		let (id, token, proof) = match self.create_sandbox_identity(arg.id.take(), arg.token.take())
		{
			Ok(identity) => identity,
			Err(error) => {
				tracing::error!(error = %error.trace(), "the sandbox task failed");
				event_sender.send(Err(error)).ok();
				return SpawnSandboxTaskOutput {
					events: event_receiver,
				};
			},
		};
		arg.id = Some(id.clone());
		arg.token = token;

		// Hold the boot from now on, remembering the boot's initial process, so that the end of the boot finishes it if it does not finish. This server spawned the initial process of a boot on the shortcut path, and the sandbox's scheduler offered the initial process of any other boot.
		let boot = Arc::new(Boot::new(arg.boot, id, proof, arg.scheduled, arg.sequence));
		if let Some(process) = &arg.process {
			boot.accept(process.clone(), !arg.scheduled);
		}
		self.state.boots.insert(boot.clone());

		let task_id = tangram_server_core::control::id();
		let mut task = self.sandbox_tasks.spawn(task_id, |stopper| {
			let component = self.clone();
			let server = server.clone();
			async move {
				// Run the sandbox task.
				let session = server.session(&server.context);
				let arg = SpawnSandboxArg {
					arg,
					boot,
					event_sender,
					stopper,
				};
				component.sandbox_task(&session, arg).boxed().await;
			}
			.in_current_span()
		});
		task.detach();
		SpawnSandboxTaskOutput {
			events: event_receiver,
		}
	}
}

impl Component {
	/// Record that the runner accepted a stop or destroy, so that reads report it before the teardown finishes.
	fn set_sandbox_status(session: &Session, id: &tg::sandbox::Id, status: tg::sandbox::Status) {
		if let Some(mut state) = session
			.server
			.components
			.runner
			.state()
			.sandboxes
			.get_mut_by_id(id)
		{
			state.status = status;
			state.changed.send_replace(());
		}
	}

	/// Reply that the runner does not hold the boot, which asks the request's sender to send the request again.
	async fn send_sandbox_not_holder_response(reply: Reply) -> tg::Result<()> {
		reply
			.send_retry(tg::control::Retry::NotHolder)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the sandbox control response"))
	}

	/// Serve a request of a boot's control stream, which is the only handler of the stream in every phase of the boot. Give up the boot first if the request says that the index records the boot as ended, and reply to every request for a boot that the runner gave up, local or remote, that the runner does not hold the boot, so that the boot neither answers nor acts for the sandbox's next boot.
	async fn serve_control(
		&self,
		session: &Session,
		boot: &Arc<Boot>,
		phase: Phase<'_>,
		message: Message,
	) -> tg::Result<Handled> {
		let id = &boot.sandbox;

		// Give up a boot that the index records as ended, and reply that the runner does not hold a boot that it gave up.
		match &message.arg {
			tg::sandbox::control::ServerRequestArg::Destroy(request) if request.ended => {
				boot.give_up(Teardown::Destroy);
			},
			tg::sandbox::control::ServerRequestArg::Stop(request) if request.ended => {
				boot.give_up(Teardown::Stop);
			},
			_ => {},
		}
		if boot.given_up().is_some() {
			Self::send_sandbox_not_holder_response(message.sender).await?;
			return Ok(Handled::Done);
		}

		// Answer the request.
		let mut handled = Handled::Done;
		let result = match message.arg {
			tg::sandbox::control::ServerRequestArg::Destroy(request) => {
				let status = Self::sandbox_status(session, id);
				match phase {
					// End a running boot with a destroy.
					Phase::Running { .. } => {
						Self::cancel_sandbox_processes(session, id, request.error)?;
						Self::set_sandbox_status(session, id, tg::sandbox::Status::Destroying);
						handled = Handled::End(Teardown::Destroy);
						Ok(tg::sandbox::control::ClientResponseOutput::Destroy(
							tg::sandbox::control::DestroyClientResponseOutput { destroyed: true },
						))
					},

					// Accept a destroy that arrives while the sandbox is stopping, which the teardown carries out before it records the stop.
					Phase::Stopping { destroy }
						if status.is_some_and(|status| !status.is_stopped_or_destroyed()) =>
					{
						Self::cancel_sandbox_processes(session, id, request.error.clone())?;
						let entry = DestroyRequest {
							error: request.error,
						};
						destroy.lock().unwrap().replace(entry);
						Self::set_sandbox_status(session, id, tg::sandbox::Status::Destroying);
						Ok(tg::sandbox::control::ClientResponseOutput::Destroy(
							tg::sandbox::control::DestroyClientResponseOutput { destroyed: true },
						))
					},

					// Reply that the runner does not hold a boot that it stopped, so that the destroy reaches the index, which holds the stopped sandbox.
					Phase::Ended | Phase::Stopping { .. } => {
						if status.is_some_and(|status| status.is_stopped()) {
							Self::send_sandbox_not_holder_response(message.sender).await?;
							return Ok(Handled::Done);
						}
						Ok(tg::sandbox::control::ClientResponseOutput::Destroy(
							tg::sandbox::control::DestroyClientResponseOutput { destroyed: false },
						))
					},
				}
			},
			tg::sandbox::control::ServerRequestArg::Get(_) => session
				.server
				.components
				.runner
				.state()
				.try_get_sandbox(id)
				.map(|data| {
					tg::sandbox::control::ClientResponseOutput::Get(
						tg::sandbox::control::GetClientResponseOutput { data },
					)
				})
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox")),
			tg::sandbox::control::ServerRequestArg::GetProcesses(arg) => session
				.server
				.components
				.runner
				.state()
				.sandboxes
				.get_by_id(id)
				.map(|sandbox| {
					tg::sandbox::control::ClientResponseOutput::GetProcesses(
						sandbox.processes(arg.position, arg.length),
					)
				})
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox")),
			tg::sandbox::control::ServerRequestArg::SpawnProcess(request) => {
				return self
					.serve_spawn(session, boot, phase, message.sender, request)
					.boxed()
					.await;
			},
			tg::sandbox::control::ServerRequestArg::Start(_) => session
				.server
				.components
				.runner
				.state()
				.try_get_sandbox(id)
				.map(|data| {
					tg::sandbox::control::ClientResponseOutput::Start(
						tg::sandbox::control::StartClientResponseOutput { data },
					)
				})
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox")),
			tg::sandbox::control::ServerRequestArg::Stop(_) => match phase {
				// End a running boot with a stop.
				Phase::Running { .. } => {
					Self::cancel_sandbox_processes(session, id, None)?;
					Self::set_sandbox_status(session, id, tg::sandbox::Status::Stopping);
					handled = Handled::End(Teardown::Stop);
					Ok(tg::sandbox::control::ClientResponseOutput::Stop(
						tg::sandbox::control::StopClientResponseOutput { stopped: true },
					))
				},

				// Reply that a boot that is no longer running did not stop now.
				Phase::Ended | Phase::Stopping { .. } => {
					if Self::sandbox_status(session, id)
						.is_some_and(|status| !status.is_destroyed())
					{
						Ok(tg::sandbox::control::ClientResponseOutput::Stop(
							tg::sandbox::control::StopClientResponseOutput { stopped: false },
						))
					} else {
						Err(tg::error!(%id, "the sandbox was destroyed"))
					}
				},
			},
		};
		message
			.sender
			.send(result)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the sandbox control response"))?;

		Ok(handled)
	}

	/// Serve a spawn into a boot. Reply to a server's spawn without a boot that the runner does not hold the sandbox, because the sandbox's scheduler sends the spawn to the runner with the boot, or that the runner is not ready until the scheduler has acknowledged the report of a boot on the shortcut path. Fail a spawn for another boot of the sandbox, which this boot does not number. Accept every other spawn into the boot, so that the boot numbers its finish and its end waits for it: a running boot starts the process, and a boot that is not running finishes it as failed at once, so that the process does not run in another boot.
	async fn serve_spawn(
		&self,
		session: &Session,
		boot: &Arc<Boot>,
		phase: Phase<'_>,
		reply: Reply,
		request: tg::sandbox::control::SpawnProcessServerRequestArg,
	) -> tg::Result<Handled> {
		let id = &boot.sandbox;

		// Reply to a server's spawn without a boot, and fail a spawn for another boot.
		let remote = matches!(reply, Reply::Remote { .. });
		match request.boot {
			None if remote => {
				let retry = if matches!(phase, Phase::Running { reported: false }) {
					tg::control::Retry::NotReady
				} else {
					tg::control::Retry::NotHolder
				};
				reply.send_retry(retry).await.map_err(|error| {
					tg::error!(!error, "failed to send the sandbox control response")
				})?;
				return Ok(Handled::Done);
			},
			Some(requested) if requested != boot.boot => {
				let error = tg::error!(
					code = tg::error::Code::Cancellation,
					%id,
					boot = %boot.boot,
					%requested,
					"the spawn is for another boot of the sandbox"
				);
				let other = Arc::new(Boot::new(requested, id.clone(), None, true, 0));
				self.reply_to_spawn(session, Some(reply), request.process, &other, Err(error))
					.boxed()
					.await?;
				return Ok(Handled::Done);
			},
			None | Some(_) => {},
		}

		// Accept the spawn into a running boot, which starts the process. Refuse a spawn whose process is not in the sandbox.
		if let Phase::Running { .. } = phase {
			let process = match Self::prepare_process(request.process, id) {
				Ok(process) => process,
				Err(error) => {
					reply.send(Err(error)).await.map_err(|error| {
						tg::error!(!error, "failed to send the sandbox control response")
					})?;
					return Ok(Handled::Done);
				},
			};
			boot.accept(process.clone(), request.boot.is_none());
			tangram_server_core::checkpoint!(session.server, "runner.process.accepted", sandbox = %id).await;
			let spawn = Spawn {
				process,
				public: request.public,
				reply,
			};
			return Ok(Handled::Spawn(spawn));
		}

		// Accept the spawn into a boot that is not running, and finish its process as failed. A process that this server spawned is finished on this server, and one that the sandbox's scheduler offered is reported to the scheduler.
		let error = if Self::sandbox_status(session, id).is_none_or(|status| status.is_destroyed())
		{
			tg::error!(
				code = tg::error::Code::Cancellation,
				%id,
				"the sandbox was destroyed"
			)
		} else {
			tg::error!(
				code = tg::error::Code::Cancellation,
				%id,
				"the sandbox is not started"
			)
		};
		boot.accept(request.process.clone(), request.boot.is_none());
		tangram_server_core::checkpoint!(session.server, "runner.process.accepted", sandbox = %id)
			.await;
		self.reply_to_spawn(session, Some(reply), request.process, boot, Err(error))
			.boxed()
			.await?;

		Ok(Handled::Done)
	}

	/// Run a future while serving the requests of a boot's control stream in a phase of the boot that is not its run, until the future completes. A failure to serve a request is logged, and the future still completes.
	async fn serve_control_while<T>(
		&self,
		session: &Session,
		boot: &Arc<Boot>,
		phase: Phase<'_>,
		control: &mut control::Control,
		future: impl Future<Output = T>,
	) -> T {
		let mut future = pin!(future);
		let mut serving = true;
		loop {
			tokio::select! {
				output = &mut future => return output,
				message = control.recv(), if serving => {
					let message = match message {
						Ok(Some(message)) => message,
						Ok(None) => continue,
						Err(error) => {
							tracing::error!(error = %error.trace(), sandbox = %boot.sandbox, "failed to receive a sandbox control message");
							serving = false;
							continue;
						},
					};
					if let Err(error) = self.serve_control(session, boot, phase, message).boxed().await {
						tracing::error!(error = %error.trace(), sandbox = %boot.sandbox, "failed to serve a sandbox control request");
					}
				},
			}
		}
	}

	/// Answer a spawn that the runner accepted into a boot with yes, and finish its process as failed if it cannot run, because from the yes only the runner finishes the process. Every reply to a spawn that the runner accepted goes through this function, and so does the failure of a boot's initial process, which has no spawn to answer. Refuse a spawn whose process has no ID and that cannot run, because no record of it exists.
	async fn reply_to_spawn(
		&self,
		session: &Session,
		reply: Option<Reply>,
		process: tg::runner::control::Process,
		boot: &Arc<Boot>,
		result: tg::Result<ConnectedEvent>,
	) -> tg::Result<()> {
		let output = match result {
			Ok(event) => Ok(tg::sandbox::control::SpawnProcessClientResponseOutput {
				lease: event.lease,
				process: event.process,
			}),
			Err(error) => match process.id.clone() {
				Some(id) => {
					let output = tg::sandbox::control::SpawnProcessClientResponseOutput {
						lease: process.lease,
						process: tg::Referent::with_node(id),
					};
					self.finish_accepted_processes(
						session,
						boot,
						vec![process],
						error.to_data_or_id(),
					);
					Ok(output)
				},
				None => Err(error),
			},
		};
		let Some(reply) = reply else {
			return Ok(());
		};
		let output = output.map(tg::sandbox::control::ClientResponseOutput::SpawnProcess);
		reply
			.send(output)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the sandbox control response"))?;

		Ok(())
	}

	/// Finish as failed with the error processes that the runner accepted and that cannot run, in a sandbox task, which the runner's drain waits for. Begin each process's finish at once, so that the end of the boot waits for it, and record each finish as answered once a server accepts or refuses it. Finish a process that this server spawned on this server, and report the processes that the sandbox's scheduler offered to the scheduler in one report.
	pub(crate) fn finish_accepted_processes(
		&self,
		session: &Session,
		boot: &Arc<Boot>,
		processes: Vec<tg::runner::control::Process>,
		error: tg::Either<tg::error::Data, tg::error::Id>,
	) {
		let (local, offered): (Vec<_>, Vec<_>) = processes
			.into_iter()
			.filter_map(|process| {
				let finish = boot.begin_finish(process.id.as_ref()?);
				Some((process, finish))
			})
			.partition(|(process, _)| boot.local(process));
		let component = self.clone();
		let session = session.clone();
		let boot = boot.clone();
		let task_id = tangram_server_core::control::id();
		let mut task = self.sandbox_tasks.spawn(task_id, move |_| {
			async move {
				for (process, finish) in local {
					if Self::finish_local_process(&session, &boot, process, error.clone()).await {
						finish.answered();
					}
				}
				if offered.is_empty() {
					return;
				}
				let Some(sequence) = boot.next() else {
					return;
				};
				let (processes, finishes): (Vec<_>, Vec<_>) = offered.into_iter().unzip();
				let result = component
					.report_finished_processes_with_runner_control(
						&boot.sandbox,
						boot.boot,
						&processes,
						error,
						sequence,
					)
					.await;
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), sandbox = %boot.sandbox, "failed to report the finished processes");
				}
				for finish in finishes {
					finish.answered();
				}
			}
			.in_current_span()
		});
		task.detach();
	}

	/// Finish as failed with the error a process that this server spawned into a boot and recorded, on this server, numbering the finish with the boot's counter, and write the finish again until this server accepts it. Return whether this server answered the finish, which it does not for a boot that the runner gave up.
	async fn finish_local_process(
		session: &Session,
		boot: &Boot,
		process: tg::runner::control::Process,
		error: tg::Either<tg::error::Data, tg::error::Id>,
	) -> bool {
		let Some(id) = &process.id else {
			return true;
		};
		let data = process.data.without_location_and_tokens_except_sync();
		let Some(sequence) = boot.next() else {
			return false;
		};
		loop {
			let result = session
				.server
				.components
				.process
				.finish_queued_process(&session.server, id, data.clone(), error.clone(), sequence)
				.await;
			let Err(error) = result else {
				return true;
			};
			tracing::error!(error = %error.trace(), process = %id, "failed to finish the process");
			tokio::time::sleep(std::time::Duration::from_secs(1)).await;
		}
	}

	#[tracing::instrument(name = "sandbox.lifecycle", level = "info", skip_all, fields(runner = self.state.id().as_ref().map(ToString::to_string), sandbox = arg.arg.id.as_ref().map(ToString::to_string)))]
	async fn sandbox_task(&self, session: &Session, arg: SpawnSandboxArg) {
		let SpawnSandboxArg {
			arg,
			boot,
			event_sender,
			stopper,
		} = arg;
		let id = boot.sandbox.clone();

		// Run the sandbox task.
		let connected = Arc::new(AtomicBool::new(false));
		let created_at = session.server.clock.unix_timestamp();
		let kept = Arc::new(AtomicBool::new(false));
		let end = EndSandboxBootArg {
			boot: boot.clone(),
			connected: connected.clone(),
			create: arg.arg.clone(),
			created_at,
			creator: arg.creator.clone(),
			event_sender: event_sender.clone(),
			kept: kept.clone(),
			location: arg.location.clone(),
			owner: arg.owner.clone(),
			stopper: stopper.clone(),
			storage: arg.allocation.capacity().storage,
		};
		let task_arg = SandboxTaskArg {
			allocation: arg.allocation,
			arg: arg.arg,
			boot: boot.clone(),
			connected,
			created_at,
			creator: arg.creator,
			event_sender,
			id: id.clone(),
			kept,
			location: arg.location,
			owner: arg.owner,
			process: arg.process,
			public: arg.public,
			stopper,
			token: arg.token,
		};
		let result = if session.server.shutdown.borrow().is_some() {
			Err(Failed::from(tg::error!("the server is shutting down")))
		} else {
			std::panic::AssertUnwindSafe(self.sandbox_task_with_id(session, task_arg).boxed())
				.catch_unwind()
				.await
				.unwrap_or_else(|_| Err(Failed::from(tg::error!("the sandbox task panicked"))))
		};

		// End the boot, and then forget it, which lets a later boot of the sandbox create.
		let retained = match result {
			Ok(output) => {
				let SandboxTaskOutput {
					connected,
					mut control,
					ending,
					guard,
					process_tasks,
					session: retained_session,
				} = output;
				let teardown = ending.teardown;
				self.end_sandbox_boot(session, &end, ending, Some(&mut control), connected)
					.boxed()
					.await;

				// Close the control stream of a stopped sandbox, so that the index serves it and its next boot's control stream is the only one.
				let control = match teardown {
					Teardown::Destroy => Some(control),
					Teardown::Stop => None,
				};
				let retain = RetainSandboxTaskArg {
					boot: boot.clone(),
					control,
					process_tasks,
					stopper: end.stopper.clone(),
				};
				Some((guard, retain, retained_session))
			},
			Err(Failed { control, error }) => {
				let ending = self.failed_sandbox_ending(session, &end, &error).await;
				let mut control = control;
				let connected = future::ok(()).boxed().shared();
				self.end_sandbox_boot(session, &end, ending, control.as_mut(), connected)
					.boxed()
					.await;
				None
			},
		};
		self.state.boots.remove(&id, boot.boot);
		self.state.sandboxes.notify_updated();

		// Retain the ended boot's state and control stream, and then discard them.
		if let Some((guard, retain, session)) = retained {
			if let Err(error) = self.retain_sandbox_task(&session, retain).boxed().await {
				tracing::error!(error = %error.trace(), sandbox = %id, "failed to retain the sandbox");
			}
			drop(guard);
		}
	}

	/// End a boot that the runner held, which is the only function that writes a boot's end and reports it, whether the boot stopped, its sandbox was destroyed, or the boot failed. Write the end while serving the boot's control stream, and then report the end to the runner's scheduler, which records the end itself if the boot's control stream could not. Record and report nothing for a boot that the runner gave up, whose end the index already records, or for a boot on the shortcut path whose control connection was never established, because no index has a record of it.
	async fn end_sandbox_boot(
		&self,
		session: &Session,
		arg: &EndSandboxBootArg,
		ending: Ending,
		control: Option<&mut control::Control>,
		connected: ConnectionReady,
	) {
		let boot = &arg.boot;
		let id = &boot.sandbox;

		// Write the end while serving the boot's control stream, unless the runner gave the boot up or its control connection was never established.
		let write =
			boot.given_up().is_none() && (boot.scheduled || arg.connected.load(Ordering::Acquire));
		let sender = control.as_ref().map(|control| control.sender());
		let end = async {
			let written = if write {
				let control = sender.map(|sender| (sender, connected));
				self.write_sandbox_boot_end(session, arg, &ending, control)
					.await
			} else {
				None
			};
			match ending.teardown {
				Teardown::Destroy => {
					tangram_server_core::checkpoint!(session.server, "runner.sandbox.destroyed", sandbox = %id)
						.await;
					tracing::info!(sandbox = %id, "destroyed the sandbox");
				},
				Teardown::Stop => {
					tangram_server_core::checkpoint!(session.server, "runner.sandbox.stopped", sandbox = %id)
						.await;
					tracing::info!(sandbox = %id, "stopped the sandbox");
				},
			}
			written
		}
		.boxed();
		let written = match control {
			Some(control) => {
				self.serve_control_while(session, boot, Phase::Ended, control, end)
					.await
			},
			None => end.await,
		};
		let Some(WrittenEnd {
			recorded,
			sequence,
			unfinished,
		}) = written
		else {
			arg.event_sender.send(Ok(Event::Destroyed)).ok();
			return;
		};

		// Record the end in this server's local copies of a sandbox whose home is remote.
		let Ending {
			data, processes, ..
		} = ending;
		if let Err(error) = self
			.record_ended_sandbox_local(
				session,
				id,
				&arg.location,
				arg.created_at,
				&data,
				&processes,
				&unfinished,
			)
			.await
		{
			tracing::error!(error = %error.trace(), sandbox = %id, "failed to record the end of the sandbox locally");
		}
		arg.event_sender.send(Ok(Event::Destroyed)).ok();

		// Report the end to the runner's scheduler with the end's number.
		let report = tg::runner::control::EndSandboxClientRequestArg {
			boot: boot.boot,
			created_at: arg.created_at,
			data,
			processes,
			proof: boot.proof.clone(),
			recorded,
			sandbox: id.clone(),
			sequence,
		};
		if let Err(error) = self.report_ended_sandbox_with_runner_control(report).await {
			tracing::error!(error = %error.trace(), sandbox = %id, "failed to report the end of the sandbox");
		}
	}

	/// Write the end of a boot. Finish the boot's unfinished processes from the runner's memory, wait for a server to answer every finish of the boot's processes, finishing again from memory a process whose finish was dropped before a server answered it, take the number of the end, and write the end on the boot's control stream once it is connected, which destroys a destroyed sandbox in the same write. Return the end's number, whether the control stream recorded the end, and the processes that the end finished from memory, or `None` if the runner gave the boot up meanwhile.
	async fn write_sandbox_boot_end(
		&self,
		session: &Session,
		arg: &EndSandboxBootArg,
		ending: &Ending,
		control: Option<(SandboxControlSender, ConnectionReady)>,
	) -> Option<WrittenEnd> {
		let boot = &arg.boot;
		let id = &boot.sandbox;

		// Finish the unfinished processes from the runner's memory, and wait for a server to answer every finish of the boot's processes, until no process is unfinished, unless the runner gives the boot up meanwhile. Then take the number of the end in the same poll, so that no spawn is accepted in between.
		let error = Self::ended_process_error(&ending.data);
		let mut finished = Vec::new();
		loop {
			let unfinished = boot.unfinished();
			if !unfinished.is_empty() {
				self.finish_accepted_processes(session, boot, unfinished.clone(), error.clone());
				finished.extend(unfinished);
			}
			tokio::select! {
				() = boot.wait_finishes() => {},
				_ = boot.wait_given_up() => return None,
			}
			if boot.unfinished().is_empty() {
				break;
			}
		}
		let sequence = boot.end()?;

		// Write the end on the boot's control stream once it is connected.
		let Some((sender, connected)) = control else {
			let written = WrittenEnd {
				recorded: false,
				sequence,
				unfinished: finished,
			};
			return Some(written);
		};
		let result = async {
			connected.await?;
			match ending.teardown {
				Teardown::Destroy => {
					Self::request_sandbox_destroy(
						&sender,
						id,
						ending.data.clone(),
						ending.processes.clone(),
						sequence,
					)
					.await
				},
				Teardown::Stop => {
					Self::request_sandbox_stop(
						&sender,
						id,
						ending.data.clone(),
						ending.processes.clone(),
						sequence,
					)
					.await
				},
			}
		}
		.await;
		if let Err(error) = &result {
			tracing::error!(error = %error.trace(), sandbox = %id, "failed to write the end of the sandbox");
		}

		let written = WrittenEnd {
			recorded: result.is_ok(),
			sequence,
			unfinished: finished,
		};

		Some(written)
	}

	/// Get how a boot whose task failed ended. Its sandbox is stopped if the sandbox's directory remains, so that it can start again, and destroyed otherwise.
	async fn failed_sandbox_ending(
		&self,
		session: &Session,
		arg: &EndSandboxBootArg,
		error: &tg::Error,
	) -> Ending {
		let boot = &arg.boot;
		let id = &boot.sandbox;
		tracing::error!(error = %error.trace(), sandbox = %id, "the sandbox task failed");
		arg.event_sender.send(Err(error.clone())).ok();

		// Keep the sandbox stopped if the failed boot's task kept its directory, or if the directory of a boot after the first remains, such as for a start that fails before it runs, and reserve the storage of a remaining directory again, which the failed boot's allocation released.
		let exists = tokio::fs::try_exists(Self::sandbox_path(session, id))
			.await
			.unwrap_or(false);
		let kept = arg.kept.load(Ordering::Acquire) || (boot.boot > 0 && exists);
		if kept && exists {
			self.reserve_stopped_sandbox_storage(id, arg.storage);
		}

		// Create the failed sandbox's data.
		let (status, teardown) = if kept {
			(tg::sandbox::Status::Stopped, Teardown::Stop)
		} else {
			(tg::sandbox::Status::Destroyed, Teardown::Destroy)
		};
		let entry = EndedSandboxDataArg {
			boot: boot.boot,
			create: arg.create.clone(),
			creator: arg.creator.clone(),
			error: Self::sandbox_error_data(session, error),
			id: id.clone(),
			location: arg.location.clone(),
			owner: arg.owner.clone(),
			status,
		};
		let data = Self::ended_sandbox_data(session, entry);
		let processes = boot.ids();

		Ending {
			data,
			processes,
			teardown,
		}
	}

	fn create_sandbox_identity(
		&self,
		id: Option<tg::sandbox::Id>,
		token: Option<String>,
	) -> tg::Result<(
		tg::sandbox::Id,
		Option<String>,
		Option<tg::sandbox::id::Proof>,
	)> {
		let runner = self
			.state
			.id()
			.ok_or_else(|| tg::error!("missing the runner id"))?;
		let identity = match (id, token) {
			(Some(id), Some(token)) => (id, Some(token), None),
			(None, None) => {
				let (id, proof) = tg::sandbox::Id::with_runner(&runner);
				(id, None, Some(proof))
			},
			_ => {
				return Err(tg::error!(
					"the sandbox id and token must be provided together"
				));
			},
		};
		Ok(identity)
	}

	async fn sandbox_task_with_id(
		&self,
		session: &Session,
		arg: SandboxTaskArg,
	) -> Result<SandboxTaskOutput, Failed> {
		let SandboxTaskArg {
			allocation,
			arg,
			boot: entry,
			connected: connected_flag,
			created_at,
			creator,
			event_sender,
			id,
			kept,
			location,
			owner,
			process,
			public,
			stopper,
			token,
		} = arg;
		let boot = entry.boot;
		let proof = entry.proof.clone();
		#[cfg(target_os = "linux")]
		let mut allocation = allocation;

		// Limit each resource without a maximum to the capacity that the sandbox was allocated, which is its lender's capacity when it borrows.
		let mut arg = arg;
		tangram_server_sandbox_common::limit_sandbox_resources(&mut arg, allocation.capacity());

		let runner = session
			.server
			.components
			.runner
			.state()
			.id()
			.ok_or_else(|| tg::error!("missing the runner id"))?;
		let shortcut = proof.is_some();
		let (token_sender, token_receiver) = tokio::sync::watch::channel(token.clone());
		let context = if let Some(token) = &token {
			Context {
				principal: tg::Principal::Sandbox(id.clone()),
				token: Some(token.clone()),
				..session.context.clone()
			}
		} else {
			Context {
				principal: tg::Principal::Runner(runner),
				token: session.server.config.runner.token.clone(),
				..session.context.clone()
			}
		};
		let connection_session = session.server.session(&context);

		// Create the sandbox concurrently with its control stream, starting a restarted sandbox on its directory after the previous boot's state is gone.
		let create_future = if boot > 0 {
			let component = self.clone();
			let session = session.clone();
			let arg = arg.clone();
			let id = id.clone();
			let kept = kept.clone();
			async move {
				session
					.server
					.components
					.runner
					.state()
					.boots
					.wait_ended_before(&id, boot)
					.await;
				// Create a new sandbox if this runner has no directory for it, because the sandbox never booted here or its directory was lost.
				let path = Self::sandbox_path(&session, &id);
				if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
					return component.create_sandbox_with_pool(&session, arg, &id).await;
				}

				// Report the storage usage of the previous boot's stopped directory until the start, which ends the stopped period, using the previous boot's state while the runner retains it, and its local record otherwise.
				let stopped_at = match component
					.state
					.sandboxes
					.get_by_id(&id)
					.filter(|sandbox| sandbox.boot.boot == boot - 1)
					.map(|sandbox| sandbox.stopped_at)
				{
					Some(stopped_at) => stopped_at,
					None => session
						.server
						.index
						.try_get_sandboxes(std::slice::from_ref(&id))
						.await
						.ok()
						.and_then(|mut indexed| indexed.pop().flatten())
						.and_then(|sandbox| sandbox.data)
						.and_then(|data| data.data.stopped_at),
				};
				if let Some(stopped_at) = stopped_at {
					component.spawn_report_restarted_sandbox_usage(
						&session,
						&id,
						boot - 1,
						stopped_at,
					);
				}

				let directory = Directory::Existing {
					#[cfg(target_os = "linux")]
					filesystem_project_id: component
						.state
						.stopped_project_ids
						.get(&id)
						.map(|entry| entry.value().value()),
					path,
				};

				// Create the sandbox on its directory, which a failure keeps.
				let result = component
					.create_sandbox_inner(&session, arg, Some(directory))
					.await;
				if result.is_err() {
					kept.store(true, Ordering::Release);
				}
				result
			}
			.boxed()
		} else {
			self.create_sandbox_with_pool(session, arg.clone(), &id)
				.boxed()
		};
		let capacity = session.server.config.runner.sandbox_channel_capacity;
		let (input, input_receiver) = tokio::sync::mpsc::channel(capacity);
		let input_stream = tokio_stream::wrappers::ReceiverStream::new(input_receiver)
			.map(Ok)
			.boxed();

		// Connect the control stream in a task, so that the connection proceeds while nothing polls it. The connection's write is the runner's first write of the boot.
		let sequence = entry
			.next()
			.ok_or_else(|| tg::error!(%id, "the runner gave up the boot"))?;
		let connect_task = Task::spawn({
			let component = self.clone();
			let create = arg.clone();
			let creator = creator.clone();
			let id = id.clone();
			let location = location.clone();
			let owner = owner.clone();
			move |_| async move {
				let connect_arg = GetSandboxControlStreamArg {
					boot,
					create,
					created_at,
					creator,
					id: &id,
					input_stream,
					location: &location,
					owner,
					proof,
					sequence,
				};
				let connection = component
					.get_sandbox_control_stream(&connection_session, connect_arg)
					.await?;
				connected_flag.store(true, Ordering::Release);

				// Send the token from the header, so that the sandbox's requests stop claiming the sandbox with the runner's token.
				token_sender.send_replace(Some(connection.token.clone()));

				Ok::<_, tg::Error>(connection)
			}
		});
		let mut connect_future: BoxFuture<'static, tg::Result<ConnectedSandboxControl>> =
			async move {
				connect_task.wait().await.map_err(|error| {
					tg::error!(!error, "the sandbox control connect task panicked")
				})?
			}
			.boxed();
		let mut create_future = pin!(create_future);
		let mut connection = None;
		let create_output = loop {
			tokio::select! {
				result = &mut create_future => break result,
				result = &mut connect_future, if connection.is_none() => {
					connection = Some(result?);
				},
			}
		};

		// Fail the boot if the sandbox cannot be created, with the boot's control stream once it is connected, so that the end of the boot records the failure and the sandbox does not remain started.
		let create_output = match create_output {
			Ok(output) => output,
			Err(error) => {
				tracing::error!(error = %error.trace(), sandbox = %id, "failed to create the sandbox");
				let connection = match connection {
					Some(connection) => Some(connection),
					None if entry.given_up().is_none() => connect_future.await.ok(),
					None => None,
				};
				let control = connection.map(|connection| {
					let (_, local) = tokio::sync::mpsc::channel(1);
					let control = tangram_server_core::control::Stream::new_reconnecting(
						connection.requests,
						input,
						tangram_server_core::control::stream_options(
							&session.server.config.control,
						),
					);
					control::Control::new(control, local)
				});
				return Err(Failed { control, error });
			},
		};
		tracing::Span::current().record("sandbox", tracing::field::display(&id));
		tracing::info!(runner = self.state.id().as_ref().map(ToString::to_string), sandbox = %id, "created the sandbox");
		let process = process
			.map(|process| Self::prepare_process(process, &id))
			.transpose()?;
		let context = Context {
			principal: tg::Principal::Sandbox(id.clone()),
			token: token.clone(),
			..session.context.clone()
		};
		let session = session.server.session(&context);
		let sandbox_create = shortcut.then(|| tg::process::control::SandboxCreate {
			arg: arg.clone(),
			created_at,
			creator: creator.clone(),
			owner: owner.clone(),
			runner: self.state.id(),
		});

		// Take the sandbox's CPU minimum from its lender, and record its CPU share so that its borrowers can take theirs from it.
		#[cfg(target_os = "linux")]
		{
			let result = create_output
				.sandbox
				.set_cpu_lender(allocation.cpu_lender())
				.await;
			if let Err(error) = result {
				tracing::warn!(error = %error.trace(), "failed to set the sandbox CPU lender");
			}
			allocation.set_cpu_share(create_output.sandbox.cpu_share().await);
		}

		// Store the identified sandbox state before starting any processes.
		let capacity = allocation.capacity();
		let allocation = Arc::new(tokio::sync::Mutex::new(Some(allocation)));
		let index = create_output.sandbox.index();
		let processes = Arc::new(tangram_server_process_core::Processes::default());
		let control_channel_capacity = session.server.config.sandbox.control_channel_capacity;
		let (control_sender, control_receiver) =
			tangram_server_sandbox_core::control::local::Local::new(control_channel_capacity);
		let state = tangram_server_runner_core::sandbox::State {
			allocation: Some(allocation),

			authorization_tokens: tg::authorization::Tokens::default(),

			boot: entry.clone(),

			capacity,

			changed: tokio::sync::watch::channel(()).0,

			control_sender,

			create: arg,

			creator,

			error: None,

			expires_at: None,

			id: id.clone(),

			location: location.clone(),

			owner,

			process_ids: indexmap::IndexMap::default(),

			processes: processes.clone(),

			sandbox: Some(create_output.sandbox.clone()),

			status: tg::sandbox::Status::Started,

			stopped_at: None,

			token: token_receiver,

			tokens: BTreeMap::new(),

			usage: None,

			vfs: create_output.vfs.as_ref().map(|vfs| vfs.provider()),
		};
		self.state.sandboxes.insert(index, state);
		let server = session.server.clone();
		let id_for_cleanup = id.clone();
		let guard = std::mem::DropGuard::new((), move |()| {
			drop(server.components.runner.state().sandboxes.remove(index));

			// Let tests observe that the runner discarded the sandbox state.
			if server.checkpoints.is_some() {
				let server = server.clone();
				let id = id_for_cleanup.clone();
				tokio::spawn(async move {
					tangram_server_core::checkpoint!(server, "runner.sandbox.state.removed", sandbox = %id).await;
				});
			}
		});
		tangram_server_core::checkpoint!(session.server, "runner.sandbox.state.inserted", index, sandbox = %id)
			.await;

		// Start no process in a boot that the runner gave up while it was starting.
		let process = process.filter(|_| entry.given_up().is_none());

		// Bind the pooled VFS to this sandbox before starting any processes.
		if let Some(vfs) = &create_output.vfs {
			vfs.provider()
				.set_principal(tg::Principal::Sandbox(id.clone()));
		}

		// Begin accounting before starting any workload processes.
		create_output.sandbox.activate().await?;

		// Spawn the process before waiting for the control stream.
		let mut process_tasks = JoinSet::new();
		let process_stopper = Stopper::new();
		let (sandbox_ready_sender, sandbox_ready_receiver) = tokio::sync::oneshot::channel();
		let process_task_output = process.map(|process| {
			let arg = SpawnProcessTaskArg {
				boot: entry.clone(),
				guest_url: &create_output.guest_url,
				location: location.clone(),
				process,
				process_stopper: &process_stopper,
				process_tasks: &mut process_tasks,
				processes: processes.clone(),
				public,
				retention_stopper: stopper.clone(),
				sandbox: &create_output.sandbox,
				sandbox_create,
				sandbox_ready_receiver: Some(sandbox_ready_receiver),
			};
			self.spawn_process_task(&session, arg)
		});

		// Drive the control stream alongside local requests.
		let (connected_sender, connected_receiver) = tokio::sync::oneshot::channel();

		// Let a process on the shortcut path start without waiting for the control stream, because its start creates the sandbox and the sandbox's control service verifies the runner.
		let mut sandbox_ready_sender = Some(sandbox_ready_sender);
		if shortcut && let Some(sender) = sandbox_ready_sender.take() {
			sender.send(()).ok();
		}
		let requests = futures::stream::once(async move {
			let connection = if let Some(connection) = connection {
				connection
			} else {
				connect_future.await?
			};
			if let Some(sender) = sandbox_ready_sender {
				sender.send(()).ok();
			}
			connected_sender.send(()).ok();
			Ok::<_, tg::Error>(connection.requests)
		})
		.try_flatten()
		.boxed();
		let control = tangram_server_core::control::Stream::new_reconnecting(
			requests,
			input,
			tangram_server_core::control::stream_options(&session.server.config.control),
		);
		let control = control::Control::new(control, control_receiver);
		let connected = async move {
			connected_receiver
				.await
				.map_err(|_| tg::error!("the sandbox failed before connecting"))
		}
		.boxed()
		.shared();
		let state = session
			.server
			.components
			.runner
			.state()
			.sandboxes
			.get(index)
			.expect("the sandbox state was not found")
			.data();
		let arg = SandboxTaskInnerArg {
			boot: entry,
			connected: connected.clone(),
			control,
			create_output,
			event_sender,
			id: id.clone(),
			kept,
			location: location.clone(),
			process_stopper,
			process_task_output,
			process_tasks,
			processes,
			state,
			stopper,
		};
		let RunSandboxTaskOutput {
			control,
			ending,
			process_tasks,
		} = self.sandbox_task_inner(&session, arg).boxed().await?;
		let output = SandboxTaskOutput {
			connected,
			control,
			ending,
			guard: Box::new(guard),
			process_tasks,
			session,
		};

		Ok(output)
	}

	#[tracing::instrument(
		name = "sandbox.acquire",
		level = "debug",
		skip_all,
		err(level = "debug")
	)]
	async fn create_sandbox_with_pool(
		&self,
		session: &Session,
		arg: tg::sandbox::create::Arg,
		id: &tg::sandbox::Id,
	) -> tg::Result<CreateSandboxOutput> {
		// Validate the sandbox's network and mounts before claiming a warm sandbox.
		if session.server.config.runner.backend.harden() {
			validate_hardened_sandbox(&arg)?;
		}
		#[cfg(target_os = "linux")]
		validate_native_mounts(&self.resolve_sandbox_backend(&session.server)?, &arg)?;
		if let Some(task) = self.sandbox_pool.take(&arg, self, session) {
			match task.wait().await {
				Ok(Ok(mut output)) => {
					#[cfg(target_os = "linux")]
					{
						let mut configure = arg.clone();
						if session.server.config.runner.backend.storage_limits() {
							let resources = session
								.server
								.config
								.runner
								.backend
								.resources(&session.server.config.runner.resources);
							let storage = tg::sandbox::Resource {
								max: resources.max_filesystem_size,
								min: 0,
							};
							configure.storage.get_or_insert(storage);
						} else {
							configure.storage = None;
						}
						tangram_server_core::checkpoint!(
							session.server,
							"runner.sandbox.pool.configure",
						)
						.await;
						if let Err(error) = output.sandbox.configure(configure).await {
							tangram_server_core::checkpoint!(
								session.server,
								"runner.sandbox.pool.reject",
							)
							.await;
							tracing::warn!(error = %error.trace(), "failed to configure a pooled sandbox; falling back to cold creation");
							if let Err(error) = self::pool::destroy(output).await {
								tracing::warn!(error = %error.trace(), "failed to destroy a rejected pooled sandbox");
							}
							let path = Self::sandbox_path(session, id);
							let directory = Directory::New { path };
							return self
								.create_sandbox_inner(session, arg, Some(directory))
								.await;
						}
					}

					// Link the sandbox's path to the pooled sandbox's temp and keep the temp, so that a crash or termination leaves the sandbox's files where its next boot finds them.
					let path = Self::sandbox_path(session, id);
					let result = async {
						tokio::fs::create_dir_all(path.parent().unwrap()).await?;
						tokio::fs::symlink(output.temp.path(), &path).await
					}
					.await;
					match result {
						Ok(()) => output.temp.set_preserve(true),
						Err(error) => {
							tracing::warn!(%error, sandbox = %id, "failed to link the sandbox directory");
						},
					}

					tangram_server_core::checkpoint!(
						session.server,
						"runner.sandbox.pool.take",
						index = output.sandbox.index(),
						path = %output.temp.path().display(),
						pid = output.sandbox.pid(),
					)
					.await;
					tracing::debug!(
						index = output.sandbox.index(),
						"claimed a sandbox from the pool"
					);

					return Ok(output);
				},
				Ok(Err(error)) => {
					tracing::warn!(
						error = %error.trace(),
						"failed to claim a sandbox from the pool; falling back to cold creation",
					);
				},
				Err(error) => {
					tracing::warn!(
						?error,
						"the sandbox pool task panicked; falling back to cold creation",
					);
				},
			}
		}

		tracing::debug!("creating a sandbox after a pool miss");

		let path = Self::sandbox_path(session, id);
		let directory = Directory::New { path };
		self.create_sandbox_inner(session, arg, Some(directory))
			.await
	}

	#[tracing::instrument(
		name = "sandbox.create",
		level = "debug",
		skip_all,
		err(level = "debug")
	)]
	async fn create_sandbox_inner(
		&self,
		session: &Session,
		mut arg: tg::sandbox::create::Arg,
		directory: Option<Directory>,
	) -> tg::Result<CreateSandboxOutput> {
		let isolation = self.resolve_sandbox_backend(&session.server)?;
		let prepare = directory.is_none()
			&& matches!(
				isolation,
				tangram_sandbox::Backend::Builtin | tangram_sandbox::Backend::Bubblewrap { .. }
			);

		let mut resources = session
			.server
			.config
			.runner
			.backend
			.resources(&session.server.config.runner.resources);
		if matches!(
			&isolation,
			tangram_sandbox::Backend::Builtin
				| tangram_sandbox::Backend::Bubblewrap { .. }
				| tangram_sandbox::Backend::Gvisor { .. }
		) && session.server.config.runner.backend.harden()
		{
			validate_hardened_sandbox(&arg)?;
		}
		#[cfg(target_os = "linux")]
		validate_native_mounts(&isolation, &arg)?;

		#[cfg(target_os = "linux")]
		self.sandbox_assets
			.ensure_cloud_hypervisor(&session.server, &isolation)
			.await?;

		let rootfs_path = self.sandbox_assets.root.clone();

		// Enforce the sandbox storage maximum only when storage limits are enabled, because enforcement requires a filesystem with project quotas.
		if session.server.config.runner.backend.storage_limits() {
			let storage = tg::sandbox::Resource {
				max: resources.max_filesystem_size,
				min: 0,
			};
			arg.storage.get_or_insert(storage);
		} else {
			arg.storage = None;
		}

		// Apply the sandbox storage maximum.
		if let Some(storage) = arg.storage {
			storage.validate()?;
			if !isolation.uses_linux_namespaces() {
				return Err(tg::error!("storage bounds require container isolation"));
			}
			if let Some(max) = storage.max {
				resources.max_filesystem_size = Some(max);
			}
		}

		// Allocate the filesystem project ID, reusing the one that a restarted sandbox's directory already has, which stays reserved for the directory so that a failed start does not lose it.
		#[cfg(target_os = "linux")]
		let filesystem_project_id = match &isolation {
			backend
				if backend.uses_linux_namespaces()
					&& (resources.max_filesystem_inodes.is_some()
						|| resources.max_filesystem_size.is_some()
						|| arg.storage.is_some()) =>
			{
				let reused = match &directory {
					Some(Directory::Existing {
						filesystem_project_id,
						..
					}) => *filesystem_project_id,
					Some(Directory::New { .. }) | None => None,
				};
				if let Some(value) = reused {
					resources.filesystem_project_id = Some(value);
					None
				} else {
					let project_id = self.state.create_filesystem_project_id()?;
					resources.filesystem_project_id = Some(project_id.value());
					Some(project_id)
				}
			},
			_ => None,
		};

		// Create the temp at the sandbox's directory, keeping an existing directory even if the start fails.
		let mut temp = match &directory {
			Some(Directory::Existing { path, .. }) => {
				let mut temp = Temp::with_path(&session.server, path.clone());
				temp.set_preserve(true);
				temp
			},
			Some(Directory::New { path }) => Temp::with_path(&session.server, path.clone()),
			None => Temp::new(&session.server),
		};
		#[cfg(target_os = "linux")]
		if let Some(project_id) = filesystem_project_id {
			temp.set_filesystem_project_id(project_id);
		}
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|error| tg::error!(!error, "failed to create the temp directory"))?;

		// Create the sandbox index.
		let index = self.state.create_sandbox_index();

		// Start an unbound per-sandbox VFS that denies access until its principal is set.
		#[cfg(target_os = "linux")]
		let (vfs, vfs_task, vfs_mount, fuse_sendfd) = match &isolation {
			tangram_sandbox::Backend::CloudHypervisor(vm) => {
				let socket = temp.path().join("vfs.sock");
				let vfs = session
					.server
					.components
					.vfs
					.start_virtiofs(
						&session.server,
						&socket,
						vm.dax,
						Origin::Sandbox(index),
						None,
					)
					.await
					.map_err(|error| tg::error!(!error, %index, "failed to start the store VFS"))?;
				(Some(vfs), None, None, None)
			},
			tangram_sandbox::Backend::Builtin
			| tangram_sandbox::Backend::Bubblewrap { .. }
			| tangram_sandbox::Backend::Gvisor { .. }
				if session.server.checkouts_enabled() && session.server.config.vfs.is_some() =>
			{
				let mount_path = temp.path().join("store");
				tokio::fs::create_dir_all(&mount_path)
					.await
					.map_err(|error| {
						tg::error!(!error, "failed to create the store mount directory")
					})?;

				if matches!(isolation, tangram_sandbox::Backend::Builtin) {
					// Create the socket pair over which the sandbox sends the mounted FUSE descriptor.
					let (sendfd, recvfd) = rustix::net::socketpair(
						rustix::net::AddressFamily::UNIX,
						rustix::net::SocketType::STREAM,
						rustix::net::SocketFlags::CLOEXEC,
						None,
					)
					.map_err(|error| tg::error!(!error, "failed to create the FUSE socket pair"))?;

					// Start the VFS concurrently, because it blocks until the sandbox mounts the filesystem and sends the descriptor.
					let mut options = session.server.config.vfs.clone().unwrap_or_default();
					options.sqpoll = false;
					let vfs_task = Task::spawn({
						let server = session.server.clone();
						let mount_path = mount_path.clone();
						move |_| {
							async move {
								let arg = tangram_server_core::vfs::MountArg {
									kind: tangram_server_core::vfs::Kind::Fuse,
									options,
									origin: Origin::Sandbox(index),
									path: &mount_path,
									principal: None,
									recvfd: Some(recvfd),
								};
								server.components.vfs.mount(&server, arg).await
							}
							.in_current_span()
						}
					});

					(None, Some(vfs_task), Some(mount_path), Some(sendfd))
				} else {
					let mut options = session.server.config.vfs.clone().unwrap_or_default();
					options.sqpoll = false;
					let arg = tangram_server_core::vfs::MountArg {
						kind: tangram_server_core::vfs::Kind::Fuse,
						options,
						origin: Origin::Sandbox(index),
						path: &mount_path,
						principal: None,
						recvfd: None,
					};
					let vfs = session
						.server
						.components
						.vfs
						.mount(&session.server, arg)
						.await
						.map_err(|error| tg::error!(!error, "failed to start the store VFS"))?;
					(Some(vfs), None, Some(mount_path), None)
				}
			},
			_ => (None, None, None, None),
		};
		#[cfg(target_os = "macos")]
		let (vfs, vfs_mount) = if session.server.components.vfs.mounted()
			&& session.server.config.vfs.as_ref().is_some_and(|config| {
				matches!(config.kind, tangram_server_core::config::VfsKind::Fskit)
			}) {
			let mount_path = temp.path().join("store");
			let arg = tangram_server_core::vfs::MountArg {
				kind: tangram_server_core::vfs::Kind::Fskit,
				options: session.server.config.vfs.clone().unwrap_or_default(),
				origin: Origin::Sandbox(index),
				path: &mount_path,
				principal: None,
				recvfd: None,
			};
			let vfs = session
				.server
				.components
				.vfs
				.mount(&session.server, arg)
				.await?;
			(Some(vfs), Some(mount_path))
		} else {
			(None, None)
		};

		// Create the listener.
		let listener_port = match &session.server.config.runner.backend {
			tangram_server_core::config::RunnerBackend::CloudHypervisor(config) => {
				Some(config.listener_port)
			},
			_ => None,
		};
		let (listener, guest_url, tangram_socket_path) =
			Self::run_create_listener(temp.path(), &isolation, &resources, listener_port)
				.await
				.map_err(
					|error| tg::error!(!error, %index, "failed to create the sandbox listener"),
				)?;

		// Create the sandbox with a readonly store mount.
		let store_path = session.server.store_path();
		let store_source = vfs_mount.clone().unwrap_or_else(|| store_path.clone());
		#[cfg(target_os = "macos")]
		let store_path = store_source.clone();
		let mut mounts = arg.mounts;
		mounts.push(tg::sandbox::Mount {
			readonly: true,
			source: store_source.clone(),
			target: store_path.clone(),
		});
		let network = match arg.network {
			None => None,
			Some(tg::sandbox::Network::Default) => Some(tangram_sandbox::Network::Default),
			Some(tg::sandbox::Network::Bridge(bridge)) => {
				Some(tangram_sandbox::Network::Bridge(tangram_sandbox::Bridge {
					ports: bridge.ports,
				}))
			},
			Some(tg::sandbox::Network::Host) => Some(tangram_sandbox::Network::Host),
		};
		let config = &session.server.config.sandbox;
		let arg = tangram_sandbox::Arg {
			backend: isolation,

			#[cfg(target_os = "linux")]
			cgroup_cleanup_interval: config.cgroup_cleanup_interval,

			#[cfg(target_os = "linux")]
			cgroup_cleanup_timeout: config.cgroup_cleanup_timeout,

			connect_timeout: config.connect_timeout,

			cpu: arg.cpu,

			#[cfg(target_os = "linux")]
			cpu_affinity: session.server.config.runner.cpu_affinity,

			#[cfg(target_os = "linux")]
			cpu_shares: Some(self.state.cpu_shares.clone()),

			#[cfg(target_os = "linux")]
			cpu_threads: Some(self.state.cpu_threads.clone()),

			dns: config.network.dns.clone(),

			#[cfg(target_os = "linux")]
			firewall: match config.network.firewall {
				tangram_server_core::config::SandboxNetworkFirewall::Iptables => {
					tangram_sandbox::network::Firewall::Iptables
				},
				tangram_server_core::config::SandboxNetworkFirewall::Nft => {
					tangram_sandbox::network::Firewall::Nft
				},
			},

			#[cfg(target_os = "linux")]
			fuse_fd: fuse_sendfd.map(Arc::new),

			hostname: arg.hostname,

			http2: config.http2,

			identity: session.server.path.clone(),

			index,

			#[cfg(target_os = "linux")]
			ip_pool: self.ip_pool.clone(),

			memory: arg.memory,

			memory_sampling_interval: session.server.config.runner.memory_sampling_interval,

			mounts,

			network,

			#[cfg(target_os = "linux")]
			network_startup_timeout: config.network.startup_timeout,

			nice: config.nice,

			path: temp.path().to_owned(),

			prepare,

			request_timeout: config.request_timeout,

			resources,

			rootfs_path,

			shutdown_timeout: config.shutdown_timeout,

			storage: arg.storage,

			#[cfg(target_os = "linux")]
			storage_pool: Some(self.state.storage_pool.clone()),

			store_path: store_source,

			tangram_path: session.server.tangram_path.clone(),

			tangram_socket_path,
		};
		tracing::debug!(sandbox_index = index, "creating the physical sandbox");
		let sandbox = tangram_sandbox::Sandbox::start(arg)
			.await
			.map_err(|error| tg::error!(!error, %index, "failed to create the sandbox"))?;

		// Wait for the per-sandbox VFS, which finishes starting once the sandbox mounts the filesystem and sends the FUSE descriptor.
		#[cfg(target_os = "linux")]
		let vfs = match vfs_task {
			None => vfs,
			Some(vfs_task) => {
				let vfs = vfs_task
					.wait()
					.await
					.map_err(|error| tg::error!(!error, "the VFS startup task panicked"))?
					.map_err(|error| tg::error!(!error, %index, "failed to start the store VFS"))?;
				Some(vfs)
			},
		};

		// Spawn the serve task.
		let serve_task = Task::spawn({
			let server = session.server.clone();
			let listener_config = tangram_server_core::config::HttpListener {
				tls: None,
				url: guest_url.clone(),
			};
			move |stopper| {
				async move {
					server
						.serve(listener, listener_config, Origin::Sandbox(index), stopper)
						.await;
				}
				.in_current_span()
			}
		});

		// Keep a new directory once the sandbox is created, so that a crash or termination leaves its files where its next boot finds them.
		if let Some(Directory::New { .. }) = directory {
			temp.set_preserve(true);
		}

		let output = CreateSandboxOutput {
			guest_url,

			sandbox,

			serve_task,

			temp,

			vfs,
		};

		Ok(output)
	}

	async fn sandbox_task_inner(
		&self,
		session: &Session,
		arg: SandboxTaskInnerArg,
	) -> tg::Result<RunSandboxTaskOutput> {
		let SandboxTaskInnerArg {
			boot,
			connected,
			control,
			create_output,
			event_sender,
			id,
			kept,
			location,
			process_stopper,
			process_task_output,
			process_tasks,
			processes,
			state,
			stopper,
		} = arg;
		let CreateSandboxOutput {
			guest_url,
			sandbox,
			serve_task,
			temp,
			mut vfs,
		} = create_output;

		let arg = RunSandboxTaskArg {
			boot,
			connected,
			control,
			event_sender,
			guest_url,
			id: id.clone(),
			location,
			process_stopper,
			process_task_output,
			process_tasks,
			processes,
			sandbox,
			serve_task,
			state,
			stopper,
		};
		let mut temp = Some(temp);
		let result = self
			.run_sandbox_task(session, arg, &mut temp, &mut vfs)
			.boxed()
			.await;

		// Stop the VFS after an early sandbox task failure.
		if let Some(vfs) = vfs {
			vfs.stop();
			vfs.wait().await;
		}

		// Keep the directory of a restarted sandbox or of a sandbox whose stop failed, so that it can start again, and remove the directory after any other early failure, which ends the sandbox without a stop or destroy.
		let keep = self.state.sandboxes.get_by_id(&id).is_some_and(|sandbox| {
			sandbox.boot.boot > 0 || sandbox.status == tg::sandbox::Status::Stopping
		});
		if let Some(temp) = temp {
			let path = Self::sandbox_path(session, &id);
			if keep {
				match temp.persist(&path).await {
					Ok(project_id) => {
						#[cfg(target_os = "linux")]
						if let Some(project_id) = project_id {
							self.state
								.stopped_project_ids
								.insert(id.clone(), project_id);
						}
						#[cfg(not(target_os = "linux"))]
						let _ = project_id;
						kept.store(true, Ordering::Release);
					},
					Err(error) => {
						tracing::error!(%error, sandbox = %id, "failed to keep the sandbox directory");
					},
				}
			} else {
				Self::remove_sandbox_directory(path, id.clone(), temp).await;
			}
		}

		result
	}

	#[tracing::instrument(name = "sandbox.run", level = "debug", skip_all, fields(sandbox = %arg.id), err(level = "debug"))]
	async fn run_sandbox_task(
		&self,
		session: &Session,
		arg: RunSandboxTaskArg,
		temp: &mut Option<Temp>,
		vfs: &mut Option<Box<dyn tangram_server_vfs_core::Server>>,
	) -> tg::Result<RunSandboxTaskOutput> {
		let RunSandboxTaskArg {
			boot: entry,
			connected,
			mut control,
			event_sender,
			guest_url,
			id,
			location,
			process_stopper,
			process_task_output,
			mut process_tasks,
			processes,
			sandbox,
			serve_task,
			state,
			stopper,
		} = arg;
		let scheduled = entry.scheduled;
		let process_stopper = std::mem::DropGuard::new(process_stopper, |stopper| stopper.stop());

		// Report a sandbox that this runner started on the shortcut path to the runner's scheduler in a task, so that the scheduler knows the sandbox and sends the spawns into it, and nothing on the shortcut's path waits for the report.
		let reported = Arc::new(AtomicBool::new(scheduled));
		let _report_task = entry.proof.clone().filter(|_| !scheduled).map(|proof| {
			let boot = state.data.boot;
			let component = self.clone();
			let id = id.clone();
			let reported = reported.clone();
			let stopper = stopper.clone();
			Task::spawn(move |_| async move {
				match component
					.report_shortcut_sandbox_with_runner_control(&id, boot, proof, &stopper)
					.await
				{
					Ok(()) => reported.store(true, Ordering::Release),
					Err(error) => {
						tracing::error!(error = %error.trace(), sandbox = %id, "failed to report the sandbox to the scheduler");
					},
				}
			})
		});

		// Create the process events.
		let mut process_events = StreamMap::new();

		// Create the idle timer, which stops or destroys the sandbox once it has no processes for its time to stop or ttl.
		let mut timer_future: Option<BoxFuture<'static, Teardown>> = None;
		let initial_process = process_task_output.is_some();
		let time_to_live = state.data.time_to_live;
		let time_to_stop = state.data.time_to_stop;

		// Observe process initialization without blocking either control transport.
		let mut pending = FuturesUnordered::new();
		if let Some(output) = process_task_output {
			pending.push(wait_for_process_control_connection(output, None));
		}
		let mut ready = false;
		let mut ready_reported = false;
		let mut ready_connection = connected.clone();

		// Wait for the sandbox process to exit.
		let mut sandbox_wait = {
			let sandbox = sandbox.clone();
			async move { sandbox.wait().await }.boxed()
		};
		let mut failure = None;
		let mut teardown = Teardown::Destroy;
		let mut stopping = false;

		// Track the sandbox's last use, which is its start and then each time it is left without processes, so that a stop records when its ttl expires.
		let mut last_used_at = session.server.clock.unix_timestamp();

		// Run the sandbox until it should be stopped or destroyed, destroying it with the error if the loop fails.
		let result = async {
			loop {
				// Hit a checkpoint the first time that the sandbox is ready.
				if ready && !ready_reported {
					ready_reported = true;
					tangram_server_core::checkpoint!(session.server, "runner.sandbox.ready", sandbox = %id).await;
				}

				let current_timer_future = timer_future.as_mut().map_or_else(
					|| future::pending().left_future(),
					|timer_future| timer_future.as_mut().right_future(),
				);

				tokio::select! {
					result = &mut ready_connection, if !initial_process && !ready => {
						result?;
						if pending.is_empty() && process_events.is_empty() {
							timer_future = idle_timer(&session.server, time_to_live, time_to_stop);
						}
						let event = ReadyEvent { connected_event: None, sandbox: id.clone() };
						event_sender.send(Ok(Event::Ready(event))).ok();
						tracing::info!(sandbox = %id, "the sandbox is ready");
						ready = true;
					},
					output = pending.next(), if !pending.is_empty() => {
						let (result, events, process, reply) = output.unwrap();
						let connected_event = match result {
							Ok(event) => event,
							Err(error) => {
								self.reply_to_spawn(session, reply, process, &entry, Err(error.clone())).boxed().await?;
								tracing::error!(error = %error.trace(), sandbox = %id, "failed to start a sandbox process");
								if !ready {
									event_sender.send(Err(error)).ok();
								}
								break;
							},
						};
						process_events.insert(connected_event.process.node.clone(), UnboundedReceiverStream::new(events));
						if let Some(reply) = reply {
							self.reply_to_spawn(session, Some(reply), process, &entry, Ok(connected_event)).boxed().await?;
						} else {
							let event = ReadyEvent { connected_event: Some(connected_event), sandbox: id.clone() };
							event_sender.send(Ok(Event::Ready(event))).ok();
							tracing::info!(sandbox = %id, "the sandbox is ready");
							ready = true;
						}
					},
					message = control.recv() => {
						// Get the message.
						let message = message
							.map_err(|error| tg::error!(!error, %id, "failed to receive a sandbox control message"))?;
						let Some(message) = message else {
							break;
						};

						// Serve the message, and start a process that the boot accepted, or end the boot's run.
						let phase = Phase::Running { reported: reported.load(Ordering::Acquire) };
						match self.serve_control(session, &entry, phase, message).boxed().await? {
							Handled::Done => {},
							Handled::End(end) => {
								teardown = end;
								break;
							},
							Handled::Spawn(Spawn { process, public, reply }) => {
								timer_future.take();
								let arg = SpawnProcessTaskArg {
									boot: entry.clone(),
									guest_url: &guest_url,
									location: location.clone(),
									process,
									process_stopper: &process_stopper,
									process_tasks: &mut process_tasks,
									processes: processes.clone(),
									public,
									retention_stopper: stopper.clone(),
									sandbox: &sandbox,
									sandbox_create: None,
									sandbox_ready_receiver: None,
								};
								let task = self.spawn_process_task(session, arg);
								pending.push(wait_for_process_control_connection(task, Some(reply)));
							},
						}
					},

					// If the runner gives up the boot because its scheduler ended it, then stop the sandbox, or destroy it if the scheduler does not keep its directory, writing nothing more for the boot.
					kind = entry.wait_given_up() => {
						let status = match kind {
							Teardown::Destroy => tg::sandbox::Status::Destroying,
							Teardown::Stop => tg::sandbox::Status::Stopping,
						};
						Self::cancel_sandbox_processes(session, &id, None)?;
						Self::set_sandbox_status(session, &id, status);
						teardown = kind;
						break;
					},

					// If the runner is shutting down gracefully, then stop the sandbox, or destroy it if its ttl is zero, once its processes finish.
					() = stopper.wait(), if !stopping => {
						stopping = true;
						teardown = Teardown::with_time_to_live(time_to_live);
						if pending.is_empty() && process_events.is_empty() {
							break;
						}
					},

					// Handle an underlying process event.
					event = process_events.next(), if !process_events.is_empty() => {
						let Some((process, event)) = event else {
							break;
						};
						match event? {
							ProcessEvent::Buffered | ProcessEvent::Released => {
								process_events.remove(&process);
								if process_events.is_empty() && pending.is_empty() {
									last_used_at = session.server.clock.unix_timestamp();
									if stopping {
										break;
									}
									if ready {
										timer_future = idle_timer(&session.server, time_to_live, time_to_stop);
									}
								}
							},
							ProcessEvent::Connected(_) => {
								return Err(tg::error!(%process, "received a duplicate process connected event"));
							},
							ProcessEvent::Exited => {},
						}
					},

					// Reap a process task after its retained state expires.
					output = process_tasks.join_next(), if !process_tasks.is_empty() => {
						let result = output
							.unwrap()
							.map_err(|error| tg::error!(!error, "a process task panicked"))?
							.map_err(|error| tg::error!(!error, "a process task failed"));
						if let Err(error) = result {
							tracing::error!(error = %error.trace(), sandbox = %id, "a sandbox process failed");
							if !ready {
								event_sender.send(Err(error)).ok();
							}
							break;
						}
					},

					// If the sandbox process exits, then break and destroy the sandbox with the error that describes its exit.
					result = &mut sandbox_wait => {
						if let Err(error) = result {
							tracing::error!(error = %error.trace(), sandbox = %id, "the sandbox process exited");
							failure = Some(error);
						}
						break;
					},

					// If the idle timer fires, then break and stop or destroy the sandbox.
					timer_teardown = current_timer_future => {
						teardown = timer_teardown;
						break;
					},
				}
			}

			Ok::<_, tg::Error>(())
		}
		.boxed()
		.await;
		if let Err(error) = result {
			tracing::error!(error = %error.trace(), sandbox = %id, "the sandbox failed");
			failure.get_or_insert(error);
		}
		if failure.is_some() {
			teardown = Teardown::Destroy;
		}

		// Record a destroy that arrives during a stop, with its error, which turns the stop into a destroy, as does giving up the boot for a sandbox whose directory the scheduler does not keep.
		let destroy_request = std::sync::Mutex::new(None::<DestroyRequest>);
		let destroyed = || {
			destroy_request.lock().unwrap().is_some()
				|| matches!(entry.given_up(), Some(Teardown::Destroy))
		};

		let destroy = async {
			let mut teardown = teardown;
			match teardown {
				Teardown::Destroy => tracing::info!(sandbox = %id, "destroying the sandbox"),
				Teardown::Stop => {
					tracing::info!(sandbox = %id, "stopping the sandbox");
					let mut state = session
						.server
						.components
						.runner
						.state()
						.sandboxes
						.get_mut_by_id(&id)
						.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;

					// Record when the stopped sandbox expires, counting a stop with running processes as a use.
					if !process_events.is_empty() || !pending.is_empty() {
						last_used_at = session.server.clock.unix_timestamp();
					}
					state.stopped_at = Some(session.server.clock.unix_timestamp());
					state.expires_at = time_to_live.map(|time_to_live| {
						last_used_at
							.saturating_add(time_to_live.as_secs().try_into().unwrap_or(i64::MAX))
					});
					state.status = tg::sandbox::Status::Stopping;
					state.changed.send_replace(());
				},
			}

			// Stop the underlying processes and await their initialization.
			process_stopper.stop();
			while let Some((result, events, process, reply)) = pending.next().await {
				let result =
					result.map_err(|source| tg::error!(!source, %id, "the sandbox was destroyed"));
				if let Ok(event) = &result {
					process_events.insert(
						event.process.node.clone(),
						UnboundedReceiverStream::new(events),
					);
				}
				self.reply_to_spawn(session, reply, process, &entry, result)
					.boxed()
					.await?;
			}

			let allocation = {
				let mut state = session
					.server
					.components
					.runner
					.state()
					.sandboxes
					.get_mut_by_id(&id)
					.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
				state
					.allocation
					.take()
					.ok_or_else(|| tg::error!(%id, "failed to find the sandbox allocation"))?
			};

			// Stop the VFS while the sandbox still owns its mount namespace.
			if let Some(vfs) = vfs.take() {
				vfs.stop();
				vfs.wait().await;
			}

			// Shut down the sandbox server so descendants in other process groups cannot hold its output streams open.
			sandbox.shutdown().await.map_err(
				|error| tg::error!(!error, %id, "failed to shut down the sandbox process"),
			)?;
			tangram_server_core::checkpoint!(session.server, "runner.sandbox.shutdown", sandbox = %id)
				.await;

			// Await the process output while retaining the sandbox's resources for termination error reporting.
			while let Some((process, event)) = process_events.next().await {
				match event? {
					ProcessEvent::Buffered | ProcessEvent::Released => {
						process_events.remove(&process);
					},
					ProcessEvent::Connected(_) => {
						return Err(
							tg::error!(%process, "received a duplicate process connected event"),
						);
					},
					ProcessEvent::Exited => {},
				}
			}

			// Destroy the sandbox instead of stopping it if a destroy arrived during the stop.
			if matches!(teardown, Teardown::Stop) && destroyed() {
				tracing::info!(sandbox = %id, "destroying the stopping sandbox");
				teardown = Teardown::Destroy;
				let mut state = session
					.server
					.components
					.runner
					.state()
					.sandboxes
					.get_mut_by_id(&id)
					.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
				state.expires_at = None;
				state.status = tg::sandbox::Status::Destroying;
				state.stopped_at = None;
				state.changed.send_replace(());
			}

			// Stop or destroy the sandbox while retaining its process and control state.
			let output = match teardown {
				Teardown::Destroy => sandbox.destroy().await.map_err(
					|error| tg::error!(!error, %id, "failed to destroy the sandbox process"),
				)?,
				Teardown::Stop => sandbox.stop().await.map_err(
					|error| tg::error!(!error, %id, "failed to stop the sandbox process"),
				)?,
			};

			let usage = output.usage;

			// Release the sandbox's capacity after its cgroup is stopped and accounted, keeping a stopped sandbox's storage reserved for its directory.
			tangram_server_core::checkpoint!(
				session.server,
				"runner.sandbox.capacity.release",
				sandbox = %id,
			)
			.await;
			let released = allocation.lock().await.take();
			let stopped_storage = match (teardown, released) {
				(Teardown::Stop, Some(released)) => {
					let storage = released.into_storage(&self.state.capacity);
					if storage.is_none() {
						tracing::warn!(sandbox = %id, "failed to reserve the stopped sandbox's storage");
					}
					storage
				},
				(Teardown::Destroy | Teardown::Stop, _) => None,
			};
			let mut state = session
				.server
				.components
				.runner
				.state()
				.sandboxes
				.get_mut_by_id(&id)
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
			state.usage = usage;
			drop(state);

			// Stop and await the serve task.
			serve_task.stop();
			serve_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the serve task panicked"))?;

			// Remove the directory of a destroyed sandbox in the background, releasing the filesystem project ID that its directory kept across boots, or keep the directory of a stopped sandbox, so that it can start again on its data.
			if let Some(temp) = temp.take() {
				let path = Self::sandbox_path(session, &id);
				match teardown {
					Teardown::Destroy => {
						#[cfg(target_os = "linux")]
						let temp = {
							let mut temp = temp;
							if let Some((_, project_id)) =
								self.state.stopped_project_ids.remove(&id)
							{
								temp.set_filesystem_project_id(project_id);
							}
							temp
						};
						tokio::spawn(Self::remove_sandbox_directory(path, id.clone(), temp));
					},
					Teardown::Stop => {
						let project_id = temp.persist(&path).await.map_err(
							|error| tg::error!(!error, %id, "failed to keep the sandbox directory"),
						)?;
						#[cfg(target_os = "linux")]
						if let Some(project_id) = project_id {
							self.state
								.stopped_project_ids
								.insert(id.clone(), project_id);
						}
						#[cfg(not(target_os = "linux"))]
						let _ = project_id;

						// Keep the storage reserved for the kept directory, unless a newer boot of the sandbox already acquired it.
						let newer = self
							.state
							.boots
							.latest(&id)
							.is_some_and(|latest| latest.boot > entry.boot);
						if let Some(storage) = stopped_storage
							&& !newer
						{
							self.state.stopped_storage.insert(id.clone(), storage);
						}
					},
				}
			}

			// Destroy the stopped sandbox if a destroy arrived after the sandbox was stopped, before the stop is recorded.
			if matches!(teardown, Teardown::Stop) && destroyed() {
				tracing::info!(sandbox = %id, "destroying the stopped sandbox");
				teardown = Teardown::Destroy;
				self.remove_stopped_sandbox_directory(session, &id).await?;
			}

			let (data, processes) = {
				let mut state = session
					.server
					.components
					.runner
					.state()
					.sandboxes
					.get_mut_by_id(&id)
					.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
				state.status = match teardown {
					Teardown::Destroy => tg::sandbox::Status::Destroyed,
					Teardown::Stop => tg::sandbox::Status::Stopped,
				};
				let destroy_error = destroy_request
					.lock()
					.unwrap()
					.as_ref()
					.and_then(|request| request.error.clone())
					.map(tg::Either::Left);
				state.error = failure
					.as_ref()
					.map(|error| Self::sandbox_error_data(session, error))
					.or(destroy_error);
				state.changed.send_replace(());
				state.sandbox.take();
				(
					state.data(),
					state.process_ids.keys().cloned().collect::<Vec<_>>(),
				)
			};
			drop(sandbox);

			Ok::<_, tg::Error>(Ending {
				data,
				processes,
				teardown,
			})
		};
		let destroy = destroy
			.instrument(tracing::debug_span!("sandbox.destroy", sandbox = %id))
			.boxed();

		// Tear the boot down while serving its control stream.
		let phase = match teardown {
			Teardown::Destroy => Phase::Ended,
			Teardown::Stop => Phase::Stopping {
				destroy: &destroy_request,
			},
		};
		let ending = self
			.serve_control_while(session, &entry, phase, &mut control, destroy)
			.await?;
		let output = RunSandboxTaskOutput {
			control,
			ending,
			process_tasks,
		};

		Ok(output)
	}

	#[tracing::instrument(name = "sandbox.retain", level = "debug", skip_all, fields(sandbox = %arg.boot.sandbox), err(level = "debug"))]
	async fn retain_sandbox_task(
		&self,
		session: &Session,
		arg: RetainSandboxTaskArg,
	) -> tg::Result<()> {
		let RetainSandboxTaskArg {
			boot,
			control,
			mut process_tasks,
			stopper,
		} = arg;

		// Await the process tasks while they retain their state and control streams.
		let tasks = async {
			while let Some(result) = process_tasks.join_next().await {
				result
					.map_err(|error| tg::error!(!error, "a process task panicked"))?
					.map_err(|error| tg::error!(!error, "a process task failed"))?;
			}
			Ok::<_, tg::Error>(())
		};
		let Some(mut control) = control else {
			return tasks.await;
		};

		// Retain the sandbox state and control stream after the process tasks, while serving the control stream.
		let retain = async {
			tasks.await?;
			let retention_time_to_live = session.server.config.runner.sandbox_state_time_to_live;
			tokio::select! {
				() = session.server.clock.sleep(retention_time_to_live) => {},
				() = stopper.wait() => {},
			}
			Ok::<_, tg::Error>(())
		};
		self.serve_control_while(session, &boot, Phase::Ended, &mut control, retain)
			.await
	}

	async fn request_sandbox_destroy(
		sender: &SandboxControlSender,
		id: &tg::sandbox::Id,
		data: tg::sandbox::get::Output,
		processes: Vec<tg::process::Id>,
		sequence: u64,
	) -> tg::Result<()> {
		let request_id = tangram_server_core::control::id();
		let request =
			tg::sandbox::control::ClientMessage::Request(tg::sandbox::control::ClientRequest {
				arg: tg::sandbox::control::ClientRequestArg::Destroy(
					tg::sandbox::control::DestroyClientRequestArg {
						data,
						processes,
						sequence,
					},
				),
				id: request_id,
			});
		let response = sender
			.request(request, tangram_server_core::control::Priority::High)
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to send the destroy sandbox request"),
			)?;
		let response = response
			.await
			.map_err(|_| tg::error!("the sandbox control response stream ended"))?;
		let tg::sandbox::control::ServerMessage::Response(response) = response else {
			return Err(tg::error!("expected a sandbox control response"));
		};
		if let Some(error) = response.error {
			let error = tg::Error::try_from(error)
				.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
			return Err(tg::error!(!error, "the destroy sandbox request failed"));
		}
		response
			.output
			.ok_or_else(|| tg::error!("missing the destroy sandbox response output"))?
			.try_unwrap_destroy()
			.map_err(|_| tg::error!("expected a destroy sandbox response"))?;

		Ok(())
	}

	async fn request_sandbox_stop(
		sender: &SandboxControlSender,
		id: &tg::sandbox::Id,
		data: tg::sandbox::get::Output,
		processes: Vec<tg::process::Id>,
		sequence: u64,
	) -> tg::Result<()> {
		let request_id = tangram_server_core::control::id();
		let request =
			tg::sandbox::control::ClientMessage::Request(tg::sandbox::control::ClientRequest {
				arg: tg::sandbox::control::ClientRequestArg::Stop(
					tg::sandbox::control::StopClientRequestArg {
						data,
						processes,
						sequence,
					},
				),
				id: request_id,
			});
		let response = sender
			.request(request, tangram_server_core::control::Priority::High)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to send the stop sandbox request"))?;
		let response = response
			.await
			.map_err(|_| tg::error!("the sandbox control response stream ended"))?;
		let tg::sandbox::control::ServerMessage::Response(response) = response else {
			return Err(tg::error!("expected a sandbox control response"));
		};
		if let Some(error) = response.error {
			let error = tg::Error::try_from(error)
				.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
			return Err(tg::error!(!error, "the stop sandbox request failed"));
		}
		response
			.output
			.ok_or_else(|| tg::error!("missing the stop sandbox response output"))?
			.try_unwrap_stop()
			.map_err(|_| tg::error!("expected a stop sandbox response"))?;

		Ok(())
	}

	fn cancel_sandbox_processes(
		session: &Session,
		id: &tg::sandbox::Id,
		error: Option<tg::error::Data>,
	) -> tg::Result<()> {
		let error = error.unwrap_or_else(|| tg::error::Data {
			code: Some(tg::error::Code::Cancellation),
			message: Some("the process was canceled".into()),
			..Default::default()
		});
		let sandbox = session
			.server
			.components
			.runner
			.state()
			.sandboxes
			.get_by_id(id)
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
		for mut process in sandbox.processes.iter_mut() {
			if !process.value().data.status.is_finished() {
				process.value_mut().finish.get_or_insert(
					tg::process::control::FinishServerRequestArg {
						error: Some(error.clone()),
						exit: 1,
					},
				);
				process.stopper.stop();
			}
		}
		Ok(())
	}

	/// Destroy a stopped sandbox on this runner by removing its directory, releasing its filesystem project ID, and reporting its destruction, or destroy a boot of it that is running.
	pub(crate) async fn destroy_stopped_sandbox(
		&self,
		session: &Session,
		arg: tg::runner::control::DestroySandboxServerRequestArg,
		sender: &crate::RunnerSender,
	) -> tg::Result<tg::runner::control::DestroySandboxClientResponseOutput> {
		let tg::runner::control::DestroySandboxServerRequestArg { error, sandbox: id } = arg;

		// Wait for a boot that is starting to be stored, so that the destroy does not remove the directory it starts on.
		let control_sender = loop {
			let updated = self.state.sandboxes.updated();
			let sandbox = self.state.sandboxes.get_by_id(&id);
			let starting = self.state.boots.latest(&id).is_some_and(|latest| {
				sandbox
					.as_ref()
					.is_none_or(|sandbox| sandbox.boot.boot != latest.boot)
			});
			let control_sender = sandbox
				.filter(|sandbox| !sandbox.status.is_stopped_or_destroyed())
				.map(|sandbox| sandbox.control_sender.clone());
			if !starting {
				break control_sender;
			}
			updated.await;
		};

		// Destroy a boot that is running through its control, which reports its destruction.
		if let Some(control_sender) = control_sender {
			let request = tg::sandbox::control::ServerRequestArg::Destroy(
				tg::sandbox::control::DestroyServerRequestArg {
					ended: false,
					error,
				},
			);
			let response = control_sender.send_request(request).await?;
			let std::ops::ControlFlow::Break(response) = response.await?? else {
				return Err(tg::error!(%id, "the runner does not hold the sandbox"));
			};
			let destroyed = response
				.try_unwrap_destroy()
				.map_err(|_| tg::error!("expected a destroy sandbox response"))?
				.destroyed;
			let output = tg::runner::control::DestroySandboxClientResponseOutput { destroyed };
			return Ok(output);
		}

		// Report the storage usage of the stopped directory until now, so that its destruction bills it.
		let data = session
			.server
			.index
			.try_get_sandboxes(std::slice::from_ref(&id))
			.await?
			.pop()
			.flatten()
			.and_then(|sandbox| sandbox.data);
		if let Some(data) = data
			&& let Some(stopped_at) = data.data.stopped_at
			&& let Err(error) = self
				.report_stopped_sandbox_usage(session, &id, data.data.boot, stopped_at, sender)
				.await
		{
			tracing::error!(error = %error.trace(), sandbox = %id, "failed to report the stopped sandbox's usage");
		}

		// Remove the directory and report the destruction, and record it in the state of a stopped boot that the runner retains.
		self.remove_stopped_sandbox(session, &id, error, sender)
			.await?;
		Self::set_sandbox_status(session, &id, tg::sandbox::Status::Destroyed);
		let output = tg::runner::control::DestroySandboxClientResponseOutput { destroyed: true };

		Ok(output)
	}

	/// Ask that a stopped sandbox whose ttl expired be destroyed, which the index decides with a write that applies only while the sandbox is still stopped at its boot. If it applies, the sandbox's scheduler has this runner remove the directory, and otherwise a start won, and this runner keeps the directory.
	async fn destroy_expired_sandbox(
		id: &tg::sandbox::Id,
		boot: u64,
		sender: &crate::RunnerSender,
	) -> tg::Result<()> {
		let arg = tg::runner::control::DestroyStoppedSandboxClientRequestArg {
			boot,
			sandbox: id.clone(),
		};
		let destroying = match Self::request_runner_stopped_sandbox_destroy(sender, arg).await? {
			std::ops::ControlFlow::Break(destroying) => destroying,
			std::ops::ControlFlow::Continue(error) => {
				return Err(tg::error!(!error, %id, "failed to request the sandbox's destruction"));
			},
		};
		if !destroying {
			tracing::debug!(sandbox = %id, boot, "the expired sandbox is no longer stopped at its boot");
		}

		Ok(())
	}

	/// Remove a stopped sandbox's directory, release its filesystem project ID, and report its destruction, which marks the sandbox destroyed.
	async fn remove_stopped_sandbox(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		error: Option<tg::error::Data>,
		sender: &crate::RunnerSender,
	) -> tg::Result<()> {
		self.remove_stopped_sandbox_directory(session, id).await?;
		let arg = tg::runner::control::DestroySandboxClientRequestArg {
			error: error.map(tg::Either::Left),
			sandbox: id.clone(),
		};
		if let std::ops::ControlFlow::Continue(error) =
			Self::request_runner_sandbox_destroy(sender, arg).await?
		{
			return Err(tg::error!(!error, %id, "failed to report the sandbox's destruction"));
		}

		Ok(())
	}

	/// Remove a sandbox's directory, along with the link at the sandbox's path to a pooled sandbox's temp.
	async fn remove_sandbox_directory(
		link: std::path::PathBuf,
		id: tg::sandbox::Id,
		mut temp: Temp,
	) {
		if let Err(error) = temp.remove().await {
			tracing::error!(%error, sandbox = %id, "failed to remove the sandbox directory");
		}
		if tokio::fs::symlink_metadata(&link)
			.await
			.is_ok_and(|metadata| metadata.file_type().is_symlink())
		{
			tokio::fs::remove_file(&link).await.ok();
		}
	}

	fn sandbox_status(session: &Session, id: &tg::sandbox::Id) -> Option<tg::sandbox::Status> {
		session
			.server
			.components
			.runner
			.state()
			.sandboxes
			.get_by_id(id)
			.map(|sandbox| sandbox.status)
	}

	/// Move pooled sandboxes' temps to their sandboxes' paths, and reserve the filesystem project IDs of the sandboxes' directories.
	pub(crate) async fn reclaim_sandbox_directories(&self, server: &Server) {
		let path = server.path.join("sandboxes");
		let mut entries = match tokio::fs::read_dir(&path).await {
			Ok(entries) => entries,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
			Err(error) => {
				tracing::error!(%error, "failed to read the sandbox directories");
				return;
			},
		};
		loop {
			let entry = match entries.next_entry().await {
				Ok(Some(entry)) => entry,
				Ok(None) => break,
				Err(error) => {
					tracing::error!(%error, "failed to read a sandbox directory");
					break;
				},
			};
			let Some(id) = entry
				.file_name()
				.to_str()
				.and_then(|name| name.parse::<tg::sandbox::Id>().ok())
			else {
				continue;
			};
			let path = entry.path();

			// Move a pooled sandbox's temp to the sandbox's path, replacing the link that pointed at it, or remove a link whose temp is gone.
			let metadata = match tokio::fs::symlink_metadata(&path).await {
				Ok(metadata) => metadata,
				Err(error) => {
					tracing::error!(%error, sandbox = %id, "failed to read the sandbox directory");
					continue;
				},
			};
			if metadata.file_type().is_symlink() {
				let result = async {
					let target = tokio::fs::read_link(&path).await?;
					tokio::fs::remove_file(&path).await?;
					if tokio::fs::try_exists(&target).await? {
						tokio::fs::rename(&target, &path).await?;
						Ok::<_, std::io::Error>(true)
					} else {
						Ok(false)
					}
				}
				.await;
				#[cfg_attr(
					not(target_os = "linux"),
					allow(
						clippy::needless_continue,
						reason = "the remaining loop body is Linux-only"
					)
				)]
				match result {
					Ok(true) => {},
					Ok(false) => {
						#[cfg(target_os = "linux")]
						continue;
					},
					Err(error) => {
						tracing::error!(%error, sandbox = %id, "failed to move the sandbox directory");
						#[cfg(target_os = "linux")]
						continue;
					},
				}
			}

			// Reserve the directory's filesystem project ID.
			#[cfg(target_os = "linux")]
			match tangram_sandbox::filesystem_project_id(&path) {
				Ok(Some(value)) => {
					if let Some(project_id) = self.state.filesystem_project_ids.acquire_value(value)
					{
						self.state.stopped_project_ids.insert(id, project_id);
					}
				},
				Ok(None) => {},
				Err(error) => {
					tracing::error!(error = %error.trace(), sandbox = %id, "failed to read the sandbox filesystem project");
				},
			}
		}
	}

	/// Keep the directory of a previous sandbox that stays stopped, reserving its storage and counting the directory in the storage budget.
	pub(crate) fn keep_stopped_sandbox_directory(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		storage: Option<tg::sandbox::Resource<u64>>,
	) {
		self.reserve_stopped_sandbox_storage(id, storage.unwrap_or_default());
		#[cfg(target_os = "linux")]
		if let Some(storage) = storage {
			self.adopt_stopped_sandbox_storage(session, id, storage);
		}
		#[cfg(not(target_os = "linux"))]
		let _ = session;
	}

	/// Report the storage usage of this runner's stopped sandboxes, and destroy those whose ttl has expired, at each interval, until the stopper stops.
	pub(crate) async fn stopped_sandbox_task(&self, session: &Session, stopper: Stopper) {
		let interval = session.server.config.runner.stopped_sandbox_interval;
		let mut first_seen = BTreeMap::new();
		loop {
			tokio::select! {
				() = session.server.clock.sleep(interval) => {},
				() = stopper.wait() => return,
			}
			if let Err(error) = self
				.update_stopped_sandboxes(session, &mut first_seen)
				.await
			{
				tracing::error!(error = %error.trace(), "failed to update the stopped sandboxes");
			}

			// Hit a checkpoint once this interval's update of the stopped sandboxes is finished.
			tangram_server_core::checkpoint!(session.server, "runner.sandbox.stopped.updated")
				.await;
		}
	}

	async fn update_stopped_sandboxes(
		&self,
		session: &Session,
		first_seen: &mut BTreeMap<(tg::sandbox::Id, u64), i64>,
	) -> tg::Result<()> {
		// Get the runner control sender, waiting for the next interval while the runner is not connected.
		let Some(sender) = self.control.borrow().clone() else {
			return Ok(());
		};

		// Get the sandboxes with directories and their local records.
		let ids = Self::sandbox_directory_ids(session).await?;
		let indexed = session.server.index.try_get_sandboxes(&ids).await?;
		let now = session.server.clock.unix_timestamp();
		first_seen.retain(|(id, _), _| ids.contains(id));

		// Measure each stopped sandbox, and find those whose ttl has expired.
		let mut reports = Vec::new();
		let mut expired = Vec::new();
		for (id, indexed) in std::iter::zip(ids, indexed) {
			// Skip a sandbox that is not stopped, or that has a boot starting or running.
			let Some(data) = indexed.and_then(|indexed| indexed.data) else {
				continue;
			};
			if data.data.status != tg::sandbox::Status::Stopped
				|| self.sandbox_is_starting_or_running(&id)
			{
				continue;
			}

			// Get when the sandbox stopped and when it expires, counting from when this runner first saw the boot stopped if its stop recorded neither, such as after a crash.
			let first_seen_at = *first_seen
				.entry((id.clone(), data.data.boot))
				.or_insert(now);
			let stopped_at = data.data.stopped_at.unwrap_or(first_seen_at);
			let expires_at = data.data.expires_at.or_else(|| {
				data.data.time_to_live.map(|time_to_live| {
					first_seen_at
						.saturating_add(time_to_live.as_secs().try_into().unwrap_or(i64::MAX))
				})
			});

			// Measure the storage usage of the stopped directory.
			let usage = self
				.stopped_sandbox_usage(session, &id, stopped_at)
				.inspect_err(|error| {
					tracing::error!(error = %error.trace(), sandbox = %id, "failed to measure the stopped sandbox's usage");
				})
				.ok()
				.flatten();
			if expires_at.is_some_and(|expires_at| now >= expires_at) {
				expired.push((id.clone(), data.data.boot));
			}
			let report = tg::runner::control::ReportSandboxUsageClientRequestSandbox {
				boot: data.data.boot,
				sandbox: id,
				usage,
			};
			reports.push(report);
		}

		// Report the stopped sandboxes in batches, and remove the directory of each sandbox that the index records as destroying or destroyed, unless a boot of it is starting or running on this runner.
		let batch_size = session
			.server
			.config
			.runner
			.stopped_sandbox_report_batch_size
			.max(1);
		let mut removed = BTreeSet::new();
		for batch in reports.chunks(batch_size) {
			let arg = tg::runner::control::ReportSandboxUsageClientRequestArg {
				sandboxes: batch.to_vec(),
			};
			let output = match crate::Component::request_runner_sandbox_usage(&sender, arg).await {
				Ok(std::ops::ControlFlow::Break(output)) => output,
				Ok(std::ops::ControlFlow::Continue(error)) | Err(error) => {
					tracing::error!(error = %error.trace(), "failed to report the stopped sandboxes' usage");
					continue;
				},
			};
			for reply in output.sandboxes {
				let tg::runner::control::StoppedSandboxAction::Remove(removal) = reply.action
				else {
					continue;
				};
				if self.sandbox_is_starting_or_running(&reply.sandbox) {
					continue;
				}

				// Remove the directory, and report the destruction of a sandbox that the index records as destroying or destroyed.
				let result = match removal {
					tg::runner::control::StoppedSandboxRemoval::Destroyed
					| tg::runner::control::StoppedSandboxRemoval::Destroying => {
						self.remove_stopped_sandbox(session, &reply.sandbox, None, &sender)
							.await
					},
				};
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), sandbox = %reply.sandbox, "failed to remove the stopped sandbox's directory");
					continue;
				}
				removed.insert(reply.sandbox);
			}
		}

		// Ask that each expired sandbox be destroyed, checking again that no boot started while the usage was reported. The index decides the destroy of a stopped sandbox, which applies only while the sandbox is still stopped at its boot, and the sandbox's scheduler then has this runner remove the directory.
		for (id, boot) in expired {
			if removed.contains(&id) || self.sandbox_is_starting_or_running(&id) {
				continue;
			}
			if let Err(error) = Self::destroy_expired_sandbox(&id, boot, &sender).await {
				tracing::error!(error = %error.trace(), sandbox = %id, "failed to destroy the expired sandbox");
			}
		}

		Ok(())
	}

	/// Get whether a boot of the sandbox is starting or running on this runner.
	fn sandbox_is_starting_or_running(&self, id: &tg::sandbox::Id) -> bool {
		let sandbox = self.state.sandboxes.get_by_id(id);
		let starting = self.state.boots.latest(id).is_some_and(|latest| {
			sandbox
				.as_ref()
				.is_none_or(|sandbox| sandbox.boot.boot != latest.boot)
		});
		let running = sandbox.is_some_and(|sandbox| !sandbox.status.is_stopped_or_destroyed());
		starting || running
	}

	/// Report the storage usage of a restarted sandbox's previous boot in a task, because the start must not wait for it.
	fn spawn_report_restarted_sandbox_usage(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		boot: u64,
		stopped_at: i64,
	) {
		let component = self.clone();
		let session = session.clone();
		let id = id.clone();
		tokio::spawn(async move {
			let Some(sender) = component.control.borrow().clone() else {
				return;
			};
			if let Err(error) = component
				.report_stopped_sandbox_usage(&session, &id, boot, stopped_at, &sender)
				.await
			{
				tracing::error!(error = %error.trace(), sandbox = %id, "failed to report the restarted sandbox's stopped usage");
			}
		});
	}

	/// Report the storage usage of a stopped boot's directory from its stop until now.
	pub(crate) async fn report_stopped_sandbox_usage(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		boot: u64,
		stopped_at: i64,
		sender: &crate::RunnerSender,
	) -> tg::Result<()> {
		// Measure the directory.
		let Some(usage) = self.stopped_sandbox_usage(session, id, stopped_at)? else {
			return Ok(());
		};

		// Report the usage.
		let report = tg::runner::control::ReportSandboxUsageClientRequestSandbox {
			boot,
			sandbox: id.clone(),
			usage: Some(usage),
		};
		let arg = tg::runner::control::ReportSandboxUsageClientRequestArg {
			sandboxes: vec![report],
		};
		if let std::ops::ControlFlow::Continue(error) =
			crate::Component::request_runner_sandbox_usage(sender, arg).await?
		{
			return Err(error);
		}

		Ok(())
	}

	/// Measure the storage usage of a stopped boot's directory from its stop until now, or return `None` if the directory has no filesystem project.
	fn stopped_sandbox_usage(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		stopped_at: i64,
	) -> tg::Result<Option<tg::sandbox::Usage>> {
		let Some(size) = self.stopped_sandbox_size(session, id)? else {
			return Ok(None);
		};
		let now = session.server.clock.unix_timestamp();
		let usage = stopped_storage_usage(size, now.saturating_sub(stopped_at));

		Ok(Some(usage))
	}

	/// Measure a stopped sandbox's directory with its filesystem project, or return `None` without one, because storage counts only with a filesystem project, as a running sandbox's storage does.
	#[cfg(target_os = "linux")]
	fn stopped_sandbox_size(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<u64>> {
		let Some(project) = self
			.state
			.stopped_project_ids
			.get(id)
			.map(|entry| entry.value().value())
		else {
			return Ok(None);
		};
		let path = session.server.path.join("sandboxes");
		let size = tangram_sandbox::filesystem_project_usage(&path, project)?;
		Ok(Some(size))
	}

	/// Measure a stopped sandbox's directory, which has no filesystem project without Linux.
	#[cfg(not(target_os = "linux"))]
	#[expect(clippy::unnecessary_wraps, clippy::unused_self)]
	fn stopped_sandbox_size(
		&self,
		_session: &Session,
		_id: &tg::sandbox::Id,
	) -> tg::Result<Option<u64>> {
		Ok(None)
	}

	/// Record the end of a boot in this server's local copies of a sandbox whose home is remote, by finishing the boot's unfinished processes and writing the sandbox's ended data, so that no local record stays unfinished while the home records the end.
	#[expect(
		clippy::too_many_arguments,
		reason = "the local copies of an ended boot have this many inputs"
	)]
	async fn record_ended_sandbox_local(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		location: &tg::Location,
		created_at: i64,
		data: &tg::sandbox::get::Output,
		processes: &[tg::process::Id],
		unfinished: &[tg::runner::control::Process],
	) -> tg::Result<()> {
		if !location.is_remote() {
			return Ok(());
		}

		// Finish the unfinished processes.
		let error = Self::ended_process_error(data);
		for process in unfinished {
			let Some(process_id) = &process.id else {
				continue;
			};
			let data = process
				.data
				.clone()
				.without_location_and_tokens_except_sync();
			let result = session
				.server
				.components
				.process
				.finish_queued_process(&session.server, process_id, data, error.clone(), 0)
				.await;
			if let Err(error) = result {
				tracing::warn!(error = %error.trace(), process = %process_id, "failed to finish the process locally");
			}
		}

		// Record the sandbox's end.
		self.index_remote_sandbox(
			session,
			id,
			location,
			created_at,
			Some(data),
			Some(processes),
		)
		.await?;

		Ok(())
	}

	/// Get the error with which the unfinished processes of an ended boot finish, which is the boot's error, or a cancellation if the boot ended without one.
	fn ended_process_error(
		data: &tg::sandbox::get::Output,
	) -> tg::Either<tg::error::Data, tg::error::Id> {
		data.data.error.clone().unwrap_or_else(|| {
			tg::Either::Left(tg::error::Data {
				code: Some(tg::error::Code::Cancellation),
				message: Some("the sandbox ended".to_owned()),
				..Default::default()
			})
		})
	}

	/// Create the data of a sandbox whose boot failed, which records a stop as of now, so that the stopped sandbox is charged and expires from now.
	fn ended_sandbox_data(session: &Session, arg: EndedSandboxDataArg) -> tg::sandbox::get::Output {
		let EndedSandboxDataArg {
			boot,
			create,
			creator,
			error,
			id,
			location,
			owner,
			status,
		} = arg;
		let now = session.server.clock.unix_timestamp();
		let (expires_at, stopped_at) = if status.is_stopped() {
			let expires_at = create.time_to_live.map(|time_to_live| {
				now.saturating_add(time_to_live.as_secs().try_into().unwrap_or(i64::MAX))
			});
			(expires_at, Some(now))
		} else {
			(None, None)
		};
		let data = tg::sandbox::Data {
			boot,
			cpu: create.cpu,
			creator,
			error: Some(error),
			expires_at,
			hostname: create.hostname,
			id,
			memory: create.memory,
			mounts: create.mounts,
			network: create.network,
			owner,
			status,
			stopped_at,
			storage: create.storage,
			time_to_live: create.time_to_live,
			time_to_stop: create.time_to_stop,
			usage: None,
		};

		tg::sandbox::get::Output {
			data,
			location: Some(location),
			tokens: tg::authorization::Tokens::default(),
		}
	}

	/// Record a previous run's sandbox as stopped in this runner's local index when its location is remote, because the remote records the stop, and the local record is the one this runner reads.
	pub(crate) async fn record_previous_sandbox_stop(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
	) -> tg::Result<()> {
		let Some(indexed) = session
			.server
			.index
			.try_get_sandboxes(std::slice::from_ref(id))
			.await?
			.pop()
			.flatten()
		else {
			return Ok(());
		};
		let (Some(location), Some(mut data)) = (indexed.location, indexed.data) else {
			return Ok(());
		};
		if !location.is_remote() || data.data.status.is_stopped_or_destroyed() {
			return Ok(());
		}
		let stopped_at = session.server.clock.unix_timestamp();
		data.data.expires_at = data.data.time_to_live.map(|time_to_live| {
			stopped_at.saturating_add(time_to_live.as_secs().try_into().unwrap_or(i64::MAX))
		});
		data.data.status = tg::sandbox::Status::Stopped;
		data.data.stopped_at = Some(stopped_at);
		self.index_remote_sandbox(
			session,
			id,
			&location,
			indexed.created_at,
			Some(&data),
			None,
		)
		.await?;

		Ok(())
	}

	/// Get the IDs of the sandboxes with directories on this runner.
	pub(crate) async fn sandbox_directory_ids(
		session: &Session,
	) -> tg::Result<Vec<tg::sandbox::Id>> {
		let path = session.server.path.join("sandboxes");
		let mut ids = Vec::new();
		let mut entries = match tokio::fs::read_dir(&path).await {
			Ok(entries) => entries,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(ids),
			Err(error) => {
				return Err(tg::error!(!error, "failed to read the sandbox directories"));
			},
		};
		while let Some(entry) = entries
			.next_entry()
			.await
			.map_err(|error| tg::error!(!error, "failed to read a sandbox directory"))?
		{
			if let Some(id) = entry
				.file_name()
				.to_str()
				.and_then(|name| name.parse::<tg::sandbox::Id>().ok())
			{
				ids.push(id);
			}
		}

		Ok(ids)
	}

	/// Count a stopped sandbox's directory in the runner's storage budget, logging a failure, because the budget would otherwise grant the directory's space to other sandboxes.
	#[cfg(target_os = "linux")]
	fn adopt_stopped_sandbox_storage(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		storage: tg::sandbox::Resource<u64>,
	) {
		let Some(project) = self
			.state
			.stopped_project_ids
			.get(id)
			.map(|entry| entry.value().value())
		else {
			return;
		};
		let path = Self::sandbox_path(session, id);
		if let Err(error) = tangram_sandbox::adopt_stopped_storage(
			&self.state.storage_pool,
			&path,
			storage,
			project,
		) {
			tracing::warn!(error = %error.trace(), sandbox = %id, "failed to count the stopped sandbox's storage");
		}
	}

	/// Reserve storage for a stopped sandbox's directory, logging a reservation that no longer fits, such as after the runner's storage capacity shrank.
	fn reserve_stopped_sandbox_storage(
		&self,
		id: &tg::sandbox::Id,
		storage: tg::sandbox::Resource<u64>,
	) {
		let capacity = tg::runner::Capacity {
			storage,
			..Default::default()
		};
		let Some(allocation) = self.state.capacity.try_acquire(capacity) else {
			tracing::warn!(sandbox = %id, "failed to reserve the stopped sandbox's storage");
			return;
		};
		self.state.stopped_storage.insert(id.clone(), allocation);
	}

	/// Remove a stopped sandbox's directory, and release its filesystem project ID and its storage reservation.
	pub(crate) async fn remove_stopped_sandbox_directory(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
	) -> tg::Result<()> {
		let mut temp = Temp::with_path(&session.server, Self::sandbox_path(session, id));
		temp.remove()
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to remove the sandbox directory"))?;
		#[cfg(target_os = "linux")]
		if let Some((_, project_id)) = self.state.stopped_project_ids.remove(id) {
			tangram_sandbox::forget_stopped_storage(&self.state.storage_pool, project_id.value());
			project_id.release();
		}
		self.state.stopped_storage.remove(id);

		Ok(())
	}

	/// Get the path of the directory that holds a sandbox's data while it is stopped.
	fn sandbox_path(session: &Session, id: &tg::sandbox::Id) -> std::path::PathBuf {
		session.server.path.join("sandboxes").join(id.to_string())
	}

	fn sandbox_error_data(
		session: &Session,
		error: &tg::Error,
	) -> tg::Either<tg::error::Data, tg::error::Id> {
		let mut error = error.to_data_or_id();
		if !session.server.config.advanced.internal_error_locations
			&& let tg::Either::Left(error) = &mut error
		{
			error.remove_internal_locations();
		}
		error
	}

	async fn get_sandbox_control_stream(
		&self,
		session: &Session,
		arg: GetSandboxControlStreamArg<'_>,
	) -> tg::Result<ConnectedSandboxControl> {
		let GetSandboxControlStreamArg {
			boot,
			create,
			created_at,
			creator,
			id,
			input_stream,
			location,
			owner,
			proof,
			sequence,
		} = arg;
		let runner = session
			.server
			.components
			.runner
			.state()
			.id()
			.ok_or_else(|| tg::error!("missing the runner id"))?;
		let data = tg::sandbox::get::Output {
			data: tg::sandbox::Data {
				boot,
				cpu: create.cpu,
				creator: creator.clone(),
				error: None,
				expires_at: None,
				hostname: create.hostname.clone(),
				id: id.clone(),
				memory: create.memory,
				mounts: create.mounts.clone(),
				network: create.network.clone(),
				owner: owner.clone(),
				status: tg::sandbox::Status::Started,
				stopped_at: None,
				storage: create.storage,
				time_to_live: create.time_to_live,
				time_to_stop: create.time_to_stop,
				usage: None,
			},
			location: Some(location.clone()),
			tokens: tg::authorization::Tokens::default(),
		};
		let arg = tg::sandbox::control::Arg {
			boot,
			create: Some(create),
			created_at: Some(created_at),
			creator,
			id: Some(id.clone()),
			location: Some(location.clone().into()),
			owner,
			proof,
			runner: Some(runner),
			sequence,
		};
		let (header, control) = self
			.connect_sandbox_control(session, arg, input_stream)
			.await?;
		if header.id != *id {
			return Err(
				tg::error!(actual = %header.id, expected = %id, "the server returned an invalid sandbox"),
			);
		}
		let token = header
			.token
			.or_else(|| session.context.token.clone())
			.ok_or_else(
				|| tg::error!(id = %header.id, "missing the sandbox authentication token"),
			)?;
		self.index_remote_sandbox(session, &header.id, location, created_at, Some(&data), None)
			.await?;
		let connection = ConnectedSandboxControl {
			requests: control,
			token,
		};

		Ok(connection)
	}

	#[tracing::instrument(name = "sandbox.connect", level = "debug", skip_all, fields(sandbox = arg.id.as_ref().map(ToString::to_string)), err(level = "debug"))]
	async fn connect_sandbox_control(
		&self,
		session: &Session,
		arg: tg::sandbox::control::Arg,
		input: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<(
		tg::sandbox::control::Header,
		BoxStream<'static, tg::Result<tg::control::Event<tg::sandbox::control::ServerMessage>>>,
	)> {
		let id = arg.id.clone();
		let shortcut = arg.proof.is_some();
		tangram_server_core::checkpoint!(session.server, "runner.sandbox.control.connect", sandbox = ?id, shortcut = %shortcut).await;
		let reconnect_context = session.context.clone();
		let reconnect_server = session.server.clone();
		let reconnect = move |header: &tg::sandbox::control::Header| {
			let token = header.token.clone().or(reconnect_context.token.clone());
			let context = Context {
				principal: tg::Principal::Sandbox(header.id.clone()),
				token,
				..reconnect_context
			};

			reconnect_server.session(&context)
		};
		let (header, control) = session
			.get_sandbox_control_stream_all(arg, input, reconnect)
			.boxed()
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					?id,
					"failed to connect to the sandbox control stream"
				)
			})?;

		Ok((header, control.boxed()))
	}

	async fn index_remote_sandbox(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		location: &tg::Location,
		created_at: i64,
		data: Option<&tg::sandbox::get::Output>,
		processes: Option<&[tg::process::Id]>,
	) -> tg::Result<()> {
		if !location.is_remote() {
			return Ok(());
		}
		let data = data.cloned().map(|mut data| {
			data.tokens.clear();
			data
		});
		let touched_at = session.server.clock.unix_timestamp();

		// Order the local copy's writes of a boot by the boot's status, which only advances on the runner.
		let sequence = data.as_ref().map_or(0, |data| match data.data.status {
			tg::sandbox::Status::Starting => 0,
			tg::sandbox::Status::Started => 1,
			tg::sandbox::Status::Destroyed
			| tg::sandbox::Status::Destroying
			| tg::sandbox::Status::Stopped
			| tg::sandbox::Status::Stopping => 2,
		});
		let sandbox = tangram_index::sandbox::put::Arg {
			account: None,
			created_at,
			data,
			id: id.clone(),
			location: Some(location.clone()),
			permissions: Vec::new(),
			principal: tg::Principal::Sandbox(id.clone()),
			process: None,
			processes: processes.map(<[_]>::to_vec),
			runner: None,
			scheduler: None,
			sequence,
			stopped_boot: None,
			stopped_usage: None,
			touched_at,
		};
		let arg = tangram_index::write::Arg {
			items: vec![tangram_index::Item::PutSandbox(Box::new(sandbox))],
		};
		session
			.server
			.components
			.indexer
			.write(&session.server, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to index the remote sandbox"))?;

		Ok(())
	}

	fn prepare_process(
		mut arg: tg::runner::control::Process,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<tg::runner::control::Process> {
		if arg.id.is_none() {
			arg.data.sandbox = Some(sandbox.clone());
		} else if arg.data.sandbox.as_ref() != Some(sandbox) {
			let process = arg.id.as_ref();
			return Err(tg::error!(
				?process,
				sandbox = %sandbox,
				"the process is not in the sandbox"
			));
		}
		Ok(arg)
	}
}

impl From<tg::Error> for Failed {
	fn from(error: tg::Error) -> Self {
		Self {
			control: None,
			error,
		}
	}
}

/// Compute the storage usage of a stopped directory of a size in bytes over a number of seconds, in mebibyte-milliseconds, as consumption without a reservation, because a stopped sandbox holds none.
fn validate_hardened_sandbox(arg: &tg::sandbox::create::Arg) -> tg::Result<()> {
	if matches!(arg.network.as_ref(), Some(tg::sandbox::Network::Host)) {
		return Err(tg::error!(
			"host networking is not allowed for hardened container isolation"
		));
	}
	if arg.mounts.iter().any(|mount| !mount.readonly) {
		return Err(tg::error!(
			"writable mounts are not allowed for hardened container isolation"
		));
	}
	Ok(())
}

#[cfg(target_os = "linux")]
fn validate_native_mounts(
	backend: &tangram_sandbox::Backend,
	arg: &tg::sandbox::create::Arg,
) -> tg::Result<()> {
	if matches!(
		backend,
		tangram_sandbox::Backend::Builtin | tangram_sandbox::Backend::Bubblewrap { .. }
	) {
		tangram_sandbox::guest::validate_mounts(&arg.mounts)?;
	}
	Ok(())
}

fn stopped_storage_usage(size: u64, seconds: i64) -> tg::sandbox::Usage {
	let milliseconds = u128::try_from(seconds).unwrap_or_default() * 1000;
	let consumption = (u128::from(size) * milliseconds).div_ceil(1_048_576);
	let storage = tg::sandbox::ResourceUsage {
		consumption: u64::try_from(consumption).unwrap_or(u64::MAX),
		reservation: 0,
	};
	tg::sandbox::Usage {
		cpu: tg::sandbox::ResourceUsage::default(),
		memory: tg::sandbox::ResourceUsage::default(),
		storage,
	}
}

/// Create the idle timer for a sandbox without processes, which fires with a destroy after its ttl, or with a stop after its time to stop if that is shorter.
fn idle_timer(
	server: &Server,
	time_to_live: Option<std::time::Duration>,
	time_to_stop: Option<std::time::Duration>,
) -> Option<BoxFuture<'static, Teardown>> {
	let (duration, teardown) = match (time_to_live, time_to_stop) {
		(None, None) => return None,
		(Some(time_to_live), Some(time_to_stop)) if time_to_stop < time_to_live => {
			(time_to_stop, Teardown::Stop)
		},
		(Some(time_to_live), _) => (time_to_live, Teardown::Destroy),
		(None, Some(time_to_stop)) => (time_to_stop, Teardown::Stop),
	};
	let timer = server.clock.sleep(duration).map(move |()| teardown).boxed();
	Some(timer)
}

fn wait_for_process_control_connection(
	output: SpawnProcessTaskOutput,
	reply: Option<Reply>,
) -> PendingProcess {
	let SpawnProcessTaskOutput {
		mut events,
		process,
	} = output;
	async move {
		let result = async {
			let event = events
				.recv()
				.await
				.ok_or_else(|| tg::error!("the process event sender was dropped"))??;
			let ProcessEvent::Connected(event) = event else {
				return Err(tg::error!("expected the process connected event"));
			};
			Ok(event)
		}
		.await;
		(result, events, process, reply)
	}
	.boxed()
}
