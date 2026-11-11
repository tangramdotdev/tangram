use {
	std::path::PathBuf,
	tangram_cli_core::Cli,
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tokio::signal::unix::{Signal, SignalKind, signal},
};

/// Spawn and await a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	/// The reference to the command, artifact, or process.
	#[arg(index = 1, value_terminator = "--")]
	pub reference: Option<tg::Reference>,

	/// Set arguments.
	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	/// Whether to check out the output.
	#[arg(
		id = "run.checkout",
		long = "checkout",
		require_equals = true,
		short = 'c'
	)]
	pub checkout: Option<Option<PathBuf>>,

	/// Whether to overwrite an existing file system object at the path.
	#[arg(
		id = "run.checkout_force",
		long = "checkout-force",
		requires = "run.checkout"
	)]
	pub checkout_force: bool,

	/// If this flag is set, then exit immediately instead of waiting for the process to finish.
	#[arg(id = "run.detach", long = "detach", short = 'd')]
	pub detach: bool,

	/// Echo the process's logged stdout and stderr to stdout and stderr while waiting for the process to finish.
	#[arg(conflicts_with = "run.detach", id = "run.echo", long = "echo")]
	pub echo: bool,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[command(flatten)]
	pub spawn: crate::spawn::Options,

	/// Print the full spawn output instead of just the process ID.
	#[arg(id = "run.verbose", long = "verbose", short = 'v')]
	pub verbose: bool,

	/// The view to display if the process's stdio is not attached.
	#[arg(id = "run.view", long = "view")]
	pub view: Option<View>,
}

#[derive(
	Clone,
	Copy,
	Debug,
	clap::ValueEnum,
	derive_more::IsVariant,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum View {
	None,
	Inline,
	Fullscreen,
}

/// The signals that end tg run.
struct Signals {
	hangup: Signal,
	interrupt: Signal,
	terminate: Signal,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let checkout = args.options.checkout.is_some();
	let detach = args.options.detach;
	let print = args.options.print.clone();
	let verbose = args.options.verbose;

	// Run.
	let output = run_inner(cli, args).await?;

	// Print the output.
	if detach && !verbose {
		let string = output
			.try_unwrap_string()
			.ok()
			.ok_or_else(|| tg::error!("expected a string"))?;
		let referent = string
			.parse::<tg::Referent<tg::Either<u32, tg::process::Id>>>()
			.map_err(|error| tg::error!(!error, "failed to parse the process referent"))?;
		Cli::print_referent(&referent, &print);
	} else if checkout {
		Cli::print_display(output);
	} else if (detach && verbose) || !output.is_null() {
		cli.print_value(&output, print, None).await?;
	}

	Ok(())
}

pub(crate) async fn run_inner(cli: &mut Cli, args: Args) -> tg::Result<tg::Value> {
	let client = cli.client().await?;

	let Args {
		mut options,
		reference,
		trailing,
	} = args;

	// If detach is set, then set stdio if necessary.
	if options.detach {
		match options.spawn.stdin {
			None => {
				options.spawn.stdin = Some(tg::process::Stdio::Null);
			},
			Some(tg::process::Stdio::Null) => (),
			Some(_) => {
				return Err(tg::error!("invalid stdin with detach"));
			},
		}
		match options.spawn.stdout {
			None => {
				options.spawn.stdout = Some(tg::process::Stdio::Log);
			},
			Some(tg::process::Stdio::Null | tg::process::Stdio::Log) => (),
			Some(_) => {
				return Err(tg::error!("invalid stdout with detach"));
			},
		}
		match options.spawn.stderr {
			None => {
				options.spawn.stderr = Some(tg::process::Stdio::Log);
			},
			Some(tg::process::Stdio::Null | tg::process::Stdio::Log) => (),
			Some(_) => {
				return Err(tg::error!("invalid stderr with detach"));
			},
		}
	}

	// Get the logged streams to echo, which a view cannot display alongside.
	let echo = if options.echo {
		if matches!(options.view, Some(View::Fullscreen | View::Inline)) {
			return Err(tg::error!("cannot echo the process's stdio with a view"));
		}
		[
			(
				tg::process::stdio::Stream::Stdout,
				options.spawn.stdout.as_ref(),
			),
			(
				tg::process::stdio::Stream::Stderr,
				options.spawn.stderr.as_ref(),
			),
		]
		.into_iter()
		.filter_map(|(stream, stdio)| {
			matches!(stdio, Some(tg::process::Stdio::Log)).then_some(stream)
		})
		.collect()
	} else {
		Vec::new()
	};

	// Determine the connection mode.
	let mode = if options.detach {
		tg::process::attach::Mode::Spawn
	} else {
		tg::process::attach::Mode::Run
	};

	// Determine whether a sandboxed process runs in a sandbox that this run creates for it, which is destroyed once it is unused.
	let ephemeral_sandbox = !matches!(options.spawn.sandbox.get(), Some(tg::Either::Right(_)))
		&& options.spawn.sandbox.time_to_live.get() == Some(std::time::Duration::ZERO);

	// Install the signal handlers before spawning a sandboxed process that this run awaits without a viewer, because the process does not receive the signals that end tg run.
	let mut signals = if !options.detach
		&& options.spawn.sandboxed()
		&& matches!(options.view, None | Some(View::None))
	{
		Some(Signals::new()?)
	} else {
		None
	};

	// Spawn the process. After a signal, finish spawning so that the process can be stopped, and exit with the code for the signal if the spawn fails or another signal arrives.
	let print = (!options.detach).then(|| options.print.clone());
	let mut spawn_signal = None;
	let process = {
		let mut spawn_future = Box::pin(crate::spawn::spawn(
			cli,
			options.spawn,
			reference,
			trailing,
			print,
			mode,
		));
		match &mut signals {
			Some(signals) => tokio::select! {
				result = &mut spawn_future => result?,
				signal = signals.recv() => {
					let signal = signal?;
					spawn_signal = Some(signal);
					tokio::select! {
						result = spawn_future => result.unwrap_or_else(|error| {
							tracing::warn!(?error, "failed to spawn the process");
							exit_with_signal(signal)
						}),
						received = signals.recv() => exit_with_signal(received?),
					}
				},
			},
			None => spawn_future.await?,
		}
	};

	// If the detach flag is set, then return the process referent.
	if options.detach {
		process.node().detach().await?;
		if options.verbose {
			let output = tg::process::spawn::Output {
				cached: process.node().cached().unwrap_or(false),
				command: None,
				lease: process.node().lease().copied(),
				location: process
					.node()
					.location()
					.and_then(|location| location.to_location()),
				outcome: process.node().outcome_data(),
				process: process.node().id().cloned(),
				tokens: process.node().tokens(),
			};
			let value = serde_json::to_value(output)
				.map_err(|error| tg::error!(!error, "failed to serialize the output"))?
				.into();
			return Ok(value);
		}
		return Ok(process.node().to_referent().to_string().into());
	}

	// Spawn the view task if necessary.
	let (view_task, view_receiver, view_ready_receiver) = match options.view {
		None | Some(View::None) => (None, None, None),
		Some(view) => {
			let (exit_sender, exit_receiver) = tokio::sync::oneshot::channel();
			let (view_sender, view_receiver) = tokio::sync::oneshot::channel();
			let (view_ready_sender, view_ready_receiver) = tokio::sync::oneshot::channel();
			let client = client.clone();
			let root = process.clone().map(tangram_cli_viewer::Item::Process);
			let config = cli.viewer_config();
			let task = Task::spawn_blocking(move |stop| -> tg::Result<()> {
				let local_set = tokio::task::LocalSet::new();
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.map_err(|error| tg::error!(!error, "failed to create the tokio runtime"))?;
				let _exit_sender = exit_sender;
				let outcome = local_set.block_on(&runtime, async move {
					let viewer_options = tangram_cli_viewer::Options {
						collapse_process_children: true,
						config,
						depth: None,
						expand_groups: false,
						expand_metadata: false,
						expand_objects: false,
						expand_organizations: false,
						expand_processes: true,
						expand_sandboxes: false,
						expand_tags: false,
						expand_users: false,
						expand_values: false,
						show_process_commands: false,
					};
					let mut viewer = tangram_cli_viewer::Viewer::new(
						&client,
						root,
						exit_receiver,
						viewer_options,
					)?;
					view_ready_sender.send(()).ok();
					let outcome = match view {
						View::Fullscreen => viewer.run_fullscreen(stop, true).await?,
						View::Inline => viewer.run_inline(stop, false).await?,
						View::None => unreachable!(),
					};

					Ok::<_, tg::Error>(outcome)
				})?;
				view_sender.send(outcome).ok();

				Ok(())
			});
			(Some(task), Some(view_receiver), Some(view_ready_receiver))
		},
	};
	if let Some(view_ready_receiver) = view_ready_receiver {
		view_ready_receiver.await.ok();
	}

	// Echo the logged streams while awaiting the process.
	let echo_task = (!echo.is_empty()).then(|| {
		let client = client.clone();
		let process = process.node().clone();
		tokio::spawn(async move { process.echo_with_instance(&client, echo).await })
	});

	// Await the process or a request to exit from the viewer.
	let mut wait_future = Box::pin(
		process
			.node()
			.wait_with_instance(&client, tg::process::wait::Options::default()),
	);
	let outcome = if let Some(mut view_receiver) = view_receiver {
		tokio::select! {
			result = &mut wait_future => result,
			result = &mut view_receiver => {
				match result {
					Err(_) | Ok(tangram_cli_viewer::Outcome::Finished) => wait_future.await,
					Ok(outcome) => {
						drop(wait_future);
						if let Some(view_task) = view_task {
							stop_view_task(view_task).await;
						}
						match outcome {
							tangram_cli_viewer::Outcome::Finished => unreachable!(),
							tangram_cli_viewer::Outcome::Interrupt => {
								cli.exit.replace(130.into());
							},
							tangram_cli_viewer::Outcome::Quit => (),
							tangram_cli_viewer::Outcome::Terminate => {
								cli.exit.replace(143.into());
							},
						}
						return Ok(tg::Value::Null);
					},
				}
			},
		}
	} else if let Some(mut signals) = signals {
		// Await the process unless a signal arrived while spawning it.
		let received = match spawn_signal {
			Some(signal) => tg::Either::Right(signal),
			None => tokio::select! {
				result = &mut wait_future => tg::Either::Left(result),
				signal = signals.recv() => tg::Either::Right(signal?),
			},
		};
		match received {
			tg::Either::Left(result) => {
				// Exit on a later signal, because the signal handlers remain installed.
				tokio::spawn(async move {
					if let Ok(signal) = signals.recv().await {
						exit_with_signal(signal);
					}
				});
				result
			},
			tg::Either::Right(signal) => {
				// Stop the process if this run spawned it, and exit. Exit at once on another signal.
				if process.node().lease().is_some() {
					let stop = async {
						match stop_process(&client, process.node(), ephemeral_sandbox).await {
							Ok(true) => {
								wait_future.await.ok();
							},
							Ok(false) => (),
							Err(error) => {
								tracing::warn!(?error, "failed to stop the process");
							},
						}
					};
					tokio::select! {
						() = stop => (),
						_ = signals.recv() => (),
					}
				}
				exit_with_signal(signal);
			},
		}
	} else {
		wait_future.await
	}
	.map_err(|error| tg::error!(!error, "failed to await the process"));

	// Stop and await the view task before handling a wait error, so that the viewer cannot keep the CLI running.
	if let Some(view_task) = view_task {
		stop_view_task(view_task).await;
	}

	// Finish echoing the logged streams, which end when the process finishes, before handling the outcome.
	if let Some(echo_task) = echo_task {
		echo_task
			.await
			.map_err(|error| tg::error!(!error, "the echo task panicked"))?
			.map_err(|error| tg::error!(!error, "failed to echo the process stdio"))?;
	}
	let outcome = outcome?;

	// If verbose, return the outcome.
	if options.verbose {
		let data = outcome.to_data();
		let value = serde_json::to_value(&data)
			.map_err(|error| tg::error!(!error, "failed to serialize the outcome"))?
			.into();
		return Ok(value);
	}

	// Set the exit.
	if outcome.exit != 0 {
		cli.exit.replace(outcome.exit.into());
	}

	// Handle an error.
	if let Some(error) = outcome.error {
		let error_options = error.to_referent().options;
		let error = error
			.to_data_or_id()
			.map_left(|data| {
				Box::new(tg::error::Object::try_from_data(data).unwrap_or_else(|_| {
					tg::error::Object {
						message: Some("invalid error".to_owned()),
						..Default::default()
					}
				}))
			})
			.map_right(|id| Box::new(tg::Error::with_id(id)));
		let mut source = process.clone().map(|_| error);
		source.options.location = error_options.location;
		source.options.tokens = error_options.tokens;
		let error = tg::Error::with_object(tg::error::Object {
			message: Some("the process failed".to_owned()),
			source: Some(source),
			values: [("id".to_owned(), process.node().id().to_string())].into(),
			..Default::default()
		});
		return Err(error);
	}

	// Handle non-zero exit.
	if outcome.exit >= 1 && outcome.exit < 128 {
		return Err(tg::error!("the process exited with code {}", outcome.exit));
	}
	if outcome.exit >= 128 {
		return Err(tg::error!(
			"the process exited with signal {}",
			outcome.exit - 128
		));
	}

	// Get the output.
	let output = outcome.output.unwrap_or(tg::Value::Null);

	// Check out the output if requested.
	if let Some(path) = options.checkout.clone() {
		let artifact: tg::Artifact = output
			.try_into()
			.map_err(|_| tg::error!("expected an artifact"))?;
		let path = if let Some(path) = path {
			let path = tangram_util::fs::canonicalize_parent(path)
				.await
				.map_err(|error| tg::error!(!error, "failed to canonicalize the path"))?;
			Some(path)
		} else {
			None
		};
		let id = artifact.id();
		let options = tg::checkout::Options {
			dependencies: true,
			extension: None,
			force: options.checkout_force,
			lock: None,
			path,
		};
		let stream = artifact
			.checkout_with_instance(&client, options)
			.await
			.map_err(
				|error| tg::error!(!error, artifact = %id, "failed to create the checkout stream"),
			)?;
		let tg::checkout::Output { mut paths } = cli.render_progress_stream(stream).await.map_err(
			|error| tg::error!(!error, artifact = %id, "failed to check out the artifact"),
		)?;
		let path = paths.pop().unwrap();
		let value = path.display().to_string().into();
		return Ok(value);
	}

	Ok(output)
}

impl Signals {
	fn new() -> tg::Result<Self> {
		let hangup = signal(SignalKind::hangup())
			.map_err(|error| tg::error!(!error, "failed to install the SIGHUP handler"))?;
		let interrupt = signal(SignalKind::interrupt())
			.map_err(|error| tg::error!(!error, "failed to install the SIGINT handler"))?;
		let terminate = signal(SignalKind::terminate())
			.map_err(|error| tg::error!(!error, "failed to install the SIGTERM handler"))?;
		let signals = Self {
			hangup,
			interrupt,
			terminate,
		};
		Ok(signals)
	}

	async fn recv(&mut self) -> tg::Result<tg::process::Signal> {
		let signal = tokio::select! {
			signal = self.hangup.recv() => signal.map(|()| tg::process::Signal::SIGHUP),
			signal = self.interrupt.recv() => signal.map(|()| tg::process::Signal::SIGINT),
			signal = self.terminate.recv() => signal.map(|()| tg::process::Signal::SIGTERM),
		};
		signal.ok_or_else(|| tg::error!("the signal stream ended"))
	}
}

/// Stop a process that this run spawned, and return whether it stops.
async fn stop_process<O>(
	client: &tg::Client,
	process: &tg::Process<O>,
	ephemeral_sandbox: bool,
) -> tg::Result<bool> {
	// Load the process.
	let state = process
		.load_with_instance(client)
		.await
		.map_err(|error| tg::error!(!error, "failed to load the process"))?;

	// Destroy the sandbox that this run created for a process that no other run can hold, which also stops the processes that the process started.
	if !state.cacheable
		&& ephemeral_sandbox
		&& let Some(sandbox) = &state.sandbox
	{
		let arg = tg::sandbox::destroy::Arg {
			error: None,
			location: process.location(),
			tokens: process.tokens(),
			wait: true,
		};
		client
			.destroy_sandbox(sandbox, arg)
			.await
			.map_err(|error| tg::error!(!error, %sandbox, "failed to destroy the sandbox"))?;
		return Ok(true);
	}

	// Otherwise, cancel the process, which only releases this run's lease on a cacheable process that another run also holds.
	let options = tg::process::cancel::Options::default();
	process
		.cancel_with_instance(client, options)
		.await
		.map_err(|error| tg::error!(!error, "failed to cancel the process"))?;

	Ok(!state.cacheable)
}

/// Exit with the code for a signal without shutting down the runtime or flushing the standard output, either of which can wait indefinitely for a blocked write.
fn exit_with_signal(signal: tg::process::Signal) -> ! {
	// SAFETY: _exit has no memory safety preconditions.
	unsafe { libc::_exit(128 + i32::from(signal as u8)) }
}

async fn stop_view_task(view_task: Task<tg::Result<()>>) {
	view_task.stop();
	match view_task.wait().await {
		Ok(Ok(())) => {},
		Ok(Err(error)) => {
			tracing::warn!(?error, "failed to render the viewer");
			Cli::print_warning_message("failed to render the viewer");
		},
		Err(error) => {
			tracing::warn!(?error, "the viewer task panicked");
			Cli::print_warning_message("failed to render the viewer");
		},
	}
}
