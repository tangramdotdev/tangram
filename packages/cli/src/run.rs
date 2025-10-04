use {
	self::signal::handle_signals,
	crate::Cli,
	anstream::eprintln,
	crossterm::style::Stylize as _,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future, stream},
	std::{path::PathBuf, pin::pin},
	tangram_client::{self as tg, prelude::*},
	tangram_futures::task::{Stop, Task},
	tokio::io::{AsyncWrite, AsyncWriteExt as _},
};

mod signal;
mod stdio;

/// Spawn and await an unsandboxed process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	/// The reference to the command.
	#[arg(index = 1)]
	pub reference: Option<tg::Reference>,

	/// Set arguments.
	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Options {
	/// If this flag is set, then build the specified target and run its output.
	#[arg(long, short)]
	pub build: bool,

	/// The view to display while building.
	#[arg(long, default_value = "inline")]
	pub build_view: crate::build::View,

	/// Whether to check out the output.
	#[allow(clippy::option_option)]
	#[arg(long, require_equals = true, short)]
	pub checkout: Option<Option<PathBuf>>,

	/// Whether to overwrite an existing file system object at the path.
	#[arg(long, requires = "checkout")]
	pub checkout_force: bool,

	/// If this flag is set, then exit immediately instead of waiting for the process to finish.
	#[arg(long, short)]
	pub detach: bool,

	/// Set the path to use for the executable.
	#[arg(long, short = 'x')]
	pub executable_path: Option<PathBuf>,

	/// Whether to print blobs.
	#[arg(long)]
	pub print_blobs: bool,

	/// The depth with which to print the output.
	#[arg(default_value = "0", long)]
	pub print_depth: crate::object::get::Depth,

	/// Whether to pretty print the output.
	#[arg(long)]
	pub print_pretty: Option<bool>,

	#[command(flatten)]
	pub spawn: crate::process::spawn::Options,
}

impl Cli {
	pub async fn command_run(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		let Args {
			options,
			reference,
			trailing,
		} = args;

		// Get the reference.
		let reference = reference.unwrap_or_else(|| ".".parse().unwrap());

		// If the build flag is set, then build and get the output.
		let reference = if options.build {
			// Spawn the process.
			let spawn = crate::process::spawn::Options {
				sandbox: crate::process::spawn::Sandbox::new(Some(true)),
				remote: options.spawn.remote.clone(),
				..Default::default()
			};
			let crate::process::spawn::Output { process, .. } = self
				.spawn(spawn, reference, vec![], None, None, None)
				.boxed()
				.await?;

			// Print the process.
			if !self.args.quiet {
				eprint!("{} {}", "info".blue().bold(), process.item().id());
				if let Some(token) = process.item().token() {
					eprint!(" {token}");
				}
				eprintln!();
			}

			// Get the process's status.
			let status = process
				.item()
				.status(&handle)
				.await?
				.try_next()
				.await?
				.ok_or_else(|| tg::error!("failed to get the status"))?;

			// If the process is finished, then get the process's output.
			let wait = if status.is_finished() {
				let output = process
					.item()
					.wait(&handle)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the output"))?;
				Some(output)
			} else {
				None
			};

			// If the process is not finished, then wait for it to finish while showing the viewer if enabled.
			let wait = if let Some(wait) = wait {
				wait
			} else {
				// Spawn the view task.
				let view_task = {
					let handle = handle.clone();
					let root = process.clone().map(crate::viewer::Item::Process);
					let task = Task::spawn_blocking(move |stop| {
						let local_set = tokio::task::LocalSet::new();
						let runtime = tokio::runtime::Builder::new_current_thread()
							.worker_threads(1)
							.enable_all()
							.build()
							.unwrap();
						local_set
							.block_on(&runtime, async move {
								let viewer_options = crate::viewer::Options {
									auto_expand_and_collapse_processes: true,
									show_process_commands: false,
								};
								let mut viewer =
									crate::viewer::Viewer::new(&handle, root, viewer_options);
								match options.build_view {
									crate::build::View::None => (),
									crate::build::View::Inline => {
										viewer.run_inline(stop).await?;
									},
									crate::build::View::Fullscreen => {
										viewer.run_fullscreen(stop).await?;
									},
								}
								Ok::<_, tg::Error>(())
							})
							.unwrap();
					});
					Some(task)
				};

				// Spawn a task to attempt to cancel the process on the first interrupt signal and exit the process on the second.
				let cancel_task = tokio::spawn({
					let handle = handle.clone();
					let process = process.clone();
					async move {
						tokio::signal::ctrl_c().await.unwrap();
						tokio::spawn(async move {
							process
								.item()
								.cancel(&handle)
								.await
								.inspect_err(|error| {
									tracing::error!(?error, "failed to cancel the process");
								})
								.ok();
						});
						tokio::signal::ctrl_c().await.unwrap();
						std::process::exit(130);
					}
				});

				// Await the process.
				let result = process.item().wait(&handle).await;

				// Abort the cancel task.
				cancel_task.abort();

				// Stop and await the view task.
				if let Some(view_task) = view_task {
					view_task.stop();
					view_task.wait().await.unwrap();
				}

				result?
			};

			// Set the exit.
			if wait.exit != 0 {
				self.exit.replace(wait.exit);
			}

			// Handle an error.
			if let Some(error) = wait.error {
				let error = tg::Error {
					message: Some("the process failed".to_owned()),
					source: Some(process.clone().map(|_| Box::new(error))),
					..Default::default()
				};
				return Err(error);
			}

			// Handle non-zero exit.
			if wait.exit > 1 && wait.exit < 128 {
				return Err(tg::error!("the process exited with code {}", wait.exit));
			}
			if wait.exit >= 128 {
				return Err(tg::error!(
					"the process exited with signal {}",
					wait.exit - 128
				));
			}

			// Get the output.
			let output = wait.output.unwrap_or(tg::Value::Null);

			let object = output
				.try_unwrap_object()
				.ok()
				.ok_or_else(|| tg::error!("expected the build to output an object"))?;
			let id = object.id();
			tg::Reference::with_object(id)
		} else {
			reference
		};

		// Handle the executable path.
		let reference = if let Some(path) = &options.executable_path {
			let referent = self.get_reference(&reference).await?;
			let directory = referent
				.item
				.right()
				.ok_or_else(|| tg::error!("expected an object"))?
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			let artifact = directory.get(&handle, path).await?;
			tg::Reference::with_object(artifact.id().into())
		} else {
			reference
		};

		// Get the remote.
		let remote = options
			.spawn
			.remote
			.clone()
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Create the stdio.
		let stdio = self
			.create_stdio(remote.clone(), &options)
			.await
			.map_err(|source| tg::error!(!source, "failed to create stdio"))?;

		// Spawn the process.
		let crate::process::spawn::Output { process, output } = self
			.spawn(
				options.spawn,
				reference,
				trailing,
				Some(stdio.stderr.clone()),
				Some(stdio.stdin.clone()),
				Some(stdio.stdout.clone()),
			)
			.boxed()
			.await?;

		// If the detach flag is set, then print the process ID and return.
		if options.detach {
			Self::print_json(&output, None).await?;
			return Ok(());
		}

		// Print the process.
		if !self.args.quiet {
			eprint!("{} {}", "info".blue().bold(), process.item().id());
			if let Some(token) = process.item().token() {
				eprint!(" {token}");
			}
			eprintln!();
		}

		// Enable raw mode.
		stdio.set_raw_mode()?;

		// Spawn the stdio task.
		let stdio_task = Task::spawn({
			let handle = handle.clone();
			let stdin = stdio.stdin.clone();
			let stdout = stdio.stdout.clone();
			let stderr = stdio.stderr.clone();
			let remote = stdio.remote.clone();
			|stop| async move { stdio_task(&handle, stop, stdin, stdout, stderr, remote).await }
		});

		// Spawn a task to handle signals.
		let signal_task = tokio::spawn({
			let handle = handle.clone();
			let process = process.item().id().clone();
			let remote = remote.clone();
			async move {
				handle_signals(&handle, &process, remote).await.ok();
			}
		});

		// Await the process.
		let result = process
			.item()
			.wait(&handle)
			.await
			.map_err(|source| tg::error!(!source, "failed await the process"));

		// Close stdio.
		stdio.close(&handle).await?;

		// Stop and await the stdio task.
		stdio_task.stop();
		stdio_task.wait().await.unwrap()?;
		drop(stdio);

		// Abort the signal task.
		signal_task.abort();

		// Handle the result.
		let wait = result.map_err(|source| tg::error!(!source, "failed to await the process"))?;

		// Set the exit.
		if wait.exit != 0 {
			self.exit.replace(wait.exit);
		}

		// Handle an error.
		if let Some(error) = wait.error {
			let error = tg::Error {
				message: Some("the process failed".to_owned()),
				source: Some(process.clone().map(|_| Box::new(error))),
				..Default::default()
			};
			return Err(error);
		}

		// Handle non-zero exit.
		if wait.exit > 1 && wait.exit < 128 {
			return Err(tg::error!("the process exited with code {}", wait.exit));
		}
		if wait.exit >= 128 {
			return Err(tg::error!(
				"the process exited with signal {}",
				wait.exit - 128
			));
		}

		// Get the output.
		let output = wait.output.unwrap_or(tg::Value::Null);

		// Check out the output if requested.
		if let Some(path) = options.checkout {
			// Get the artifact.
			let artifact: tg::Artifact = output
				.clone()
				.try_into()
				.map_err(|_| tg::error!("expected an artifact"))?;

			// Get the path.
			let path = if let Some(path) = path {
				let path = std::path::absolute(path)
					.map_err(|source| tg::error!(!source, "failed to get the path"))?;
				Some(path)
			} else {
				None
			};

			// Check out the artifact.
			let artifact = artifact.id();
			let arg = tg::checkout::Arg {
				artifact,
				dependencies: path.is_some(),
				force: options.checkout_force,
				lock: false,
				path,
			};
			let stream = handle.checkout(arg).await?;
			let tg::checkout::Output { path, .. } = self
				.render_progress_stream(stream)
				.await
				.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;

			// Print the path.
			println!("{}", path.display());

			return Ok(());
		}

		// Print the output.
		if !output.is_null() {
			Self::print_value(
				&handle,
				&output,
				options.print_depth,
				options.print_pretty,
				options.print_blobs,
			)
			.await?;
		}

		Ok(())
	}
}

async fn stdio_task<H>(
	handle: &H,
	stop: Stop,
	stdin: tg::process::Stdio,
	stdout: tg::process::Stdio,
	stderr: tg::process::Stdio,
	remote: Option<String>,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	// Spawn stdin task.
	let stream = crate::util::stdio::stdin_stream();
	let stdin_task = tokio::spawn({
		let remote = remote.clone();
		let handle = handle.clone();
		async move {
			let result = match stdin {
				tg::process::Stdio::Pipe(id) => {
					let stream = stream
						.map(|bytes| bytes.map(tg::pipe::Event::Chunk))
						.chain(stream::once(future::ok(tg::pipe::Event::End)))
						.take_until(stop.wait())
						.boxed();
					let arg = tg::pipe::write::Arg { remote };
					let handle = handle.clone();
					async move { handle.write_pipe(&id, arg, stream).await }.boxed()
				},
				tg::process::Stdio::Pty(id) => {
					let stream = stream
						.map(|bytes| bytes.map(tg::pty::Event::Chunk))
						.chain(stream::once(future::ok(tg::pty::Event::End)))
						.take_until(stop.wait())
						.boxed();
					let arg = tg::pty::write::Arg {
						master: true,
						remote,
					};
					let handle = handle.clone();
					async move { handle.write_pty(&id, arg, stream).await }.boxed()
				},
			};
			let stop = stop.wait();
			match future::select(pin!(result), pin!(stop)).await {
				future::Either::Left((result, _)) => result,
				future::Either::Right(_) => Ok(()),
			}
		}
	});

	// Create the stdout task.
	let stdout_task = tokio::spawn({
		let stdout = stdout.clone();
		let handle = handle.clone();
		let remote = remote.clone();
		async move {
			let writer = tokio::io::BufWriter::new(tokio::io::stdout());
			stdio_task_inner(&handle, &stdout, remote, writer).await?;
			Ok::<_, tg::Error>(())
		}
	});

	// Create the stderr task.
	let stderr_task = tokio::spawn({
		let stdout = stdout.clone();
		let stderr = stderr.clone();
		let handle = handle.clone();
		let remote = remote.clone();
		async move {
			if stdout == stderr {
				return Ok(());
			}
			let writer = tokio::io::BufWriter::new(tokio::io::stderr());
			stdio_task_inner(&handle, &stderr, remote, writer).await?;
			Ok::<_, tg::Error>(())
		}
	});

	// Join the stdout and stderr tasks.
	let (stdout_result, stderr_result, stdin_result) =
		future::join3(stdout_task, stderr_task, stdin_task).await;

	// Return errors from the stdout and stderr tasks.
	stdout_result.unwrap()?;
	stderr_result.unwrap()?;
	stdin_result.unwrap()?;

	Ok(())
}

async fn stdio_task_inner<H>(
	handle: &H,
	stdio: &tg::process::Stdio,
	remote: Option<String>,
	mut writer: impl AsyncWrite + Unpin,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	let stream = match stdio {
		tg::process::Stdio::Pipe(id) => {
			let arg = tg::pipe::read::Arg { remote };
			handle
				.read_pipe(id, arg)
				.await?
				.take_while(|chunk| future::ready(!matches!(chunk, Ok(tg::pipe::Event::End))))
				.try_filter_map(|event| {
					future::ok({
						if let tg::pipe::Event::Chunk(chunk) = event {
							Some(chunk)
						} else {
							None
						}
					})
				})
				.left_stream()
		},
		tg::process::Stdio::Pty(id) => {
			let arg = tg::pty::read::Arg {
				master: true,
				remote,
			};
			handle
				.read_pty(id, arg)
				.await?
				.take_while(|chunk| future::ready(!matches!(chunk, Ok(tg::pty::Event::End))))
				.try_filter_map(|event| {
					future::ok({
						if let tg::pty::Event::Chunk(chunk) = event {
							Some(chunk)
						} else {
							None
						}
					})
				})
				.right_stream()
		},
	};
	let mut stream = pin!(stream);
	while let Some(chunk) = stream.try_next().await? {
		writer
			.write_all(&chunk)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the chunk"))?;
		writer
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush the writer"))?;
	}
	Ok(())
}
