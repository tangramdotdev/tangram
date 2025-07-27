use self::signal::handle_signals;
use crate::Cli;
use anstream::eprintln;
use crossterm::style::Stylize as _;
use futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future, stream};
use std::{path::PathBuf, pin::pin};
use tangram_client as tg;
use tangram_futures::task::{Stop, Task};
use tokio::io::{AsyncWrite, AsyncWriteExt as _};

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
	#[arg(short, long)]
	pub build: bool,

	/// If this flag is set, then exit immediately instead of waiting for the process to finish.
	#[arg(short, long)]
	pub detach: bool,

	/// Set the path to use for the executable.
	#[arg(short = 'x', long)]
	pub executable_path: Option<PathBuf>,

	/// The depth with which to print the output.
	#[arg(long, default_value = "0")]
	pub print_depth: crate::object::get::Depth,

	#[command(flatten)]
	pub spawn: crate::process::spawn::Options,
}

impl Cli {
	pub async fn command_run(&mut self, args: Args) -> tg::Result<()> {
		// Get the reference.
		let reference = args.reference.unwrap_or_else(|| ".".parse().unwrap());

		// Run.
		Box::pin(self.run(args.options, reference, args.trailing)).await?;

		Ok(())
	}

	pub async fn run(
		&mut self,
		options: Options,
		reference: tg::Reference,
		trailing: Vec<String>,
	) -> tg::Result<()> {
		let handle = self.handle().await?;

		// If the build flag is set, then build and get the output.
		let reference = if options.build {
			let spawn = crate::process::spawn::Options {
				remote: options.spawn.remote.clone(),
				..Default::default()
			};
			let options = crate::build::Options {
				checkout: None,
				detach: false,
				print_depth: crate::object::get::Depth::Finite(0),
				spawn,
				view: crate::build::View::default(),
			};
			let Some(output) = self.build(options, reference, vec![], false).await? else {
				return Ok(());
			};
			let object = output
				.try_unwrap_object()
				.ok()
				.ok_or_else(|| tg::error!("expected the build to output an object"))?;
			let id = object.id();
			tg::Reference::with_object(&id)
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
			tg::Reference::with_object(&artifact.id().into())
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
		let (referent, process) = self
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
			println!("{}", process.id());
			return Ok(());
		}

		// Print the process.
		if !self.args.quiet {
			eprint!("{} {}", "info".blue().bold(), process.id());
			if let Some(token) = process.token() {
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
			let process = process.id().clone();
			let remote = remote.clone();
			async move {
				handle_signals(&handle, &process, remote).await.ok();
			}
		});

		// Await the process.
		let result = process
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

		// Print the error.
		if let Some(error) = wait.error {
			eprintln!("{} the process failed", "error".red().bold());
			let error = referent.map(|_| error);
			self.print_error(error).await;
		}

		// Print the output.
		if let Some(value) = wait.output {
			if !value.is_null() {
				Self::print_output(&value, options.print_depth);
			}
		}

		// Set the exit.
		self.exit.replace(wait.exit);

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
