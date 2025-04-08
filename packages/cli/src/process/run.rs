use self::signal::handle_signals;
use crate::Cli;
use bytes::Bytes;
use crossterm::style::Stylize as _;
use futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future, stream};
use std::{
	io::{IsTerminal as _, Read as _},
	pin::pin,
};
use tangram_client as tg;
use tangram_futures::task::Task;
use tokio::io::{AsyncWrite, AsyncWriteExt as _};
use tokio_stream::wrappers::ReceiverStream;

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
	/// If this flag is set, then exit immediately instead of waiting for the process to finish.
	#[arg(short, long)]
	pub detach: bool,

	#[command(flatten)]
	pub spawn: crate::process::spawn::Options,
}

impl Cli {
	pub async fn command_process_run(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.options
			.spawn
			.remote
			.clone()
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Get the reference.
		let reference = args.reference.unwrap_or_else(|| ".".parse().unwrap());

		// Create the stdio.
		let stdio = self
			.create_stdio(remote.clone(), &args.options)
			.await
			.map_err(|source| tg::error!(!source, "failed to open pipes"))?;

		// If the detach flag is set, then spawn without stdio and return.
		if args.options.detach {
			let process = self
				.spawn_process(
					args.options.spawn,
					reference,
					args.trailing,
					None,
					None,
					None,
				)
				.boxed()
				.await?;
			println!("{}", process.id());
			return Ok(());
		}

		// Spawn a task for stdout and stderr before spawing the process.
		let stdio_task = tokio::spawn({
			let handle = handle.clone();
			let stdin = stdio.stdin.clone();
			let stdout = stdio.stdout.clone();
			let stderr = stdio.stderr.clone();
			let remote = stdio.remote.clone();
			async move { stdio_task(&handle, stdin, stdout, stderr, remote).await }
		});

		// Spawn the process.
		let process = self
			.spawn_process(
				args.options.spawn,
				reference,
				args.trailing,
				Some(stdio.stderr.clone()),
				Some(stdio.stdin.clone()),
				Some(stdio.stdout.clone()),
			)
			.boxed()
			.await?;

		// Print the process ID.
		eprint!("{} process {}\r\n", "info".blue().bold(), process.id());

		// Spawn a task to attempt to cancel the process on the first interrupt signal and exit the process on the second.
		let cancel_task = tokio::spawn({
			let handle = handle.clone();
			let process = process.clone();
			let remote = remote.clone();
			async move {
				tokio::signal::ctrl_c().await.unwrap();
				tokio::spawn(async move {
					let arg = tg::process::finish::Arg {
						error: Some(tg::error!("the process was explicitly canceled")),
						exit: None,
						output: None,
						remote,
						status: tg::process::Status::Finished,
					};
					process
						.finish(&handle, arg)
						.await
						.inspect_err(|error| {
							tracing::error!(?error, "failed to cancel the process");
						})
						.ok();
				});
				tokio::signal::ctrl_c().await.unwrap();
				std::process::abort();
			}
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
		self.close_stdio(&stdio).await?;

		// Stop and await the stdio task.
		stdio_task.await.unwrap()?;

		// Abort the cancel task.
		cancel_task.abort();

		// Abort the signal task.
		signal_task.abort();

		// Restore termios.
		drop(stdio);

		// Handle the result.
		let wait = result.map_err(|source| tg::error!(!source, "failed to await the process"))?;

		// Return an error if necessary.
		if let Some(error) = wait.error {
			return Err(error);
		}

		// Print the output if it is set and is not null.
		if let Some(value) = wait.output {
			if !value.is_null() {
				let stdout = std::io::stdout();
				let output = if stdout.is_terminal() {
					let options = tg::value::print::Options {
						recursive: false,
						style: tg::value::print::Style::Pretty { indentation: "  " },
					};
					value.print(options)
				} else {
					value.to_string()
				};
				println!("{output}");
			}
		}

		// Fork and exit.
		let code = match wait.exit {
			Some(tg::process::Exit::Code { code }) => code,
			Some(tg::process::Exit::Signal { signal }) => 128 + signal,
			None => {
				return Err(tg::error!("expected the exit to be set"));
			},
		};
		let pid = unsafe { libc::fork() };
		if pid < 0 {
			let error = std::io::Error::last_os_error();
			return Err(tg::error!(!error, "failed to fork"));
		}
		if pid > 0 {
			std::process::exit(code);
		}

		Ok(())
	}
}

async fn stdio_task<H>(
	handle: &H,
	stdin: tg::process::Stdio,
	stdout: tg::process::Stdio,
	stderr: tg::process::Stdio,
	remote: Option<String>,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	// Spawn stdin task.
	let stream = stdin_stream();
	let stdin_task = Task::spawn(|stop| {
		let remote = remote.clone();
		let handle = handle.clone();
		async move {
			match stdin {
				tg::process::Stdio::Pipe(id) => {
					let stop = async move { stop.wait().await };
					let stream = stream
						.map(|bytes| bytes.map(tg::pipe::Event::Chunk))
						.take_until(stop)
						.chain(stream::once(future::ok(tg::pipe::Event::End)))
						.boxed();
					let arg = tg::pipe::write::Arg { remote };
					handle.write_pipe(&id, arg, stream).await?;
				},
				tg::process::Stdio::Pty(id) => {
					let stop = async move { stop.wait().await };
					let stream = stream
						.map(|bytes| bytes.map(tg::pty::Event::Chunk))
						.take_until(stop)
						.chain(stream::once(future::ok(tg::pty::Event::End)))
						.boxed();
					let arg = tg::pty::write::Arg {
						master: true,
						remote,
					};
					handle.write_pty(&id, arg, stream).await?;
				},
			}
			Ok::<_, tg::Error>(())
		}
	});

	// Create the stdout task.
	let stdout_task = tokio::spawn({
		let stdout = stdout.clone();
		let handle = handle.clone();
		let remote = remote.clone();
		async move {
			stdio_task_inner(&handle, &stdout, remote, tokio::io::stdout()).await?;
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
			};
			stdio_task_inner(&handle, &stderr, remote, tokio::io::stderr()).await?;
			Ok::<_, tg::Error>(())
		}
	});

	// Join the stdout and stderr tasks.
	let (stdout_result, stderr_result) = future::join(stdout_task, stderr_task).await;

	// Stop and await the stdin task.
	stdin_task.stop();
	stdin_task.wait().await.unwrap()?;

	// Return errors from the stdout and stderr tasks.
	stdout_result.unwrap()?;
	stderr_result.unwrap()?;

	Ok(())
}

fn stdin_stream() -> ReceiverStream<tg::Result<Bytes>> {
	// Create a send/receive pair for sending stdin chunks. The channel is bounded to 1 to avoid buffering stdin messages.
	let (send, recv) = tokio::sync::mpsc::channel(1);

	// Spawn the stdin thread and detach it. This is necessary because the read from stdin cannot be interrupted or canceled.
	std::thread::spawn(move || {
		let mut stdin = std::io::stdin();
		loop {
			let mut buf = vec![0u8; 4096];
			let result = match stdin.read(&mut buf) {
				Ok(0) => break,
				Ok(n) => {
					let chunk = Bytes::copy_from_slice(&buf[0..n]);
					Ok(chunk)
				},
				Err(source) => Err(tg::error!(!source, "failed to read stdin")),
			};
			if let Err(_error) = send.blocking_send(result) {
				break;
			}
		}
	});

	ReceiverStream::new(recv)
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
				remote,
				master: false,
			};
			handle
				.read_pty(id, arg)
				.await?
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
			.map_err(|source| tg::error!(!source, "failed to write chunk"))?;
		writer.flush().await.ok();
	}
	Ok(())
}
