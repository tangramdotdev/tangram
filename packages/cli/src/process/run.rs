use self::signal::handle_signals;
use crate::Cli;
use bytes::Bytes;
use crossterm::style::Stylize as _;
use futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream};
use std::{
	io::{IsTerminal as _, Read as _},
	pin::pin,
};
use tangram_client as tg;
use tangram_futures::task::{Stop, Task};
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
	pub async fn command_process_run(&mut self, args: Args) -> tg::Result<()> {
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
		stdio.close(&handle).await?;

		// Stop and await the stdio task.
		stdio_task.stop();
		stdio_task.wait().await.unwrap()?;
		drop(stdio);

		// Abort the cancel task.
		cancel_task.abort();

		// Abort the signal task.
		signal_task.abort();

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

		// Set the exit.
		let exit = wait
			.exit
			.ok_or_else(|| tg::error!("expected the exit to be set"))?;
		self.exit.replace(exit);

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
	let stream = stdin_stream();
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
			}
			stdio_task_inner(&handle, &stderr, remote, tokio::io::stderr()).await?;
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

fn stdin_stream() -> impl Stream<Item = tg::Result<Bytes>> + Send + 'static {
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
			if send.blocking_send(result).is_err() {
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
			.map_err(|source| tg::error!(!source, "failed to write chunk"))?;
		writer.flush().await.ok();
	}
	Ok(())
}
