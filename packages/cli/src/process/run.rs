use crate::Cli;
use bytes::Bytes;
use crossterm::style::Stylize as _;
use futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream};
use signal::handle_signals;
use std::io::{IsTerminal, Read};
use std::pin::pin;
use stdio::Stdio;
use tangram_client as tg;
use tg::handle::Ext as _;
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

		// Get the reference.
		let reference = args.reference.unwrap_or_else(|| ".".parse().unwrap());

		// Open pipes. This has to happen first to avoid leaving pipes dangling if run_inner fails.
		let remote = args.options.spawn.remote.clone().flatten();

		let stdio = self
			.init_stdio(remote, args.options.detach)
			.await
			.map_err(|source| tg::error!(!source, "failed to open pipes"))?;

		// Get the result.
		let result = self
			.run_process_inner(args.options, reference, args.trailing, &stdio)
			.await;

		// Close the pipes, in case the inner process errored.
		stdio.close_client_half(&handle).await;

		// Drop stdio to restore termios.
		drop(stdio);

		// Get the output if it exists, forking to exit the current process so cleanup happens in the background.
		let Some(output) = result? else {
			fork_and_exit(0).ok();
			return Ok(());
		};

		// Print a value if it exists and is non-null.
		if let Some(value) = output.output {
			let value =
				tg::Value::try_from(value).map_err(|_| tg::error!("failed to get value"))?;
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

		// Return an error if appropriate.
		if let Some(source) = output.error {
			return Err(tg::error!(!source, "the process failed"));
		}

		// Check the exit status.
		match output.exit {
			Some(tg::process::Exit::Code { code }) => {
				fork_and_exit(code).ok();
				Ok(())
			},
			Some(tg::process::Exit::Signal { signal }) => {
				fork_and_exit(128 + signal).ok();
				Ok(())
			},
			None => Ok(()),
		}
	}

	#[allow(dead_code)]
	async fn run_process_inner(
		&self,
		options: Options,
		reference: tg::Reference,
		trailing: Vec<String>,
		stdio: &Stdio,
	) -> tg::Result<Option<tg::process::wait::Wait>> {
		eprintln!("client pid: {}", unsafe { libc::getpid() });

		let handle = self.handle().await?;

		// Get the remote.
		let remote = options
			.spawn
			.remote
			.clone()
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));

		// If the detach flag is set, then spawn without stdio and return None.
		if options.detach {
			let process = self
				.spawn_process(options.spawn, reference, trailing, None, None, None)
				.boxed()
				.await?;
			println!("{}", process.id());
			return Ok(None);
		}

		// Spawn a task for stdout/stderr, before spawing the process.
		let stdio_task = tokio::spawn({
			let handle = handle.clone();
			let stdin = stdio.stdin.client.clone();
			let stdout = stdio.stdout.client.clone();
			let stderr = stdio.stderr.client.clone();
			let remote = stdio.remote.clone();
			async move { stdio_task(&handle, stdin, stdout, stderr, remote).await }
		});

		// Spawn the process.
		let process = self
			.spawn_process(
				options.spawn,
				reference,
				trailing,
				Some(stdio.stderr.server.clone()),
				Some(stdio.stdin.server.clone()),
				Some(stdio.stdout.server.clone()),
			)
			.boxed()
			.await?;

		// Print the process.
		eprint!("{} process {}\r\n", "info".blue().bold(), process.id());

		// Close unnused pipes.
		stdio.close_server_half(&handle).await;

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
						status: tg::process::Status::Failed,
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
				fork_and_exit(130).ok();
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

		// Spawn a task to wait for the output.
		let output = tokio::spawn({
			let handle = handle.clone();
			let process = process.clone();
			async move {
				process
					.wait(&handle)
					.await
					.map_err(|source| tg::error!(!source, "failed to wait for the process"))
			}
		});

		// Wait for the output.
		let result = output
			.await
			.unwrap()
			.map_err(|source| tg::error!(!source, "failed to wait for the output"));

		// Close stdio.
		stdio.close_client_half(&handle).await;

		// Stop and wait for stdio.
		stdio_task.await.unwrap()?;

		// Abort the cancel task.
		cancel_task.abort();

		// Abort the signal task.
		signal_task.abort();

		// Return the output.
		result.map(Some)
	}
}

async fn stdio_task<H>(
	handle: &H,
	stdin: tg::pipe::Id,
	stdout: tg::pipe::Id,
	stderr: tg::pipe::Id,
	remote: Option<String>,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	// Spawn a task to read from stdin.
	let stream = stdin_stream()
		.map(|bytes| bytes.map(tg::pipe::Event::Chunk))
		.chain(stream::once(future::ok(tg::pipe::Event::End)))
		.boxed();
	let stdin = tokio::spawn({
		let pipe = stdin;
		let remote = remote.clone();
		let handle = handle.clone();
		async move {
			let arg = tg::pipe::post::Arg { remote };
			handle.post_pipe(&pipe, arg, stream).await.ok();
		}
	});

	// Create the output streams.
	let arg = tg::pipe::get::Arg {
		remote: remote.clone(),
	};

	// Create the stderr task if the pipe is different from stdout, to avoid duplicating output.
	let stderr = if stdout != stderr {
		handle
			.read_pipe(&stderr, arg.clone())
			.await
			.map_err(|source| tg::error!(!source, %stderr, "failed to open stderr stream"))?
			.left_stream()
	} else {
		stream::empty().right_stream()
	};
	let stderr = tokio::spawn(drain_stream_to_writer(stderr, tokio::io::stderr()));

	// Create the stdout stream.
	let stdout = handle
		.read_pipe(&stdout, arg.clone())
		.await
		.map_err(|source| tg::error!(!source, %stdout, "failed to open stdout stream"))?;
	let stdout = tokio::spawn(drain_stream_to_writer(stdout, tokio::io::stdout()));

	// Join all tasks and return errors.
	let (stdout, stderr) = future::try_join(stdout, stderr).await.unwrap();
	stdin.abort();

	stdout?;
	stderr?;
	Ok(())
}

async fn drain_stream_to_writer(
	stream: impl Stream<Item = tg::Result<Bytes>> + Send + 'static,
	mut writer: impl AsyncWrite + Unpin + Send + 'static,
) -> tg::Result<()> {
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

	// Convert to a stream.
	ReceiverStream::new(recv)
}

fn fork_and_exit(code: i32) -> std::io::Result<()> {
	unsafe {
		let pid = libc::fork();
		if pid < 0 {
			return Err(std::io::Error::last_os_error());
		} else if pid == 0 {
			return Ok(());
		} else {
			std::process::exit(code)
		}
	}
}
