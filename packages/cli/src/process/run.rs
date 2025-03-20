use self::stdio::Stdio;
use crate::Cli;
use bytes::Bytes;
use crossterm::style::Stylize as _;
use futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future, stream};
use signal::handle_signals;
use std::{
	io::{IsTerminal, Read},
	pin::pin,
};
use tangram_client as tg;
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

		let remote = args.options.spawn.remote.clone().flatten();

		// Create the stdio.
		let stdio = self
			.create_stdio(remote, args.options.detach)
			.await
			.map_err(|source| tg::error!(!source, "failed to open pipes"))?;

		// Get the result.
		let result = self
			.run_process_inner(args.options, reference, args.trailing, &stdio)
			.await;

		// Close the pipes, in case the inner process errored.
		stdio.delete_io(&handle);

		// Drop stdio to restore termios.
		drop(stdio);

		// Get the output if it exists, forking to exit the current process so cleanup happens in the background.
		let Some(output) = result? else {
			fork_and_exit(0).ok();
			return Ok(());
		};

		// Print a value if it exists and is non-null.
		if let Some(value) = output.output {
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
		mut options: Options,
		reference: tg::Reference,
		trailing: Vec<String>,
		stdio: &Stdio,
	) -> tg::Result<Option<tg::process::wait::Wait>> {
		let handle = self.handle().await?;

		// Add the root to mounts.
		if !options.spawn.sandbox {
			options.spawn.mount.push(tg::process::Mount {
				source: tg::process::mount::Source::Path("/".into()),
				target: "/".into(),
				readonly: false,
			});
		}

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
			let stdin = stdio.stdin.clone();
			let stdout = stdio.stdout.clone();
			let stderr = stdio.stderr.clone();
			let remote = stdio.remote.clone();
			async move { stdio_task(&handle, stdin, stdout, stderr, remote).await }
		});

		// Spawn the process.
		let process = self
			.spawn_process(
				options.spawn,
				reference,
				trailing,
				Some(stdio.stderr.clone()),
				Some(stdio.stdin.clone()),
				Some(stdio.stdout.clone()),
			)
			.boxed()
			.await?;

		// Print the process.
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
		stdio.delete_io(&handle);

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

fn fork_and_exit(code: i32) -> std::io::Result<()> {
	let pid = unsafe { libc::fork() };
	if pid < 0 {
		return Err(std::io::Error::last_os_error());
	}
	if pid == 0 {
		Ok(())
	} else {
		std::process::exit(code);
	}
}

async fn stdio_task<H>(
	handle: &H,
	stdin: tg::process::Io,
	stdout: tg::process::Io,
	stderr: tg::process::Io,
	remote: Option<String>,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	// Spawn a task to read from stdin.
	let stream = stdin_stream();
	let stdin = tokio::spawn({
		let io = stdin;
		let remote = remote.clone();
		let handle = handle.clone();
		async move {
			match io {
				tg::process::Io::Pipe(id) => {
					let stream = stream
						.map(|bytes| bytes.map(tg::pipe::Event::Chunk))
						.chain(stream::once(future::ok(tg::pipe::Event::End)))
						.boxed();
					let arg = tg::pipe::post::Arg { remote };
					handle.post_pipe(&id, arg, stream).await.ok();
				},
				tg::process::Io::Pty(id) => {
					let stream = stream
						.map(|bytes| bytes.map(tg::pty::Event::Chunk))
						.chain(stream::once(future::ok(tg::pty::Event::End)))
						.boxed();
					let arg = tg::pty::post::Arg {
						remote,
						master: true,
					};
					handle.post_pty(&id, arg, stream).await.ok();
				},
			}
		}
	});

	// Create the stderr task if the pipe is different from stdout, to avoid duplicating output.
	let stderr = tokio::spawn({
		let stdout = stdout.clone();
		let stderr = stderr.clone();
		let handle = handle.clone();
		let remote = remote.clone();
		async move {
			if stdout == stderr {
				return Ok(());
			};
			output(&handle, &stderr, remote, tokio::io::stderr()).await
		}
	});

	let stdout = tokio::spawn({
		let stdout = stdout.clone();
		let handle = handle.clone();
		let remote = remote.clone();
		async move { output(&handle, &stdout, remote, tokio::io::stdout()).await }
	});

	// Join all tasks and return errors.
	let (stdout, stderr) = future::try_join(stdout, stderr).await.unwrap();
	stdin.abort();

	stdout?;
	stderr?;
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

async fn output<H>(
	handle: &H,
	io: &tg::process::Io,
	remote: Option<String>,
	mut writer: impl AsyncWrite + Unpin,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	let stream = match io {
		tg::process::Io::Pipe(id) => {
			let arg = tg::pipe::get::Arg { remote };
			handle
				.get_pipe_stream(id, arg)
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
		tg::process::Io::Pty(id) => {
			let arg = tg::pty::get::Arg {
				remote,
				master: false,
			};
			handle
				.get_pty_stream(id, arg)
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
