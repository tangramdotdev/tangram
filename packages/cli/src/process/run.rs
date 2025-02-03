use crate::Cli;
use bytes::Bytes;
use crossterm::style::Stylize as _;
use futures::{
	future, stream, FutureExt as _, Stream, StreamExt as _, TryFutureExt, TryStreamExt as _,
};
use std::io::{IsTerminal, Read};
use std::pin::pin;
use tangram_client::{self as tg, Handle};
use tg::handle::Ext as _;
use tokio::io::{AsyncWrite, AsyncWriteExt as _};
use tokio_stream::wrappers::ReceiverStream;

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

#[derive(Clone)]
struct Pipes {
	remote: Option<String>,
	stdin: tg::pipe::open::Output,
	stderr: tg::pipe::open::Output,
	stdout: tg::pipe::open::Output,
}

impl Pipes {
	async fn open<H>(handle: &H, remote: Option<String>) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		// Try to get the window sizes.
		let stderr = get_window_size(libc::STDERR_FILENO)?;
		let stdin = get_window_size(libc::STDIN_FILENO)?;
		let stdout = get_window_size(libc::STDOUT_FILENO)?;

		// Open the pipes.
		let stderr = handle
			.open_pipe(tg::pipe::open::Arg {
				remote: remote.clone(),
				window_size: stderr,
			})
			.map_err(|source| tg::error!(!source, "failed to create stderr"))
			.await;
		let stdin = handle
			.open_pipe(tg::pipe::open::Arg {
				remote: remote.clone(),
				window_size: stdin,
			})
			.map_err(|source| tg::error!(!source, "failed to create stdin"))
			.await;
		let stdout = handle
			.open_pipe(tg::pipe::open::Arg {
				remote: remote.clone(),
				window_size: stdout,
			})
			.map_err(|source| tg::error!(!source, "failed to create stdout"))
			.await;

		// Handle any errors that occured when opening the pipes.
		let errored = stderr.is_err() || stdin.is_err() || stdout.is_err();
		if errored {
			for pipe in [&stderr, &stdin, &stdout] {
				let arg = tg::pipe::close::Arg {
					remote: remote.clone(),
				};
				if let Ok(pipe) = pipe {
					handle.close_pipe(&pipe.reader, arg.clone()).await.ok();
					handle.close_pipe(&pipe.writer, arg).await.ok();
				}
			}
		}

		// Return the pipes.
		Ok(Self {
			remote,
			stderr: stderr?,
			stdin: stdin?,
			stdout: stdout?,
		})
	}

	async fn close<H>(&self, handle: &H) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let arg = tg::pipe::close::Arg {
			remote: self.remote.clone(),
		};
		let (_stderr, _stdin, _stdout) = future::join3(
			handle.close_pipe(&self.stderr.reader, arg.clone()),
			handle.close_pipe(&self.stdin.writer, arg.clone()),
			handle.close_pipe(&self.stdout.reader, arg.clone()),
		)
		.await;
		Ok(())
	}
}

impl Cli {
	pub async fn command_process_run(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the reference.
		let reference = args.reference.unwrap_or_else(|| ".".parse().unwrap());

		// Open pipes.
		let remote = reference
			.options()
			.and_then(|options| options.remote.clone());
		let pipes = Pipes::open(&handle, remote)
			.await
			.map_err(|source| tg::error!(!source, "failed to open pipes"))?;

		// Get the result.
		let result = self
			.run_process(args.options, reference, args.trailing, &pipes)
			.await;

		// Close the pipes.
		pipes.close(&handle).await.ok();

		// Return the result.
		result
	}

	#[allow(dead_code)]
	async fn run_process(
		&self,
		mut options: Options,
		reference: tg::Reference,
		trailing: Vec<String>,
		pipes: &Pipes,
	) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Override the sandbox arg.
		options.spawn.sandbox = false;

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
			return Ok(());
		}

		// Spawn a task for stdout/stderr, before spawing the process.
		let output_task = tokio::spawn({
			let handle = handle.clone();
			let pipes = pipes.clone();
			async move { output_task(&handle, &pipes).await }
		});

		// Spawn the process.
		let process = self
			.spawn_process(
				options.spawn,
				reference,
				trailing,
				Some(pipes.stderr.writer.clone()),
				Some(pipes.stdin.reader.clone()),
				Some(pipes.stdout.writer.clone()),
			)
			.boxed()
			.await?;

		// Print the process.
		eprintln!("{} process {}", "info".blue().bold(), process.id());

		// Close unnused pipes.
		let arg = tg::pipe::close::Arg {
			remote: pipes.remote.clone(),
		};
		handle
			.close_pipe(&pipes.stdin.reader, arg.clone())
			.await
			.ok();
		handle
			.close_pipe(&pipes.stdout.writer, arg.clone())
			.await
			.ok();
		handle
			.close_pipe(&pipes.stderr.writer, arg.clone())
			.await
			.ok();

		// Spawn a task for stdin after starting the process.
		let input_task = tokio::spawn({
			let handle = handle.clone();
			let pipes = pipes.clone();
			async move { input_task(&handle, &pipes).await }
		});

		// Spawn a task to attempt to cancel the process on the first interrupt signal and exit the process on the second.
		let cancel_task = tokio::spawn({
			let handle = handle.clone();
			let process = process.clone();
			async move {
				tokio::signal::ctrl_c().await.unwrap();
				tokio::spawn(async move {
					let arg = tg::process::finish::Arg {
						error: Some(tg::error!("the process was explicitly canceled")),
						exit: None,
						output: None,
						remote,
						status: tg::process::Status::Canceled,
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
				std::process::exit(130);
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

		// Close pipes.
		pipes.close(&handle).await.ok();
		eprintln!("closed pipes");

		// Stop and wait for stdio.
		input_task.abort();
		output_task.await.unwrap()?;

		// Abort the cancel task.
		cancel_task.abort();

		// Get the output.
		let output = result?;

		// Return an error if appropriate.
		if let Some(source) = output.error {
			return Err(tg::error!(!source, "the process failed"));
		}
		if matches!(output.status, tg::process::Status::Canceled) {
			return Err(tg::error!("the process was canceled"));
		}
		let output_value: tg::Value = output
			.output
			.ok_or_else(|| tg::error!("expected an output"))?;

		// Print the output.
		if !output_value.is_null() {
			let stdout = std::io::stdout();
			let output = if stdout.is_terminal() {
				let options = tg::value::print::Options {
					recursive: false,
					style: tg::value::print::Style::Pretty { indentation: "  " },
				};
				output_value.print(options)
			} else {
				output_value.to_string()
			};
			println!("{output}");
		}

		match &output.exit {
			Some(tg::process::Exit::Code { code }) => {
				fork_and_die(*code)
					.map_err(|source| tg::error!(!source, "failed to fork and die"))?;
			},
			Some(tg::process::Exit::Signal { signal }) => {
				return Err(tg::error!("the process exited with signal {signal}"));
			},
			_ => (),
		}

		Ok(())
	}
}

async fn output_task<H>(handle: &H, pipes: &Pipes) -> tg::Result<()>
where
	H: tg::Handle,
{
	// Create the stdout task.
	let arg = tg::pipe::get::Arg {
		remote: pipes.remote.clone(),
	};
	let stdout = handle
		.read_pipe(&pipes.stdout.reader, arg.clone())
		.await
		.map_err(
			|source| tg::error!(!source, %stdout = pipes.stdout.reader, "failed to open stdout stream"),
		)?;
	let stdout = tokio::spawn(drain_stream_to_writer(stdout, tokio::io::stdout()));

	// Create the stdin task.
	let stderr = handle
		.read_pipe(&pipes.stderr.reader, arg.clone())
		.await
		.map_err(|source| tg::error!(!source, "failed to open stderr stream"))?;
	let stderr = tokio::spawn(drain_stream_to_writer(stderr, tokio::io::stderr()));

	// Join all tasks and return errors.
	let (stdout, stderr) = future::try_join(stdout, stderr).await.unwrap();
	stdout?;
	stderr?;
	Ok(())
}

async fn input_task<H: tg::Handle>(handle: &H, pipes: &Pipes) -> tg::Result<()> {
	// Spawn a task to read from stdin.
	let pipe = pipes.stdin.writer.clone();
	let arg = tg::pipe::post::Arg {
		remote: pipes.remote.clone(),
	};
	// Create the stream.
	let stream = stdin_stream()
		.map(|bytes| bytes.map(tg::pipe::Event::Chunk))
		.chain(stream::once(future::ok(tg::pipe::Event::End)))
		.boxed();
	handle.post_pipe(&pipe, arg, stream).await.ok();
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

fn get_window_size(fd: i32) -> tg::Result<Option<tg::pipe::WindowSize>> {
	unsafe {
		if libc::isatty(fd) == 0 {
			return Ok(None);
		}
		let mut winsize = libc::winsize {
			ws_col: 0,
			ws_row: 0,
			ws_xpixel: 0,
			ws_ypixel: 0,
		};
		if libc::ioctl(fd, libc::TIOCGWINSZ, std::ptr::addr_of_mut!(winsize)) != 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to get the window size"
			));
		}
		Ok(Some(tg::pipe::WindowSize {
			rows: winsize.ws_row,
			cols: winsize.ws_col,
			xpos: winsize.ws_xpixel,
			ypos: winsize.ws_ypixel,
		}))
	}
}

fn fork_and_die(code: i32) -> std::io::Result<()> {
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
