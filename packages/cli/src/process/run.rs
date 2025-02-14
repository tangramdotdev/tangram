use std::io::{IsTerminal, Read};

use crate::Cli;
use bytes::Bytes;
use crossterm::style::Stylize as _;
use futures::{future, FutureExt as _, Stream, StreamExt as _, TryStreamExt as _};
use std::pin::pin;
use tangram_client as tg;
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
		// Open all the pipes.
		let arg = tg::pipe::open::Arg {
			remote: remote.clone(),
		};
		let (stderr, stdin, stdout) = future::join3(
			handle.open_pipe(arg.clone()),
			handle.open_pipe(arg.clone()),
			handle.open_pipe(arg.clone()),
		)
		.await;
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
		let stderr = dbg!(stderr?);
		let stdin = dbg!(stdin?);
		let stdout = dbg!(stdout?);
		Ok(Self {
			remote,
			stderr,
			stdin,
			stdout,
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
		let pipes = Pipes::open(&handle, remote).await?;

		// Get the result.
		let result = self
			.run_process(args.options, reference, args.trailing, &pipes)
			.await;

		// Close the pipes.
		pipes.close(&handle).await.ok();
		eprintln!("closed pipes for good");

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

		// Otherwise bind the stdio and print the process ID.
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

		// Spawn a task for stdio.
		let stdio = tokio::spawn({
			let handle = handle.clone();
			let pipes = pipes.clone();
			async move { stdio_task(&handle, &pipes).await }
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
		eprintln!("got result.");

		// Close pipes.
		pipes.close(&handle).await.ok();
		eprintln!("closed pipes");

		// Stop and wait for the stdio task.
		stdio.await.ok();
		eprintln!("awaited stdio");

		// Abort the cancel task.
		cancel_task.abort();
		eprintln!("aborted, exiting");

		// Get the output.
		let output = result?;

		// Return an error if appropriate.
		if let Some(source) = output.error {
			eprintln!("exiting");
			return Err(tg::error!(!source, "the process failed"));
		}
		if matches!(output.status, tg::process::Status::Canceled) {
			return Err(tg::error!("the process was canceled"));
		}
		match &output.exit {
			Some(tg::process::Exit::Code { code }) if *code != 0 => {
				return Err(tg::error!("the process exited with code {code}"));
			},
			Some(tg::process::Exit::Signal { signal }) => {
				return Err(tg::error!("the process exited with signal {signal}"));
			},
			_ => (),
		}
		let output: tg::Value = output
			.output
			.ok_or_else(|| tg::error!("expected an output"))?;

		// Print the output.
		if !output.is_null() {
			let stdout = std::io::stdout();
			let output = if stdout.is_terminal() {
				let options = tg::value::print::Options {
					recursive: false,
					style: tg::value::print::Style::Pretty { indentation: "  " },
				};
				output.print(options)
			} else {
				output.to_string()
			};
			println!("{output}");
		}

		Ok(())
	}
}

async fn stdio_task<H>(handle: &H, pipes: &Pipes) -> tg::Result<()>
where
	H: tg::Handle,
{
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

	// Spawn a task to read from stdin.
	let stdin = tokio::spawn({
		let handle = handle.clone();
		let pipe = pipes.stdin.writer.clone();
		let arg = tg::pipe::write::Arg {
			remote: pipes.remote.clone(),
		};
		async move {
			// Create the stream.
			let stream = stdin_stream().boxed();
			handle.write_pipe(&pipe, arg, stream).await
		}
	});

	// Create the stdout task.
	let arg = tg::pipe::read::Arg {
		remote: pipes.remote.clone(),
	};
	let stdout = handle.read_pipe(&pipes.stdout.reader, arg.clone()).await?;
	let stdout = tokio::spawn(drain_stream_to_writer(stdout, tokio::io::stdout()));

	// Create the stdin task.
	let stderr = handle.read_pipe(&pipes.stderr.reader, arg.clone()).await?;
	let stderr = tokio::spawn(drain_stream_to_writer(stderr, tokio::io::stderr()));

	// Join all tasks and return errors.
	let (stdout, stderr) = future::try_join(stdout, stderr).await.unwrap();
	stdin.abort();
	eprintln!("awaited stdout, stderr");

	stdout?;
	stderr?;
	Ok(())
}
async fn drain_stream_to_writer(
	stream: impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static,
	mut writer: impl AsyncWrite + Unpin + Send + 'static,
) -> tg::Result<()> {
	let mut stream = pin!(stream);
	while let Some(event) = stream.try_next().await? {
		match event {
			tg::pipe::Event::Chunk(chunk) => {
				writer
					.write_all(&chunk)
					.await
					.map_err(|source| tg::error!(!source, "failed to write chunk"))?;
			},
			tg::pipe::Event::End => break,
		}
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
					eprintln!("sent chunk {chunk:?}");
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
