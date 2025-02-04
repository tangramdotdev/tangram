use std::io::IsTerminal;

use crate::Cli;
use bytes::Bytes;
use crossterm::style::Stylize;
use futures::{future, stream, FutureExt, Stream, StreamExt, TryStreamExt};
use std::pin::pin;
use tangram_client as tg;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

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
	stdin: tg::pipe::Id,
	stderr: tg::pipe::Id,
	stdout: tg::pipe::Id,
}

impl Pipes {
	async fn open<H>(handle: &H) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		// Open all the pipes.
		let (stderr, stdin, stdout) =
			future::join3(handle.open_pipe(), handle.open_pipe(), handle.open_pipe()).await;
		let errored = stderr.is_err() || stdin.is_err() || stdout.is_err();
		if errored {
			for pipe in [&stderr, &stdin, &stdout] {
				if let Ok(pipe) = pipe {
					handle.close_pipe(&pipe.id).await.ok();
				}
			}
		}
		let stderr = stderr?.id;
		let stdin = stdin?.id;
		let stdout = stdout?.id;
		Ok(Self {
			stderr,
			stdin,
			stdout,
		})
	}

	async fn close<H>(&self, handle: &H) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let (_stderr, _stdin, _stdout) = future::join3(
			handle.close_pipe(&self.stderr),
			handle.close_pipe(&self.stdin),
			handle.close_pipe(&self.stdout),
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
		let pipes = Pipes::open(&handle).await?;

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

		// Otherwise bind the stdio and print the process ID.
		let process = self
			.spawn_process(
				options.spawn,
				reference,
				trailing,
				Some(pipes.stderr.clone()),
				Some(pipes.stdin.clone()),
				Some(pipes.stdout.clone()),
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

		// Wait for the output and stdio tasks.
		let (_input, output) = future::join(stdio, output).await;
		let output = output
			.unwrap()
			.map_err(|source| tg::error!(!source, "failed to wait for the output"))?;

		// Abort the cancel task.
		cancel_task.abort();

		// Return an error if appropriate.
		if let Some(source) = output.error {
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
	let stdin = tokio::spawn({
		let handle = handle.clone();
		let pipe = pipes.stdin.clone();
		async move {
			let stream = chunk_stream_from_reader(tokio::io::stdin())
				.map_ok(tg::pipe::Event::Chunk)
				.chain(stream::once(future::ok(tg::pipe::Event::End)))
				.boxed();
			handle.write_pipe(&pipe, stream).await?;
			Ok::<_, tg::Error>(())
		}
	});

	let stdout = handle.read_pipe(&pipes.stdout).await?;
	let stderr = handle.read_pipe(&pipes.stderr).await?;
	let stdout = tokio::spawn(drain_stream_to_writer(stdout, tokio::io::stdout()));
	let stderr = tokio::spawn(drain_stream_to_writer(stderr, tokio::io::stderr()));

	let (stdin, stdout, stderr) = future::try_join3(stdin, stdout, stderr).await.unwrap();
	stdin?;
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

fn chunk_stream_from_reader(
	reader: impl AsyncRead + Unpin + Send + 'static,
) -> impl Stream<Item = tg::Result<Bytes>> + Send + 'static {
	let buffer = vec![0u8; 4096];
	stream::try_unfold((reader, buffer), |(mut reader, mut buffer)| async move {
		let size = reader
			.read(&mut buffer)
			.await
			.map_err(|source| tg::error!(!source, "failed to read"))?;
		if size == 0 {
			return Ok(None);
		}
		let chunk = Bytes::copy_from_slice(&buffer[0..size]);
		Ok(Some((chunk, (reader, buffer))))
	})
}
