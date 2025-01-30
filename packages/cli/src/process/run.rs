use crate::Cli;
use bytes::Bytes;
use crossterm::style::Stylize as _;
use futures::{future, stream, StreamExt, TryStreamExt as _};
use std::{
	io::{IsTerminal as _, Read as _},
	path::PathBuf,
	pin::pin,
};
use tangram_client::{self as tg, Handle as _};
use tangram_futures::task::Task;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_stream::wrappers::ReceiverStream;

/// Spawn and await an unsandboxed process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub inner: InnerArgs,

	/// The reference to the command.
	#[arg(index = 1)]
	pub reference: Option<tg::Reference>,
}

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct InnerArgs {
	/// Whether to check out the output. The output must be an artifact. A path to check out to may be provided.
	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub checkout: Option<Option<PathBuf>>,

	/// If this flag is set, then exit immediately instead of waiting for the process to finish.
	#[arg(short, long, conflicts_with = "checkout")]
	pub detach: bool,

	#[command(flatten)]
	pub inner: crate::process::spawn::InnerArgs,

	/// Choose the view.
	#[arg(long, default_value = "inline")]
	pub view: View,
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum View {
	None,
	#[default]
	Inline,
	Fullscreen,
}

impl Cli {
	pub async fn command_process_run(&self, args: Args) -> tg::Result<()> {
		let reference = args.reference.unwrap_or_else(|| ".".parse().unwrap());
		self.command_process_run_inner(args.inner, reference)
			.await?;
		Ok(())
	}

	pub async fn command_process_run_inner(
		&self,
		args: InnerArgs,
		reference: tg::Reference,
	) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.inner
			.remote
			.clone()
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));

		// Spawn the process.
		let process = self
			.command_process_spawn_inner(args.inner, reference)
			.await?;

		// If the detach flag is set, then return the process.
		if args.detach {
			println!("{}", process.id());
			return Ok(());
		}

		// Print the process.
		eprintln!("{} process {}", "info".blue().bold(), process.id());

		// Get the process's status.
		let status = process
			.status(&handle)
			.await?
			.try_next()
			.await?
			.ok_or_else(|| tg::error!("failed to get the status"))?;

		// If the process is finished, then get the process's output.
		let output = if status.is_finished() {
			let output = process
				.wait(&handle)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the output"))?;
			Some(output)
		} else {
			None
		};

		// If the process is not finished, then wait for it to finish while showing the viewer if enabled.
		let result = if let Some(output) = output {
			Ok(output)
		} else {
			// Spawn the view task.
			let view_task = {
				let handle = handle.clone();
				let process = process.clone();
				let task = Task::spawn_blocking(move |stop| {
					let local_set = tokio::task::LocalSet::new();
					let runtime = tokio::runtime::Builder::new_current_thread()
						.worker_threads(1)
						.enable_all()
						.build()
						.unwrap();
					local_set
						.block_on(&runtime, async move {
							let options = crate::viewer::Options {
								condensed_processes: true,
								expand_on_create: true,
							};
							let item = crate::viewer::Item::Process(process);
							let mut viewer = crate::viewer::Viewer::new(&handle, item, options);
							match args.view {
								View::None => (),
								View::Inline => {
									viewer.run_inline(stop).await?;
								},
								View::Fullscreen => {
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

			// Wait for the process's output.
			let result = process.wait(&handle).await;

			// Abort the cancel task.
			cancel_task.abort();

			// Stop and await the view task.
			if let Some(view_task) = view_task {
				view_task.stop();
				view_task.wait().await.unwrap();
			}

			result
		};

		// // Close the pipes.
		// for pipe in [stderr, stdin, stdout].into_iter().flatten() {
		// 	handle.close_pipe(&pipe).await.ok();
		// }

		// Get the output or return an error if we failed to wait for the process.
		let output =
			result.map_err(|source| tg::error!(!source, "failed to wait for the process"))?;

		// // Wait for the stdio task.
		// if let Some(stdio_task) = stdio_task {
		// 	stdio_task.stop();
		// 	stdio_task.wait().await.unwrap();
		// }

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
			.ok_or_else(|| tg::error!("expected an output"))?
			.try_into()?;

		// Check out the output if requested.
		if let Some(path) = args.checkout {
			// Get the artifact.
			let artifact: tg::Artifact = output
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
			let arg = tg::artifact::checkout::Arg {
				dependencies: path.is_some(),
				force: false,
				lockfile: false,
				path,
			};
			let stream = handle
				.check_out_artifact(&artifact.id(&handle).await?, arg)
				.await?;
			let output = self
				.render_progress_stream(stream)
				.await
				.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;

			// Print the path.
			println!("{}", output.path.display());

			return Ok(());
		}

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

fn spawn_stdio_task<H>(
	handle: &H,
	stdin: Option<tg::pipe::Id>,
	stdout: Option<tg::pipe::Id>,
	stderr: Option<tg::pipe::Id>,
) -> Task<()>
where
	H: tg::Handle,
{
	let handle = handle.clone();
	Task::spawn(|stop| async move {
		let stdin = tokio::spawn({
			let handle = handle.clone();
			async move {
				let Some(pipe) = stdin else {
					return;
				};
				let fut = stdin_task(&handle, pipe);
				let stop = stop.wait();
				match future::select(pin!(fut), pin!(stop)).await {
					future::Either::Left((Err(error), ..)) => {
						eprintln!("stdin error: {error}");
					},
					_ => (),
				}
			}
		});

		let stdout = tokio::spawn({
			let handle = handle.clone();
			async move {
				let Some(pipe) = stdout else {
					return;
				};
				if let Err(error) = output_task(&handle, &pipe, tokio::io::stdout()).await {
					eprintln!("stdout error: {error}");
				}
			}
		});

		let stderr = tokio::spawn({
			let handle = handle.clone();
			async move {
				let Some(pipe) = stderr else {
					return;
				};
				if let Err(error) = output_task(&handle, &pipe, tokio::io::stderr()).await {
					eprintln!("stdout error: {error}");
				}
			}
		});

		let (stdin, stdout, stderr) = future::join3(stdin, stdout, stderr).await;
		stdin.unwrap();
		stdout.unwrap();
		stderr.unwrap();
	})
}

async fn stdin_task<H>(handle: &H, pipe: tg::pipe::Id) -> tg::Result<()>
where
	H: tg::Handle,
{
	// Create a send/receive pair for sending stdin chunks. The channel is bounded to 1 to avoid buffering stdin messages.
	let (send, recv) = tokio::sync::mpsc::channel(1);

	// Spawn the stdin thread and detach it. This is necessary because the read from stdin cannot be interrupted or canceled.
	std::thread::spawn(move || {
		let mut stdin = std::io::stdin();
		let mut buf = vec![0u8; 4096];
		loop {
			let result = match stdin.read(&mut buf) {
				Ok(0) => return,
				Ok(n) => Ok(tg::pipe::Event::Chunk(Bytes::copy_from_slice(&buf[0..n]))),
				Err(source) => Err(tg::error!(!source, "failed to read from stdin")),
			};
			if let Err(_error) = send.blocking_send(result) {
				break;
			}
		}
	});

	// Create a stream.
	let stream = ReceiverStream::new(recv)
		.chain(stream::once(future::ready(Ok(tg::pipe::Event::End))))
		.boxed();

	// Write.
	handle
		.write_pipe(&pipe, stream)
		.await
		.map_err(|source| tg::error!(!source, %pipe, "failed to write pipe"))?;

	Ok(())
}

async fn output_task<H, W>(handle: &H, pipe: &tg::pipe::Id, writer: W) -> tg::Result<()>
where
	H: tg::Handle,
	W: AsyncWrite + Send + 'static,
{
	let mut writer = pin!(writer);

	// Create the stream.
	let stream = handle
		.read_pipe(pipe)
		.await
		.map_err(|source| tg::error!(!source, %pipe, "failed to get pipe read stream"))?;
	let mut stream = pin!(stream);

	// Drain the pipe.
	while let Some(event) = stream
		.try_next()
		.await
		.map_err(|source| tg::error!(%source, "failed to read pipe"))?
	{
		match event {
			tg::pipe::Event::Chunk(chunk) => {
				writer
					.write_all(&chunk)
					.await
					.map_err(|source| tg::error!(!source, "failed to write from pipe"))?;
				writer
					.flush()
					.await
					.map_err(|source| tg::error!(!source, "failed to flush"))?;
			},
			tg::pipe::Event::End => {
				break;
			},
		}
	}

	Ok(())
}
