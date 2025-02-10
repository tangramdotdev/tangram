use crate::Cli;
use crossterm::style::Stylize as _;
use futures::{FutureExt as _, TryStreamExt as _};
use std::{io::IsTerminal as _, path::PathBuf};
use tangram_client::{self as tg, Handle as _};
use tangram_futures::task::Task;

/// Spawn and await a sandboxed process.
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
	/// Whether to check out the output. The output must be an artifact. A path to check out to may be provided.
	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub checkout: Option<Option<PathBuf>>,

	/// If this flag is set, then exit immediately instead of waiting for the process to finish.
	#[arg(short, long)]
	pub detach: bool,

	#[command(flatten)]
	pub spawn: crate::process::spawn::Options,

	#[arg(default_value = "inline", short, long)]
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
	pub async fn command_process_build(&self, args: Args) -> tg::Result<()> {
		// Get the reference.
		let reference = args.reference.unwrap_or_else(|| ".".parse().unwrap());

		// Build.
		self.build_process(args.options, reference, args.trailing)
			.await?;

		Ok(())
	}

	pub async fn build_process(
		&self,
		mut options: Options,
		reference: tg::Reference,
		trailing: Vec<String>,
	) -> tg::Result<Option<tg::Value>> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = options
			.spawn
			.remote
			.clone()
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));

		// Spawn the process.
		options.spawn.sandbox = true;
		let process = self
			.spawn_process(options.spawn, reference, trailing)
			.boxed()
			.await?;

		// If the detach flag is set, then print the process ID and return.
		if options.detach {
			println!("{}", process.id());
			return Ok(None);
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
							let viewer_options = crate::viewer::Options {
								condensed_processes: true,
								expand_on_create: true,
							};
							let item = crate::viewer::Item::Process(process);
							let mut viewer =
								crate::viewer::Viewer::new(&handle, item, viewer_options);
							match options.view {
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
							error: Some(tg::error!(
								canceled = true,
								"the process was explicitly canceled"
							)),
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

		// Get the output or return an error if waiting for the process failed.
		let output =
			result.map_err(|source| tg::error!(!source, "failed to wait for the process"))?;

		// Return an error if appropriate.
		if output.error.as_ref().map_or(false, |error| {
			matches!(error.code, Some(tg::error::code::CANCELED))
		}) {
			return Err(tg::error!(canceled = true, "the process was canceled"));
		}
		if let Some(source) = output.error {
			return Err(tg::error!(!source, "the process failed"));
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
			let arg = tg::artifact::checkout::Arg {
				dependencies: path.is_some(),
				force: false,
				lockfile: false,
				path,
			};
			let stream = handle
				.check_out_artifact(&artifact.id(&handle).await?, arg)
				.await?;
			let tg::artifact::checkout::Output { path, .. } = self
				.render_progress_stream(stream)
				.await
				.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;

			// Print the path.
			println!("{}", path.display());

			return Ok(Some(output));
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

		Ok(Some(output))
	}
}
