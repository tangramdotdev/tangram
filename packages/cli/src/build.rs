use crate::Cli;
use anstream::{eprintln, println};
use crossterm::style::Stylize as _;
use futures::{FutureExt as _, TryStreamExt as _};
use std::path::PathBuf;
use tangram_client::{self as tg, prelude::*};
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

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	/// Whether to check out the output.
	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub checkout: Option<Option<PathBuf>>,

	/// If this flag is set, then exit immediately instead of waiting for the process to finish.
	#[arg(short, long)]
	pub detach: bool,

	#[command(flatten)]
	pub spawn: crate::process::spawn::Options,

	/// The view to display if the process' stdio is not attached.
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
	pub async fn command_build(&mut self, args: Args) -> tg::Result<()> {
		// Get the reference.
		let reference = args.reference.unwrap_or_else(|| ".".parse().unwrap());

		// Build.
		self.build(args.options, reference, args.trailing, true)
			.await?;

		Ok(())
	}

	pub async fn build(
		&mut self,
		options: Options,
		reference: tg::Reference,
		trailing: Vec<String>,
		print: bool,
	) -> tg::Result<Option<tg::Value>> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = options
			.spawn
			.remote
			.clone()
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));

		// Spawn the process.
		let spawn = crate::process::spawn::Options {
			sandbox: true,
			..options.spawn
		};
		let (referent, process) = self
			.spawn(spawn, reference, trailing, None, None, None)
			.boxed()
			.await?;

		// If the detach flag is set, then print the process ID and return.
		if options.detach {
			if print {
				println!("{}", process.id());
			}
			return Ok(None);
		}

		// Print the process.
		if !self.args.quiet {
			eprintln!("{} {}", "info".blue().bold(), process.id());
		}

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
		let wait = if let Some(output) = output {
			output
		} else {
			// Spawn the view task.
			let view_task = {
				let handle = handle.clone();
				let item = crate::viewer::Item::Process(process.clone());
				let tg::Referent { path, tag, .. } = referent.clone();
				let root = tg::Referent { item, path, tag };
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
							let mut viewer =
								crate::viewer::Viewer::new(&handle, root, viewer_options);
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
							checksum: None,
							error: Some(
								tg::error!(
									code = tg::error::Code::Cancelation,
									"the process was explicitly canceled"
								)
								.to_data(),
							),
							exit: 1,
							force: false,
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
					std::process::exit(130);
				}
			});

			// Await the process.
			let result = process.wait(&handle).await;

			// Abort the cancel task.
			cancel_task.abort();

			// Stop and await the view task.
			if let Some(view_task) = view_task {
				view_task.stop();
				view_task.wait().await.unwrap();
			}

			result?
		};

		// Get the output.
		if let Some(error) = wait.error {
			eprintln!("{} the process failed", "error".red().bold());
			Self::print_error(&error, Some(&referent), self.config.as_ref());
		}

		// Set the exit.
		self.exit.replace(wait.exit);

		let Some(output) = wait.output else {
			return Ok(None);
		};

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
			let artifact = artifact.id();
			let arg = tg::checkout::Arg {
				artifact,
				dependencies: path.is_some(),
				force: false,
				lockfile: false,
				path,
			};
			let stream = handle.checkout(arg).await?;
			let tg::checkout::Output { path, .. } = self
				.render_progress_stream(stream)
				.await
				.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;

			// Print the path.
			println!("{}", path.display());

			return Ok(Some(output));
		}

		// Print the output.
		if print && !output.is_null() {
			Self::print_output(&output);
		}

		Ok(Some(output))
	}
}
