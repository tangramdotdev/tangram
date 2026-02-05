use {
	crate::Cli,
	futures::{FutureExt as _, TryStreamExt as _},
	std::{fmt::Write as _, path::PathBuf},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

/// Spawn and await a sandboxed process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	/// The reference to the command.
	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,

	/// Set arguments.
	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Options {
	/// Whether to check out the output.
	#[expect(clippy::option_option)]
	#[arg(long, require_equals = true, short)]
	pub checkout: Option<Option<PathBuf>>,

	/// Whether to overwrite an existing file system object at the path.
	#[arg(long, requires = "checkout")]
	pub checkout_force: bool,

	/// If this flag is set, then exit immediately instead of waiting for the process to finish.
	#[arg(long, short)]
	pub detach: bool,

	#[command(flatten)]
	pub print: crate::print::Options,

	/// Print the full spawn output instead of just the process ID.
	#[arg(long, short)]
	pub verbose: bool,

	#[command(flatten)]
	pub spawn: crate::process::spawn::Options,

	/// The view to display if the process's stdio is not attached.
	#[arg(default_value = "inline", long)]
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
		let handle = self.handle().await?;

		let Args {
			options,
			reference,
			trailing,
		} = args;

		// Spawn the process.
		let sandbox =
			crate::process::spawn::Sandbox::new(options.spawn.sandbox.get().or(Some(true)));
		let local = options.spawn.local.local;
		let remotes = options.spawn.remotes.remotes.clone();
		let spawn = crate::process::spawn::Options {
			sandbox,
			..options.spawn
		};
		let crate::process::spawn::Output { process, output } = self
			.spawn(spawn, reference, trailing, None, None, None)
			.boxed()
			.await?;

		// If the detach flag is set, then print the process ID and return.
		if options.detach {
			if options.verbose {
				self.print_serde(output, options.print).await?;
			} else {
				Self::print_display(&output.process);
			}
			return Ok(());
		}

		// Print the process.
		if !self.args.quiet {
			let mut message = process.item().id().to_string();
			if let Some(token) = process.item().token() {
				write!(message, " {token}").unwrap();
			}
			Self::print_info_message(&message);
		}

		// Get the process's status.
		let status = process
			.item()
			.status(&handle)
			.await?
			.try_next()
			.await?
			.ok_or_else(|| tg::error!("failed to get the status"))?;

		// If the process is finished, then get the process's output.
		let wait = if status.is_finished() {
			let output = process
				.item()
				.wait(&handle, tg::process::wait::Arg::default())
				.await
				.map_err(|source| tg::error!(!source, "failed to get the output"))?;
			Some(output)
		} else {
			None
		};

		// If the process is not finished, then wait for it to finish while showing the viewer if enabled.
		let wait = if let Some(wait) = wait {
			wait
		} else {
			// Spawn the view task.
			let view_task = {
				let handle = handle.clone();
				let root = process.clone().map(crate::viewer::Item::Process);
				let task = Task::spawn_blocking(move |stop| {
					let local_set = tokio::task::LocalSet::new();
					let runtime = tokio::runtime::Builder::new_current_thread()
						.enable_all()
						.build()
						.unwrap();
					local_set
						.block_on(&runtime, async move {
							let viewer_options = crate::viewer::Options {
								collapse_process_children: true,
								depth: None,
								expand_objects: false,
								expand_packages: false,
								expand_processes: true,
								expand_metadata: false,
								expand_tags: false,
								expand_values: false,
								show_process_commands: false,
							};
							let mut viewer =
								crate::viewer::Viewer::new(&handle, root, viewer_options);
							match options.view {
								View::None => (),
								View::Inline => {
									viewer.run_inline(stop, false).await?;
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
						process
							.item()
							.cancel(&handle)
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
			let result = process
				.item()
				.wait(&handle, tg::process::wait::Arg::default())
				.await;

			// Abort the cancel task.
			cancel_task.abort();

			// Stop and await the view task.
			if let Some(view_task) = view_task {
				view_task.stop();
				view_task.wait().await.unwrap();
			}

			result?
		};

		// Print verbose output if requested.
		if options.verbose {
			let output = tg::process::wait::Output {
				error: wait.error.as_ref().map(tg::Error::to_data_or_id),
				exit: wait.exit,
				output: wait.output.as_ref().map(tg::Value::to_data),
			};
			self.print_serde(output, options.print.clone()).await?;
			return Ok(());
		}

		// Set the exit.
		if wait.exit != 0 {
			self.exit.replace(wait.exit);
		}

		// Handle an error.
		if let Some(error) = wait.error {
			let error = match error.to_data_or_id() {
				tg::Either::Left(data) => {
					let object = tg::error::Object::try_from_data(data)
						.map_err(|source| tg::error!(!source, "invalid error"))?;
					tg::Either::Left(Box::new(object))
				},
				tg::Either::Right(id) => tg::Either::Right(Box::new(tg::Error::with_id(id))),
			};
			let error = tg::Error::with_object(tg::error::Object {
				message: Some("the process failed".to_owned()),
				source: Some(process.clone().map(|_| error)),
				values: [("id".to_owned(), process.item().id().to_string())].into(),
				..Default::default()
			});
			return Err(error);
		}

		// Handle non-zero exit.
		if wait.exit > 1 && wait.exit < 128 {
			return Err(tg::error!("the process exited with code {}", wait.exit));
		}
		if wait.exit >= 128 {
			return Err(tg::error!(
				"the process exited with signal {}",
				wait.exit - 128
			));
		}

		// Get the output.
		let output = wait.output.unwrap_or(tg::Value::Null);

		// Check out the output if requested.
		if let Some(path) = options.checkout {
			// Get the artifact.
			let artifact: tg::Artifact = output
				.clone()
				.try_into()
				.map_err(|_| tg::error!("expected an artifact"))?;

			// Get the path.
			let path = if let Some(path) = path {
				let path = tangram_util::fs::canonicalize_parent(path)
					.await
					.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
				Some(path)
			} else {
				None
			};

			// Check out the artifact.
			let artifact = artifact.id();
			let arg = tg::checkout::Arg {
				artifact: artifact.clone(),
				dependencies: path.is_some(),
				extension: None,
				force: options.checkout_force,
				lock: None,
				path,
			};
			let stream = handle.checkout(arg).await.map_err(
				|source| tg::error!(!source, %artifact, "failed to check out the artifact"),
			)?;
			let tg::checkout::Output { path, .. } =
				self.render_progress_stream(stream).await.map_err(
					|source| tg::error!(!source, %artifact, "failed to check out the artifact"),
				)?;

			// Print the path.
			self.print_serde(path, options.print).await?;

			return Ok(());
		}

		// Print the output.
		if !output.is_null() {
			let arg = tg::object::get::Arg {
				local,
				metadata: false,
				remotes,
			};
			self.print_value(&output, options.print, arg).await?;
		}

		Ok(())
	}
}
