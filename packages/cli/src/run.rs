use {
	crate::Cli,
	anstream::eprintln,
	crossterm::style::Stylize as _,
	futures::prelude::*,
	std::path::PathBuf,
	tangram_client::{self as tg, prelude::*},
	tangram_futures::task::Task,
};

mod signal;
mod stdio;

/// Spawn and await an unsandboxed process.
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
	/// If this flag is set, then build the specified target and run its output.
	#[arg(long, short)]
	pub build: bool,

	/// The view to display while building.
	#[arg(long, default_value = "inline")]
	pub build_view: crate::build::View,

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

	/// Set the path to use for the executable.
	#[arg(long, short = 'x')]
	pub executable_path: Option<PathBuf>,

	/// Whether to print blobs.
	#[arg(long)]
	pub print_blobs: bool,

	/// The depth with which to print the output.
	#[arg(default_value = "0", long)]
	pub print_depth: crate::object::get::Depth,

	/// Whether to pretty print the output.
	#[arg(long)]
	pub print_pretty: Option<bool>,

	#[command(flatten)]
	pub spawn: crate::process::spawn::Options,
}

impl Cli {
	pub async fn command_run(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		let Args {
			options,
			reference,
			trailing,
		} = args;

		// If the build flag is set, then build and get the output.
		let reference = if options.build {
			// Spawn the process.
			let spawn = crate::process::spawn::Options {
				sandbox: crate::process::spawn::Sandbox::new(Some(true)),
				remote: options.spawn.remote.clone(),
				..Default::default()
			};
			let crate::process::spawn::Output { process, .. } = self
				.spawn(spawn, reference, vec![], None, None, None)
				.boxed()
				.await?;

			// Print the process.
			if !self.args.quiet {
				eprint!("{} {}", "info".blue().bold(), process.item().id());
				if let Some(token) = process.item().token() {
					eprint!(" {token}");
				}
				eprintln!();
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
					.wait(&handle)
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
									expand_tags: false,
									show_process_commands: false,
								};
								let mut viewer =
									crate::viewer::Viewer::new(&handle, root, viewer_options);
								match options.build_view {
									crate::build::View::None => (),
									crate::build::View::Inline => {
										viewer.run_inline(stop, false).await?;
									},
									crate::build::View::Fullscreen => {
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
				let result = process.item().wait(&handle).await;

				// Abort the cancel task.
				cancel_task.abort();

				// Stop and await the view task.
				if let Some(view_task) = view_task {
					view_task.stop();
					view_task.wait().await.unwrap();
				}

				result?
			};

			// Set the exit.
			if wait.exit != 0 {
				self.exit.replace(wait.exit);
			}

			// Handle an error.
			if let Some(error) = wait.error {
				let error = tg::Error {
					message: Some("the process failed".to_owned()),
					source: Some(process.clone().map(|_| Box::new(error))),
					values: [("id".to_owned(), process.item().id().to_string())].into(),
					..Default::default()
				};
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

			let object = output
				.try_unwrap_object()
				.ok()
				.ok_or_else(|| tg::error!("expected the build to output an object"))?;
			let id = object.id();
			tg::Reference::with_object(id)
		} else {
			reference
		};

		// Handle the executable path.
		let reference = if let Some(path) = &options.executable_path {
			let referent = self.get_reference(&reference).await?;
			let directory = referent
				.item
				.right()
				.ok_or_else(|| tg::error!("expected an object"))?
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			let artifact = directory.get(&handle, path).await?;
			tg::Reference::with_object(artifact.id().into())
		} else {
			reference
		};

		// Get the remote.
		let remote = options
			.spawn
			.remote
			.clone()
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Create the stdio.
		let stdio = stdio::Stdio::new(&handle, remote.clone(), &options)
			.await
			.map_err(|source| tg::error!(!source, "failed to create stdio"))?;

		// Spawn the process.
		let crate::process::spawn::Output { process, output } = self
			.spawn(
				options.spawn,
				reference,
				trailing,
				stdio.stdin.clone(),
				stdio.stdout.clone(),
				stdio.stderr.clone(),
			)
			.boxed()
			.await?;

		// If the detach flag is set, then print the process ID and return.
		if options.detach {
			Self::print_json(&output, None).await?;
			return Ok(());
		}

		// Print the process.
		if !self.args.quiet {
			eprint!("{} {}", "info".blue().bold(), process.item().id());
			if let Some(token) = process.item().token() {
				eprint!(" {token}");
			}
			eprintln!();
		}

		// Enable raw mode if necessary.
		if let Some(tty) = &stdio.tty {
			tty.enable_raw_mode()?;
		}

		// Spawn the stdio task.
		let stdio_task = Task::spawn({
			let handle = handle.clone();
			let stdio = stdio.clone();
			|stop| async move { self::stdio::task(&handle, stop, stdio).await }
		});

		// Spawn signal task.
		let signal_task = tokio::spawn({
			let handle = handle.clone();
			let process = process.item().id().clone();
			let remote = remote.clone();
			async move {
				self::signal::task(&handle, &process, remote).await.ok();
			}
		});

		// Await the process.
		let result = process
			.item()
			.wait(&handle)
			.await
			.map_err(|source| tg::error!(!source, "failed to await the process"));

		// Close stdout and stderr.
		stdio.close(&handle).await?;

		// Stop and await the stdio task.
		stdio_task.stop();
		stdio_task.wait().await.unwrap()?;

		// Delete stdio.
		stdio.delete(&handle).await?;

		// Abort the signal task.
		signal_task.abort();

		// Handle the result.
		let wait = result.map_err(|source| tg::error!(!source, "failed to await the process"))?;

		// Set the exit.
		if wait.exit != 0 {
			self.exit.replace(wait.exit);
		}

		// Handle an error.
		if let Some(error) = wait.error {
			let error = tg::Error {
				message: Some("the process failed".to_owned()),
				source: Some(process.clone().map(|_| Box::new(error))),
				values: [("id".to_owned(), process.item().id().to_string())].into(),
				..Default::default()
			};
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
				force: options.checkout_force,
				lock: false,
				path,
			};
			let stream = handle.checkout(arg).await?;
			let tg::checkout::Output { path, .. } = self
				.render_progress_stream(stream)
				.await
				.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;

			// Print the path.
			println!("{}", path.display());

			return Ok(());
		}

		// Print the output.
		if !output.is_null() {
			Self::print_value(
				&handle,
				&output,
				options.print_depth,
				options.print_pretty,
				options.print_blobs,
			)
			.await?;
		}

		Ok(())
	}
}
