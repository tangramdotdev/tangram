use {
	crate::Cli,
	futures::prelude::*,
	std::{fmt::Write as _, path::PathBuf},
	tangram_client::prelude::*,
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

	#[command(flatten)]
	pub print: crate::print::Options,

	/// Print the full spawn output instead of just the process ID.
	#[arg(long, short)]
	pub verbose: bool,

	#[command(flatten)]
	pub spawn: crate::process::spawn::Options,
}

impl Cli {
	pub async fn command_run(&mut self, args: Args) -> tg::Result<()> {
		let Args {
			options,
			reference,
			trailing,
		} = args;

		// If the build flag is set, then build and run the output.
		if options.build {
			return self.command_run_build(options, reference, trailing).await;
		}

		self.command_run_inner(options, reference, trailing).await
	}

	async fn command_run_build(
		&mut self,
		options: Options,
		reference: tg::Reference,
		trailing: Vec<String>,
	) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Spawn the process.
		let spawn = crate::process::spawn::Options {
			sandbox: crate::process::spawn::Sandbox::new(Some(true)),
			local: options.spawn.local.clone(),
			remotes: options.spawn.remotes.clone(),
			..Default::default()
		};
		let crate::process::spawn::Output { process, output } = self
			.spawn(spawn, reference, vec![], None, None, None)
			.boxed()
			.await?;

		// Print the process.
		if !self.args.quiet {
			let mut message = process.item().id().to_string();
			if let Some(token) = process.item().token() {
				write!(message, " {token}").unwrap();
			}
			Self::print_info_message(&message);
		}

		// If the spawn output includes a wait output, then use it.
		let wait = output
			.wait
			.map(TryInto::try_into)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the wait output"))?;

		// If the process is not finished, then wait for it to finish while showing the viewer if enabled.
		let wait = if let Some(wait) = wait {
			wait
		} else {
			// Spawn the view task.
			let view_task = {
				let handle = handle.clone();
				let root = process.clone().map(crate::viewer::Item::Process);
				let task = Task::spawn_blocking(move |stop| -> tg::Result<()> {
					let local_set = tokio::task::LocalSet::new();
					let runtime = tokio::runtime::Builder::new_current_thread()
						.enable_all()
						.build()
						.map_err(|source| {
							tg::error!(!source, "failed to create the tokio runtime")
						})?;
					local_set.block_on(&runtime, async move {
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
						let mut viewer = crate::viewer::Viewer::new(&handle, root, viewer_options);
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
			let arg = tg::process::wait::Arg {
				token: process.item().token().cloned(),
				..tg::process::wait::Arg::default()
			};
			let result = process.item().wait(&handle, arg).await;

			// Abort the cancel task.
			cancel_task.abort();

			// Stop and await the view task.
			if let Some(view_task) = view_task {
				view_task.stop();
				match view_task.wait().await {
					Ok(Ok(())) => {},
					Ok(Err(error)) => {
						tracing::warn!(?error, "failed to render the process viewer");
						Self::print_warning_message("failed to render the process viewer");
					},
					Err(error) => {
						tracing::warn!(?error, "failed to join the process viewer task");
						Self::print_warning_message("failed to render the process viewer");
					},
				}
			}

			result?
		};

		// Set the exit.
		if wait.exit != 0 {
			self.exit.replace(wait.exit);
		}

		// Handle an error.
		if let Some(error) = wait.error {
			let error = error
				.to_data_or_id()
				.map_left(|data| {
					Box::new(tg::error::Object::try_from_data(data).unwrap_or_else(|_| {
						tg::error::Object {
							message: Some("invalid error".to_owned()),
							..Default::default()
						}
					}))
				})
				.map_right(|id| Box::new(tg::Error::with_id(id)));
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

		let object = output
			.try_unwrap_object()
			.ok()
			.ok_or_else(|| tg::error!("expected the build to output an object"))?;
		let id = object.id();
		let reference = tg::Reference::with_object(id);

		// Run the built output.
		self.command_run_inner(options, reference, trailing).await
	}

	async fn command_run2(&mut self, args: Args) -> tg::Result<()> {
		let Args {
			reference,
			mut options,
			trailing,
		} = args;

		// Spawn a sandboxed run for builds.
		let reference = if options.build {
			let options = Options {
				checkout_force: false,
				checkout: None,
				detach: false,
				executable_path: None,
				..options.clone()
			};
			let output = self
				.run_sandboxed(&options, reference.clone(), Vec::new())
				.await
				.map_err(|source| tg::error!(!source, %reference, "failed to build"))?
				.ok_or_else(|| tg::error!("expected an output"))?;
			let object = output
				.try_unwrap_object()
				.ok()
				.ok_or_else(|| tg::error!("expected the build to output an object"))?;
			let id = object.id();
			tg::Reference::with_object(id)
		} else {
			reference
		};
		options.build = false;

		// Determine if the process should be sandboxed.
		let sandboxed = true;
		let output = if sandboxed {
			self.run_sandboxed(&options, reference, trailing).await?
		} else {
			self.run_unsandboxed(&options, reference, trailing).await?
		};

		// Check out the output if requested.
		if let Some(path) = options.checkout {
			let handle = self.handle().await?;
			let output = output.ok_or_else(|| tg::error!("expected an output"))?;

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
		if !options.verbose
			&& let Some(output) = output
			&& !output.is_null()
		{
			let arg = tg::object::get::Arg {
				local: options.spawn.local.local,
				metadata: false,
				remotes: options.spawn.remotes.remotes,
			};
			self.print_value(&output, options.print, arg).await?;
		}
		Ok(())
	}

	async fn run_unsandboxed(
		&mut self,
		options: &Options,
		reference: tg::Reference,
		trailing: Vec<String>,
	) -> tg::Result<Option<tg::Value>> {
		todo!("implement the unsandboxed run")
	}

	async fn run_sandboxed(
		&mut self,
		options: &Options,
		reference: tg::Reference,
		trailing: Vec<String>,
	) -> tg::Result<Option<tg::Value>> {
		let handle = self.handle().await?;

		// Handle the executable path.
		let reference = if let Some(path) = &options.executable_path {
			let referent = self.get_reference(&reference).await?;
			let directory = referent
				.item
				.left()
				.ok_or_else(|| tg::error!("expected an object"))?
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			let artifact = directory.get(&handle, path).await.map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to get the artifact"),
			)?;
			let id = artifact
				.store(&handle)
				.await
				.map_err(|source| tg::error!(!source, "failed to store the artifact"))?;
			tg::Reference::with_object(id.into())
		} else {
			reference
		};

		// Get the remote
		let remote = options
			.spawn
			.remotes
			.remotes
			.clone()
			.and_then(|remotes| remotes.into_iter().next());

		// Create the stdio if this is not a build.
		let stdio = if options.build {
			None
		} else {
			let stdio = stdio::Stdio::new(&handle, remote.clone(), &options)
				.await
				.map_err(|source| tg::error!(!source, "failed to create stdio"))?;
			Some(stdio)
		};

		// Spawn the process.
		let spawn = crate::process::spawn::Options {
			sandbox: crate::process::spawn::Sandbox::new(Some(true)),
			local: options.spawn.local.clone(),
			remotes: options.spawn.remotes.clone(),
			..Default::default()
		};
		let stdin = stdio.as_ref().and_then(|stdio| stdio.stdin.clone());
		let stdout = stdio.as_ref().and_then(|stdio| stdio.stdout.clone());
		let stderr = stdio.as_ref().and_then(|stdio| stdio.stderr.clone());
		let crate::process::spawn::Output { process, output } = self
			.spawn(spawn, reference, trailing, stdin, stdout, stderr)
			.boxed()
			.await?;

		// If the detach flag is set, then print the process ID and return.
		if options.detach {
			if options.verbose {
				self.print_serde(output, options.print.clone()).await?;
			} else {
				Self::print_display(&output.process);
			}
			return Ok(None);
		}

		// Print the process.
		if !self.args.quiet {
			let mut message = process.item().id().to_string();
			if let Some(token) = process.item().token() {
				write!(message, " {token}").unwrap();
			}
			Self::print_info_message(&message);
		}

		// Enable raw mode if necessary.
		if let Some(stdio) = &stdio
			&& let Some(tty) = &stdio.tty
		{
			tty.enable_raw_mode()?;
		}

		// If the spawn output includes a wait output, then use it.
		let wait = output
			.wait
			.map(TryInto::try_into)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the wait output"))?;

		// If the process is not finished, then wait for it to finish while showing the viewer if enabled.
		let wait = if let Some(wait) = wait {
			wait
		} else {
			// Spawn the stdio task.
			let stdio_task = if let Some(stdio) = stdio.clone() {
				Some(Task::spawn({
					let handle = handle.clone();
					|stop| async move { self::stdio::task(&handle, stop, stdio).boxed().await }
				}))
			} else {
				None
			};

			// Spawn signal task. This will be handled by the cancellation tasks for builds.
			let signal_task = if options.build {
				None
			} else {
				Some(tokio::spawn({
					let handle = handle.clone();
					let process = process.item().id().clone();
					let remote = remote.clone();
					async move {
						self::signal::task(&handle, &process, remote).await.ok();
					}
				}))
			};

			// Spawn the view task, if this is a build.
			let view_task = if options.build {
				let handle = handle.clone();
				let root = process.clone().map(crate::viewer::Item::Process);
				let build_view = options.build_view;
				let task = Task::spawn_blocking(move |stop| -> tg::Result<()> {
					let local_set = tokio::task::LocalSet::new();
					let runtime = tokio::runtime::Builder::new_current_thread()
						.enable_all()
						.build()
						.map_err(|source| {
							tg::error!(!source, "failed to create the tokio runtime")
						})?;
					local_set.block_on(&runtime, async move {
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
						let mut viewer = crate::viewer::Viewer::new(&handle, root, viewer_options);
						match build_view {
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
				});
				Some(task)
			} else {
				None
			};

			// Spawn a task to attempt to cancel the process on the first interrupt signal and exit the process on the second.
			let cancel_task = if options.build {
				Some(tokio::spawn({
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
				}))
			} else {
				None
			};

			// Await the process.
			let arg = tg::process::wait::Arg {
				token: process.item().token().cloned(),
				..tg::process::wait::Arg::default()
			};
			let result = process.item().wait(&handle, arg).await;

			// Close stdio.
			if let Some(stdio) = stdio {
				stdio.close(&handle).await?;
				if let Some(task) = stdio_task {
					task.stop();
					task.wait().await.unwrap()?;
				}
				stdio.delete(&handle).await?;
			}

			// Abort the signal task.
			if let Some(signal_task) = signal_task {
				signal_task.abort();
			}

			// Abort the cancel task.
			if let Some(cancel_task) = cancel_task {
				cancel_task.abort();
			}

			// Stop and await the view task.
			if let Some(view_task) = view_task {
				view_task.stop();
				match view_task.wait().await {
					Ok(Ok(())) => {},
					Ok(Err(error)) => {
						tracing::warn!(?error, "failed to render the process viewer");
						Self::print_warning_message("failed to render the process viewer");
					},
					Err(error) => {
						tracing::warn!(?error, "failed to join the process viewer task");
						Self::print_warning_message("failed to render the process viewer");
					},
				}
			}

			result.map_err(|error| tg::error!(!error, "failed to await the process"))?
		};

		// Print verbose output if requested and this is not a pre-run build.
		if options.verbose && !options.build {
			let output = tg::process::wait::Output {
				error: wait.error.as_ref().map(tg::Error::to_data_or_id),
				exit: wait.exit,
				output: wait.output.as_ref().map(tg::Value::to_data),
			};
			self.print_serde(output, options.print.clone()).await?;
			return Ok(None);
		}

		// Set the exit.
		if wait.exit != 0 {
			self.exit.replace(wait.exit);
		}

		// Handle an error.
		if let Some(error) = wait.error {
			let error = error
				.to_data_or_id()
				.map_left(|data| {
					Box::new(tg::error::Object::try_from_data(data).unwrap_or_else(|_| {
						tg::error::Object {
							message: Some("invalid error".to_owned()),
							..Default::default()
						}
					}))
				})
				.map_right(|id| Box::new(tg::Error::with_id(id)));
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

		Ok(wait.output)
	}

	async fn command_run_inner(
		&mut self,
		options: Options,
		reference: tg::Reference,
		trailing: Vec<String>,
	) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Handle the executable path.
		let reference = if let Some(path) = &options.executable_path {
			let referent = self.get_reference(&reference).await?;
			let directory = referent
				.item
				.left()
				.ok_or_else(|| tg::error!("expected an object"))?
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			let artifact = directory.get(&handle, path).await.map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to get the artifact"),
			)?;
			let id = artifact
				.store(&handle)
				.await
				.map_err(|source| tg::error!(!source, "failed to store the artifact"))?;
			tg::Reference::with_object(id.into())
		} else {
			reference
		};

		// Get the remote.
		let remote = options
			.spawn
			.remotes
			.remotes
			.clone()
			.and_then(|remotes| remotes.into_iter().next());

		// Create the stdio.
		let stdio = stdio::Stdio::new(&handle, remote.clone(), &options)
			.await
			.map_err(|source| tg::error!(!source, "failed to create stdio"))?;

		let local = options.spawn.local.local;
		let remotes = options.spawn.remotes.remotes.clone();

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

		// Enable raw mode if necessary.
		if let Some(tty) = &stdio.tty {
			tty.enable_raw_mode()?;
		}

		// Spawn the stdio task.
		let stdio_task = Task::spawn({
			let handle = handle.clone();
			let stdio = stdio.clone();
			|stop| async move { self::stdio::task(&handle, stop, stdio).boxed().await }
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

		// Await the process unless the spawn output already includes the wait output.
		let result = if let Some(wait) = output
			.wait
			.map(TryInto::try_into)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the wait output"))?
		{
			Ok(wait)
		} else {
			let arg = tg::process::wait::Arg {
				token: process.item().token().cloned(),
				..tg::process::wait::Arg::default()
			};
			process
				.item()
				.wait(&handle, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to await the process"))
		};

		// Close stdout and stderr.
		stdio.close(&handle).await?;

		// Stop and await the stdio task.
		stdio_task.stop();

		// Await the stdio task.
		stdio_task.wait().await.unwrap()?;

		// Delete stdio.
		stdio.delete(&handle).await?;

		// Abort the signal task.
		signal_task.abort();

		// Handle the result.
		let wait = result.map_err(|source| tg::error!(!source, "failed to await the process"))?;

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
			let error = error
				.to_data_or_id()
				.map_left(|data| {
					Box::new(tg::error::Object::try_from_data(data).unwrap_or_else(|_| {
						tg::error::Object {
							message: Some("invalid error".to_owned()),
							..Default::default()
						}
					}))
				})
				.map_right(|id| Box::new(tg::Error::with_id(id)));
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
		if !options.verbose && !output.is_null() {
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
