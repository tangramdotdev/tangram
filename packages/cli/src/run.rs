use {
	crate::Cli,
	futures::FutureExt as _,
	std::{fmt::Write as _, path::PathBuf},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

mod signal;
mod stdio;

/// Spawn and await a process.
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

	#[command(flatten)]
	pub spawn: crate::process::spawn::Options,

	/// Print the full spawn output instead of just the process ID.
	#[arg(long, short)]
	pub verbose: bool,

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
	pub async fn command_run(&mut self, args: Args) -> tg::Result<()> {
		let checkout = args.options.checkout.is_some();
		let print = args.options.print.clone();
		let local = args.options.spawn.local.local;
		let remotes = args.options.spawn.remotes.remotes.clone();

		// Run the process.
		let output = self.run(args).await?;

		// Print the output.
		if checkout {
			Self::print_display(output);
		} else if !output.is_null() {
			let arg = tg::object::get::Arg {
				local,
				metadata: false,
				remotes,
			};
			self.print_value(&output, print, arg).await?;
		}

		Ok(())
	}

	async fn run(&mut self, args: Args) -> tg::Result<tg::Value> {
		let handle = self.handle().await?;

		let Args {
			options,
			reference,
			trailing,
		} = args;

		// If the build flag is set, then build the specified target and run its output.
		let reference = if options.build {
			let build_args = Args {
				options: Options {
					build: false,
					checkout: None,
					checkout_force: false,
					detach: false,
					executable_path: None,
					print: crate::print::Options::default(),
					spawn: crate::process::spawn::Options {
						sandbox: crate::process::spawn::Sandbox::new(Some(true)),
						local: options.spawn.local.clone(),
						remotes: options.spawn.remotes.clone(),
						stderr: Some(tg::run::Stdio::Log),
						stdin: Some(tg::run::Stdio::Null),
						stdout: Some(tg::run::Stdio::Log),
						..Default::default()
					},
					verbose: false,
					view: options.view,
				},
				reference,
				trailing: vec![],
			};
			let output = Box::pin(self.run(build_args)).await?;
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

		// Set up stdio.
		let mut stdio = stdio::Stdio::new(remote.clone(), &options)
			.await
			.map_err(|source| tg::error!(!source, "failed to create stdio"))?;
		{
			if let Some(stdin) = options.spawn.stdin.clone() {
				let (stdin, stdin_blob) = stdin.into_stdin();
				if stdin_blob.is_some() {
					stdio.stdin = tg::process::Stdio::Null;
				}
				if let Some(stdin) = stdin {
					stdio.stdin = stdin;
				}
			}
			if let Some(stdout) = options.spawn.stdout.clone()
				&& let Some(stdout) = stdout
					.into_process_stdio()
					.map_err(|source| tg::error!(!source, "invalid stdout stdio"))?
			{
				stdio.stdout = stdout;
			}
			if let Some(stderr) = options.spawn.stderr.clone()
				&& let Some(stderr) = stderr
					.into_process_stdio()
					.map_err(|source| tg::error!(!source, "invalid stderr stdio"))?
			{
				stdio.stderr = stderr;
			}
		}
		let pty = stdio
			.tty
			.as_ref()
			.map(|tty| {
				let size = tty.get_size()?;
				let pty = tg::process::pty::Pty { size };
				Ok::<_, tg::Error>(pty)
			})
			.transpose()?;

		// Spawn the process.
		let process = self
			.spawn(
				options.spawn.clone(),
				reference,
				trailing,
				pty,
				stdio.stdin,
				stdio.stdout,
				stdio.stderr,
			)
			.boxed()
			.await?;

		// If the detach flag is set, then return the process ID.
		if options.detach {
			if options.verbose {
				let mut map = serde_json::Map::new();
				map.insert(
					"process".into(),
					serde_json::Value::String(process.item().id().to_string()),
				);
				if let Some(token) = process.item().token() {
					map.insert("token".into(), serde_json::Value::String(token.clone()));
				}
				let json = serde_json::to_string(&map)
					.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;
				println!("{json}");
			} else {
				println!("{}", process.item().id());
			}
			return Ok(tg::Value::Null);
		}

		// Print the process.
		if !self.args.quiet {
			let mut message = process.item().id().to_string();
			if let Some(token) = process.item().token() {
				write!(message, " {token}").unwrap();
			}
			Self::print_info_message(&message);
		}

		// Wait for the process to finish.
		let wait = if matches!(
			stdio.stdin,
			tg::process::Stdio::Pipe | tg::process::Stdio::Pty
		) || matches!(
			stdio.stdout,
			tg::process::Stdio::Pipe | tg::process::Stdio::Pty
		) || matches!(
			stdio.stderr,
			tg::process::Stdio::Pipe | tg::process::Stdio::Pty
		) {
			// Enable raw mode if necessary.
			if let Some(tty) = &stdio.tty {
				tty.enable_raw_mode()?;
			}

			// Spawn the stdio task.
			let stdio_task = Task::spawn({
				let handle = handle.clone();
				let process = process.item().id().clone();
				let stdio = stdio.clone();
				|stop| async move {
					self::stdio::task(&handle, stop, process, stdio)
						.boxed()
						.await
				}
			});

			// Spawn the signal task.
			let signal_task = tokio::spawn({
				let handle = handle.clone();
				let process = process.item().id().clone();
				let remote = remote.clone();
				async move {
					self::signal::task(&handle, &process, remote).await.ok();
				}
			});

			// Await the process.
			let arg = tg::process::wait::Arg {
				token: process.item().token().cloned(),
				..tg::process::wait::Arg::default()
			};
			let result = process
				.item()
				.wait(&handle, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to await the process"));

			// Stop and await the stdio task.
			stdio_task.stop();
			stdio_task.wait().await.unwrap()?;

			// Abort the signal task.
			signal_task.abort();

			result.map_err(|source| tg::error!(!source, "failed to await the process"))?
		} else {
			// Spawn the view task.
			let view_task = {
				let handle = handle.clone();
				let root = process.clone().map(crate::viewer::Item::Process);
				let view = options.view;
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
						match view {
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

		// If verbose, return the wait output.
		if options.verbose {
			let output = tg::process::wait::Output {
				error: wait.error.as_ref().map(tg::Error::to_data_or_id),
				exit: wait.exit,
				output: wait.output.as_ref().map(tg::Value::to_data),
			};
			let value = serde_json::to_value(&output)
				.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
				.into();
			return Ok(value);
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
		if let Some(path) = options.checkout.clone() {
			let artifact: tg::Artifact = output
				.try_into()
				.map_err(|_| tg::error!("expected an artifact"))?;
			let path = if let Some(path) = path {
				let path = tangram_util::fs::canonicalize_parent(path)
					.await
					.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
				Some(path)
			} else {
				None
			};
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
			let value = path.display().to_string().into();
			return Ok(value);
		}

		Ok(output)
	}
}
