use {
	crate::Cli,
	futures::FutureExt as _,
	std::{fmt::Write as _, path::PathBuf},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

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

#[derive(Clone, Debug, Default, clap::Args)]
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

	#[command(flatten)]
	pub print: crate::print::Options,

	#[command(flatten)]
	pub spawn: crate::process::spawn::Options,

	/// Print the full spawn output instead of just the process ID.
	#[arg(long, short)]
	pub verbose: bool,

	/// The view to display if the process's stdio is not attached.
	#[arg(long)]
	pub view: Option<View>,
}

#[derive(
	Clone,
	Copy,
	Debug,
	clap::ValueEnum,
	derive_more::IsVariant,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum View {
	None,
	Inline,
	Fullscreen,
}

impl Cli {
	pub async fn command_run(&mut self, args: Args) -> tg::Result<()> {
		let checkout = args.options.checkout.is_some();
		let detach = args.options.detach;
		let local = args.options.spawn.local.get();
		let print = args.options.print.clone();
		let remotes = args.options.spawn.remotes.get();
		let verbose = args.options.verbose;

		// Run.
		let output = self.run(args).await?;

		// Print the output.
		if detach && !verbose {
			let string = output
				.try_unwrap_string()
				.ok()
				.ok_or_else(|| tg::error!("expected a string"))?;
			Self::print_display(string);
		} else if checkout {
			Self::print_display(output);
		} else if (detach && verbose) || !output.is_null() {
			let arg = tg::object::get::Arg {
				local,
				metadata: false,
				remotes,
			};
			self.print_value(&output, print, arg).await?;
		}

		Ok(())
	}

	pub(crate) async fn run(&mut self, args: Args) -> tg::Result<tg::Value> {
		let handle = self.handle().await?;

		let Args {
			mut options,
			reference,
			trailing,
		} = args;

		// Handle the build flag.
		let reference = if options.build {
			let build_args = Args {
				options: Options {
					spawn: crate::process::spawn::Options {
						local: options.spawn.local.clone(),
						remotes: options.spawn.remotes.clone(),
						..Default::default()
					},
					..Default::default()
				},
				reference,
				trailing: vec![],
			};
			let output = Box::pin(self.build(build_args)).await?;
			let object = output
				.try_unwrap_object()
				.ok()
				.ok_or_else(|| tg::error!("expected the build to output an object"))?;
			let id = object.id();
			tg::Reference::with_object(id)
		} else {
			reference
		};

		// If detach is set, then set stdio if necessary.
		if options.detach {
			match options.spawn.stdin {
				None => {
					options.spawn.stdin = Some(tg::process::Stdio::Null);
				},
				Some(tg::process::Stdio::Null) => (),
				Some(_) => {
					return Err(tg::error!("invalid stdin with detach"));
				},
			}
			match options.spawn.stdout {
				None => {
					options.spawn.stdout = Some(tg::process::Stdio::Log);
				},
				Some(tg::process::Stdio::Null | tg::process::Stdio::Log) => (),
				Some(_) => {
					return Err(tg::error!("invalid stdout with detach"));
				},
			}
			match options.spawn.stderr {
				None => {
					options.spawn.stderr = Some(tg::process::Stdio::Log);
				},
				Some(tg::process::Stdio::Null | tg::process::Stdio::Log) => (),
				Some(_) => {
					return Err(tg::error!("invalid stderr with detach"));
				},
			}
		}

		// Spawn the process.
		let output = self
			.spawn(options.spawn, reference, trailing)
			.boxed()
			.await?;
		let process = output;
		let sandboxed = process.item().pid().is_none();

		// If the detach flag is set, then return the process ID.
		if options.detach {
			if options.verbose {
				let output = tg::process::spawn::Output {
					cached: process.item().cached().unwrap_or(false),
					process: process.item().id().clone(),
					remote: process.item().remote().cloned(),
					token: process.item().token().cloned(),
					wait: None,
				};
				let value = serde_json::to_value(output)
					.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
					.into();
				return Ok(value);
			}
			return Ok(process.item().id().to_string().into());
		}

		// Print the process.
		if !self.args.quiet && sandboxed {
			let mut message = process.item().id().to_string();
			if let Some(token) = process.item().token() {
				write!(message, " {token}").unwrap();
			}
			Self::print_info_message(&message);
		}

		// Spawn the view task if necessary.
		let (_exit_sender, exit_receiver) = tokio::sync::oneshot::channel();
		let view_task = if let Some(view) = options.view {
			let handle = handle.clone();
			let root = process.clone().map(crate::viewer::Item::Process);
			let task = Task::spawn_blocking(move |stop| -> tg::Result<()> {
				let local_set = tokio::task::LocalSet::new();
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.map_err(|source| tg::error!(!source, "failed to create the tokio runtime"))?;
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
					let mut viewer =
						crate::viewer::Viewer::new(&handle, root, exit_receiver, viewer_options);
					match view {
						View::None => (),
						View::Inline => {
							viewer.run_inline(stop, false).await?;
						},
						View::Fullscreen => {
							viewer.run_fullscreen(stop, true).await?;
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
		let cancel_task = if view_task.is_some() {
			Some(Task::spawn({
				let handle = handle.clone();
				let process = process.clone();
				|_| async move {
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
		let wait = process
			.item()
			.wait(&handle, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to await the process"))?;

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
					tracing::warn!(?error, "failed to render the viewer");
					Self::print_warning_message("failed to render the viewer");
				},
				Err(error) => {
					tracing::warn!(?error, "the viewer task panicked");
					Self::print_warning_message("failed to render the viewer");
				},
			}
		}

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
