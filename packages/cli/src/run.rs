use {
	crate::Cli,
	futures::prelude::*,
	num::ToPrimitive as _,
	std::{
		collections::BTreeMap, fmt::Write as _, os::unix::process::ExitStatusExt as _,
		path::PathBuf,
	},
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
			reference,
			mut options,
			trailing,
		} = args;

		// Spawn a sandboxed run for builds.
		let reference = if options.build {
			// Get the reference.
			let arg = tg::get::Arg {
				checkin: options.spawn.checkin.to_options(),
				..Default::default()
			};
			let referent = self.get_reference_with_arg(&reference, arg, true).await?;
			let item = referent
				.item
				.clone()
				.left()
				.ok_or_else(|| tg::error!("expected an object"))?;
			let referent = referent.map(|_| item);

			let options = Options {
				checkout_force: false,
				checkout: None,
				detach: false,
				executable_path: None,
				..options.clone()
			};
			let output = self
				.run_sandboxed(&options, &reference, &referent, Vec::new())
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

		// Get the reference.
		let arg = tg::get::Arg {
			checkin: options.spawn.checkin.to_options(),
			..Default::default()
		};
		let referent = self.get_reference_with_arg(&reference, arg, true).await?;
		let item = referent
			.item
			.clone()
			.left()
			.ok_or_else(|| tg::error!("expected an object"))?;
		let referent = referent.map(|_| item);

		// Run the process.
		let output = if Self::needs_sandbox(&options) {
			self.run_sandboxed(&options, &reference, &referent, trailing)
				.await?
		} else {
			self.run_unsandboxed(&options, &reference, &referent, trailing)
				.await?
		};

		// Check out the output if requested.
		if let Some(path) = options.checkout {
			let handle = self.handle().await?;
			let output = output
				.filter(|v| !v.is_null())
				.ok_or_else(|| tg::error!("expected an output"))?;

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
			Self::print_display(path.display());

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
		_options: &Options,
		_reference: &tg::Reference,
		referent: &tg::Referent<tg::Object>,
		trailing: Vec<String>,
	) -> tg::Result<Option<tg::Value>> {
		let handle = self.handle().await?;

		// Create a temp directory for the output.
		let temp = tempfile::tempdir()
			.map_err(|source| tg::error!(!source, "failed to create a temp directory"))?;
		let output_dir = temp.path().join("output");
		tokio::fs::create_dir_all(&output_dir)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the output directory"))?;
		let process_id = referent.item().id().to_string();
		let output_path = output_dir.join(&process_id);

		// Set the artifacts path for rendering.
		let artifacts_path = self.directory_path().join("artifacts");
		tg::run::set_artifacts_path(&artifacts_path);

		// Inherit the process env.
		let mut env: BTreeMap<String, String> = std::env::vars().collect();
		env.remove("TANGRAM_OUTPUT");
		env.remove("TANGRAM_PROCESS");
		env.remove("TANGRAM_URL");

		// Get the server URL.
		let url = match &handle {
			tg::Either::Left(client) => client.url().to_string(),
			tg::Either::Right(server) => server
				.url()
				.ok_or_else(|| tg::error!("the server does not have a URL"))?
				.to_string(),
		};

		// Create the command based on the referent item type.
		let (executable, args, command_env, cwd) = match referent.item().clone() {
			tg::Object::Command(command) => {
				let data = command
					.data(&handle)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the command data"))?;

				// Cache the command's artifact children.
				let artifacts: Vec<tg::artifact::Id> = command
					.children(&handle)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the command's children"))?
					.into_iter()
					.filter_map(|object| object.id().try_into().ok())
					.collect();
				if !artifacts.is_empty() {
					let arg = tg::cache::Arg { artifacts };
					let stream = handle
						.cache(arg)
						.await
						.map_err(|source| tg::error!(!source, "failed to cache the artifacts"))?;
					self.render_progress_stream(stream).await?;
				}

				// Determine if this is a JS or builtin command.
				let is_js = matches!(data.host.as_str(), "js" | "builtin");

				// Resolve the executable and render the args.
				let (executable, mut args) = if is_js {
					let executable = tangram_util::env::current_exe().map_err(|source| {
						tg::error!(!source, "failed to get the current executable")
					})?;
					let subcommand = if data.host == "builtin" {
						"builtin"
					} else {
						"js"
					};
					let args = vec![subcommand.to_owned(), data.executable.to_string()];
					(executable, args)
				} else {
					let executable = match &data.executable {
						tg::command::data::Executable::Artifact(exe) => {
							let mut path = artifacts_path.join(exe.artifact.to_string());
							if let Some(subpath) = &exe.path {
								path.push(subpath);
							}
							path
						},
						tg::command::data::Executable::Module(_) => {
							return Err(tg::error!("invalid executable"));
						},
						tg::command::data::Executable::Path(exe) => exe.path.clone(),
					};
					let args_values: Vec<tg::Value> = data
						.args
						.iter()
						.cloned()
						.map(tg::Value::try_from_data)
						.collect::<tg::Result<_>>()?;
					let args = tg::run::render_args(&args_values, &output_path)?;
					(executable, args)
				};

				// Render the command env.
				let env_values: tg::value::Map = data
					.env
					.iter()
					.map(|(k, v)| {
						Ok::<_, tg::Error>((k.clone(), tg::Value::try_from_data(v.clone())?))
					})
					.collect::<tg::Result<_>>()?;
				let command_env = tg::run::render_env(&env_values, &output_path)?;

				// Append trailing args.
				args.extend(trailing);

				(executable, args, Some(command_env), data.cwd.clone())
			},

			tg::Object::Directory(directory) => {
				let executable = tangram_util::env::current_exe().map_err(|source| {
					tg::error!(!source, "failed to get the current executable")
				})?;
				let id = directory.id();
				let mut args = vec!["js".to_owned(), id.to_string()];
				args.extend(trailing);
				(executable, args, None, None)
			},

			tg::Object::File(file) => {
				let kind = file
					.module(&handle)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the module kind"))?;
				if kind.is_some() {
					let tg_exe = tangram_util::env::current_exe().map_err(|source| {
						tg::error!(!source, "failed to get the current executable")
					})?;
					let id = file.id();
					let mut args = vec!["js".to_owned(), id.to_string()];
					args.extend(trailing);
					(tg_exe, args, None, None)
				} else {
					// Cache the file.
					let artifact_id = file.id();
					let arg = tg::cache::Arg {
						artifacts: vec![artifact_id.clone().into()],
					};
					let stream = handle
						.cache(arg)
						.await
						.map_err(|source| tg::error!(!source, "failed to cache the artifact"))?;
					self.render_progress_stream(stream).await?;
					let executable = artifacts_path.join(artifact_id.to_string());
					(executable, trailing, None, None)
				}
			},

			tg::Object::Symlink(_) => {
				return Err(tg::error!("unimplemented"));
			},

			_ => {
				return Err(tg::error!("expected a command or an artifact"));
			},
		};

		// Merge the command env on top of the process env.
		if let Some(command_env) = command_env {
			env.extend(command_env);
		}

		// Create the tokio process command.
		let mut cmd = tokio::process::Command::new(&executable);
		cmd.args(&args)
			.env_clear()
			.envs(&env)
			.env("TANGRAM_URL", &url)
			.env("TANGRAM_OUTPUT", output_path.to_str().unwrap())
			.stdin(std::process::Stdio::inherit())
			.stdout(std::process::Stdio::inherit())
			.stderr(std::process::Stdio::inherit());
		if let Some(cwd) = cwd {
			cmd.current_dir(cwd);
		}

		// Spawn the process.
		let mut child = cmd
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;

		// Wait for the process to exit.
		let status = child
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "failed to wait for the process"))?;
		let exit = None
			.or(status.code())
			.or(status.signal().map(|signal| 128 + signal))
			.unwrap()
			.to_u8()
			.unwrap();

		// Check for output.
		let mut output = None;
		let mut error = None;

		// Try to read the user.tangram.output xattr.
		if let Ok(Some(bytes)) = xattr::get(&output_path, "user.tangram.output") {
			let tgon = String::from_utf8(bytes)
				.map_err(|source| tg::error!(!source, "failed to decode the output xattr"))?;
			output = Some(
				tgon.parse::<tg::Value>()
					.map_err(|source| tg::error!(!source, "failed to parse the output xattr"))?,
			);
		}

		// Try to read the user.tangram.error xattr.
		if let Ok(Some(bytes)) = xattr::get(&output_path, "user.tangram.error") {
			let data = serde_json::from_slice::<tg::error::Data>(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the error xattr"))?;
			error = Some(
				tg::Error::try_from(data)
					.map_err(|source| tg::error!(!source, "failed to convert the error data"))?,
			);
		}

		// If no xattr output was set but the output path exists, check it in destructively.
		let exists = tokio::fs::try_exists(&output_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if the output path exists"))?;
		if output.is_none() && exists {
			let arg = tg::checkin::Arg {
				options: tg::checkin::Options {
					destructive: true,
					deterministic: true,
					ignore: false,
					lock: None,
					locked: true,
					root: true,
					..Default::default()
				},
				path: output_path.clone(),
				updates: Vec::new(),
			};
			let stream = handle
				.checkin(arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to check in the output"))?;
			let checkin_output = self
				.render_progress_stream(stream)
				.await
				.map_err(|source| tg::error!(!source, "failed to check in the output"))?;
			output = Some(tg::Artifact::with_id(checkin_output.artifact.item).into());
		}

		// Set the exit.
		if exit != 0 {
			self.exit.replace(exit);
		}

		// Handle an error.
		if let Some(error) = error {
			return Err(tg::error!(source = error, "the process failed"));
		}

		// Handle non-zero exit.
		if exit > 1 && exit < 128 {
			return Err(tg::error!("the process exited with code {}", exit));
		}
		if exit >= 128 {
			return Err(tg::error!("the process exited with signal {}", exit - 128));
		}

		Ok(output)
	}

	async fn run_sandboxed(
		&mut self,
		options: &Options,
		reference: &tg::Reference,
		referent: &tg::Referent<tg::Object>,
		trailing: Vec<String>,
	) -> tg::Result<Option<tg::Value>> {
		let handle = self.handle().await?;

		// Handle the executable path.
		let referent = if let Some(executable_path) = &options.executable_path {
			let directory = referent
				.item()
				.try_unwrap_directory_ref()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			let artifact = directory.get(&handle, executable_path).await.map_err(
				|source| tg::error!(!source, path = %executable_path.display(), "failed to get the artifact"),
			)?;
			let id = artifact
				.store(&handle)
				.await
				.map_err(|source| tg::error!(!source, "failed to store the artifact"))?;
			let mut referent = referent.clone().map(|_| tg::Object::with_id(id.into()));
			referent.options.path = Some(executable_path.clone());
			referent
		} else {
			referent.clone()
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
			let stdio = stdio::Stdio::new(&handle, remote.clone(), options)
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
		let process_stdio = stdio
			.as_ref()
			.map(|stdio| crate::process::spawn::Stdio {
				stdin: stdio.stdin.clone(),
				stdout: stdio.stdout.clone(),
				stderr: stdio.stderr.clone(),
			})
			.unwrap_or_default();
		let crate::process::spawn::Output { process, output } = self
			.spawn(spawn, reference.clone(), referent, trailing, process_stdio)
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
			let stdio_task = stdio.clone().map(|stdio| {
				Task::spawn({
					let handle = handle.clone();
					|stop| async move { self::stdio::task(&handle, stop, stdio).boxed().await }
				})
			});

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

	fn needs_sandbox(options: &Options) -> bool {
		// Sandbox if explicitly requested.
		if options.spawn.sandbox.get().is_some_and(|sbx| sbx) {
			return true;
		}

		// Remote processes imply sandboxing.
		if options
			.spawn
			.remotes
			.remotes
			.as_ref()
			.is_some_and(|remotes| !remotes.is_empty())
		{
			return true;
		}

		// Detached processes are currently sandboxed. This could change?
		if options.detach {
			return true;
		}

		// Cached processes must have been sandboxed.
		if options.spawn.cached.is_some_and(|cached| cached) {
			return true;
		}

		// Processes with a checksum should also run in a sandbox, to ensure cache hits.
		if options.spawn.checksum.is_some() {
			return true;
		}

		// You need a sandbox to deny network access.
		if options.spawn.network.is_none_or(|network| !network) {
			return true;
		}

		if !options.spawn.mounts.is_empty() {
			return true;
		}

		false
	}
}
