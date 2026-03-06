use {crate::Cli, futures::prelude::*, std::path::PathBuf, tangram_client::prelude::*};

/// Spawn a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,

	/// Set arguments.
	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,

	/// Print the full spawn output instead of just the process ID.
	#[arg(long, short)]
	pub verbose: bool,
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	/// Set arguments as strings.
	#[arg(
		action = clap::ArgAction::Append,
		long = "arg-string",
		num_args = 1,
		short = 'a',
	)]
	pub arg_strings: Vec<String>,

	/// Set arguments as values.
	#[arg(
		action = clap::ArgAction::Append,
		long = "arg-value",
		num_args = 1,
		short = 'A',
	)]
	pub arg_values: Vec<String>,

	/// Set this flag to true to require a cached process. Set this flag to false to require a new process to be created. Omit this flag to use a cached process if possible, and create a new process if not.
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		require_equals = true,
	)]
	pub cached: Option<bool>,

	#[command(flatten)]
	pub checkin: crate::checkin::Options,

	/// The output's expected checksum.
	#[arg(long)]
	pub checksum: Option<tg::Checksum>,

	/// Set the working directory for the process.
	#[arg(long, short = 'C')]
	pub cwd: Option<PathBuf>,

	/// Set environment variables as strings.
	#[arg(
		action = clap::ArgAction::Append,
		long = "env-string",
		num_args = 1,
		short = 'e',
	)]
	pub env_strings: Vec<String>,

	/// Set environment variables as values.
	#[arg(
		action = clap::ArgAction::Append,
		long = "env-value",
		num_args = 1,
		short = 'E',
	)]
	pub env_values: Vec<String>,

	/// Set the host.
	#[arg(long)]
	pub host: Option<String>,

	#[command(flatten)]
	pub local: crate::util::args::Local,

	/// Configure mounts.
	#[arg(
		action = clap::ArgAction::Append,
		long = "mount",
		num_args = 1,
		short,
	)]
	pub mounts: Vec<tg::Either<tg::process::Mount, tg::command::Mount>>,

	/// Enable network access.
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		require_equals = true,
	)]
	pub network: Option<bool>,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,

	/// Whether to retry failed processes.
	#[arg(long)]
	pub retry: bool,

	#[command(flatten)]
	pub sandbox: Sandbox,

	/// Set the stderr mode.
	#[arg(long)]
	pub stderr: Option<tg::run::Stdio>,

	/// Set the stdin mode.
	#[arg(long)]
	pub stdin: Option<tg::run::Stdio>,

	/// Set the stdout mode.
	#[arg(long)]
	pub stdout: Option<tg::run::Stdio>,

	/// Tag the process.
	#[arg(long)]
	pub tag: Option<tg::Tag>,

	#[command(flatten)]
	pub tty: Tty,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Sandbox {
	/// Whether to sandbox the process.
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_sandbox",
		require_equals = true,
	)]
	sandbox: Option<bool>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "sandbox",
		require_equals = true,
	)]
	no_sandbox: Option<bool>,
}

impl Sandbox {
	pub fn new(sandbox: Option<bool>) -> Self {
		Self {
			sandbox,
			no_sandbox: None,
		}
	}

	pub fn get(&self) -> Option<bool> {
		self.sandbox.or(self.no_sandbox.map(|v| !v))
	}
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Tty {
	/// Whether to allocate a terminal.
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_tty",
		require_equals = true,
	)]
	tty: Option<bool>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "tty",
		require_equals = true,
	)]
	no_tty: Option<bool>,
}

impl Tty {
	pub fn get(&self) -> bool {
		self.tty.or(self.no_tty.map(|v| !v)).unwrap_or(true)
	}
}

impl Cli {
	pub async fn command_process_spawn(&mut self, args: Args) -> tg::Result<()> {
		let process = self
			.spawn(
				args.options,
				args.reference,
				args.trailing,
				None,
				tg::process::Stdio::default(),
				tg::process::Stdio::default(),
				tg::process::Stdio::default(),
			)
			.boxed()
			.await?;
		if args.verbose {
			let output = tg::process::spawn::Output {
				process: process.item().id().clone(),
				remote: process.item().remote().cloned(),
				token: process.item().token().cloned(),
				wait: None,
			};
			self.print_serde(output, args.print).await?;
		} else {
			Self::print_display(process.item().id());
		}
		Ok(())
	}

	#[expect(clippy::too_many_arguments)]
	pub(crate) async fn spawn(
		&mut self,
		options: Options,
		reference: tg::Reference,
		trailing: Vec<String>,
		pty: Option<tg::process::Pty>,
		stdin: tg::process::Stdio,
		stdout: tg::process::Stdio,
		stderr: tg::process::Stdio,
	) -> tg::Result<tg::Referent<tg::Process>> {
		let handle = self.handle().await?;

		// Determine if the process is sandboxed.
		let sandbox =
			options.sandbox.get().unwrap_or_default() || options.remotes.remotes.is_some();
		let stderr_option = options.stderr.clone();
		let stdin_option = options.stdin.clone();
		let stdout_option = options.stdout.clone();
		let (stdin_mode_from_option, stdin_blob_from_option) =
			stdin_option.map_or((None, None), tg::run::Stdio::into_stdin);
		let stdin = stdin_mode_from_option.unwrap_or(stdin);
		let stdout = if let Some(stdout_mode) = stdout_option {
			if let Some(stdout_mode) = stdout_mode
				.into_process_stdio()
				.map_err(|source| tg::error!(!source, "invalid stdout stdio"))?
			{
				stdout_mode
			} else {
				stdout
			}
		} else {
			stdout
		};
		let stderr = if let Some(stderr_mode) = stderr_option {
			if let Some(stderr_mode) = stderr_mode
				.into_process_stdio()
				.map_err(|source| tg::error!(!source, "invalid stderr stdio"))?
			{
				stderr_mode
			} else {
				stderr
			}
		} else {
			stderr
		};

		// Get the reference.
		let arg = tg::get::Arg {
			checkin: options.checkin.to_options(),
			..Default::default()
		};
		let referent = self.get_reference_with_arg(&reference, arg, true).await?;
		let item = referent
			.item
			.clone()
			.left()
			.ok_or_else(|| tg::error!("expected an object"))?;
		let mut referent = referent.map(|_| item);

		// Create the command builder.
		let mut command_env = None;
		let mut command = match referent.item.clone() {
			tg::Object::Command(command) => {
				let object = command.object(&handle).await?;
				command_env = Some(object.env.clone());
				tg::Command::builder(object.host.clone(), object.executable.clone())
					.args(object.args.clone())
					.cwd(object.cwd.clone())
					.mounts(object.mounts.clone())
					.stdin(object.stdin.clone())
			},

			tg::Object::Directory(directory) => {
				let root_module_file_name = tg::module::try_get_root_module_file_name(
					&handle,
					tg::Either::Left(&directory),
				)
				.await?
				.ok_or_else(
					|| tg::error!(directory = %directory.id(), "failed to find a root module"),
				)?;
				if let Some(path) = &mut referent.options.path {
					*path = path.join(root_module_file_name);
				} else {
					referent.options.path.replace(root_module_file_name.into());
				}
				let kind = tg::module::module_kind_for_path(root_module_file_name).unwrap();
				let item = directory
					.get_entry_edge(&handle, root_module_file_name)
					.await?;
				let item = tg::module::Item::Edge(item.into());
				let referent = tg::Referent::with_item(item);
				let module = tg::Module { kind, referent };
				let export = reference.export().unwrap_or("default").to_owned();
				let host = "js".to_owned();
				let executable = tg::command::Executable::Module(tg::command::ModuleExecutable {
					module,
					export: Some(export),
				});
				tg::Command::builder(host, executable)
			},

			tg::Object::File(file) => {
				let kind = referent.path().and_then(|path| {
					tg::module::module_kind_for_path(path).ok().or_else(|| {
						if let Ok(Some(xattr)) = xattr::get(path, tg::file::MODULE_XATTR_NAME)
							&& let Some(kind) = String::from_utf8(xattr)
								.ok()
								.and_then(|s| s.parse::<tg::module::Kind>().ok())
						{
							Some(kind)
						} else {
							None
						}
					})
				});
				let kind = if kind.is_some() {
					kind
				} else {
					file.module(&handle)
						.await
						.map_err(|source| tg::error!(!source, "failed to get the module kind"))?
				};
				if let Some(kind) = kind {
					let item = file.clone().into();
					let item = tg::graph::Edge::Object(item);
					let item = tg::module::Item::Edge(item);
					let referent = tg::Referent::with_item(item);
					let module = tg::Module { kind, referent };
					let export = reference.export().unwrap_or("default").to_owned();
					let host = "js".to_owned();
					let executable =
						tg::command::Executable::Module(tg::command::ModuleExecutable {
							module,
							export: Some(export),
						});
					tg::Command::builder(host, executable)
				} else {
					let host = tg::host().to_owned();
					let executable =
						tg::command::Executable::Artifact(tg::command::ArtifactExecutable {
							artifact: file.clone().into(),
							path: None,
						});
					tg::Command::builder(host, executable)
				}
			},

			tg::Object::Symlink(_) => {
				return Err(tg::error!("unimplemented"));
			},

			_ => {
				return Err(tg::error!("expected a command or an artifact"));
			},
		};
		if let Some(stdin_blob) = stdin_blob_from_option {
			command = command.stdin(Some(stdin_blob));
		}

		// Set the args.
		let mut args_: Vec<tg::Value> = Vec::new();
		let mut matches = &self.matches;
		while let Some((_, matches_)) = matches.subcommand() {
			matches = matches_;
		}
		let arg_string_indices = if matches.try_contains_id("arg_strings").unwrap_or(false) {
			matches
				.indices_of("arg_strings")
				.map(std::iter::Iterator::collect)
				.unwrap_or_default()
		} else {
			Vec::new()
		};
		let arg_value_indices = if matches.try_contains_id("arg_values").unwrap_or(false) {
			matches
				.indices_of("arg_values")
				.map(std::iter::Iterator::collect)
				.unwrap_or_default()
		} else {
			Vec::new()
		};
		if arg_string_indices.is_empty() && arg_value_indices.is_empty() {
			for value in options.arg_strings {
				args_.push(tg::Value::String(value));
			}
			for value in options.arg_values {
				let value = value
					.parse()
					.map_err(|error| tg::error!(!error, "failed to parse the arg"))?;
				args_.push(value);
			}
		} else {
			let mut indexed: Vec<(usize, tg::Value)> = Vec::new();
			for (index, value) in arg_string_indices.into_iter().zip(options.arg_strings) {
				let value = tg::Value::String(value);
				indexed.push((index, value));
			}
			for (index, value) in arg_value_indices.into_iter().zip(options.arg_values) {
				let value = value
					.parse()
					.map_err(|error| tg::error!(!error, "failed to parse the arg"))?;
				indexed.push((index, value));
			}
			indexed.sort_by_key(|&(index, _)| index);
			args_.extend(indexed.into_iter().map(|(_, value)| value));
		}
		for arg in trailing {
			args_.push(tg::Value::String(arg));
		}
		command = command.args(args_);

		// Set the cwd.
		if !sandbox {
			let cwd = std::env::current_dir()
				.map_err(|source| tg::error!(!source, "failed to get the working directory"))?;
			command = command.cwd(cwd);
		}
		if let Some(cwd) = options.cwd {
			command = command.cwd(cwd);
		}

		// Set the env.
		let mut env = tg::value::Map::new();
		if !sandbox {
			env.extend(std::env::vars().map(|(key, value)| (key, value.into())));
			env.remove("TANGRAM_OUTPUT");
			env.remove("TANGRAM_PROCESS");
			env.remove("TANGRAM_URL");
		}
		for (key, value) in command_env.into_iter().flatten() {
			if let Ok(mutation) = value.try_unwrap_mutation_ref() {
				mutation.apply(&mut env, &key)?;
			} else {
				env.insert(key, value);
			}
		}
		for string in options.env_strings {
			let (key, value) = string
				.split_once('=')
				.ok_or_else(|| tg::error!("expected KEY=VALUE"))?;
			let key = key.to_owned();
			let value = tg::Value::String(value.to_owned());
			if let Ok(mutation) = value.try_unwrap_mutation_ref() {
				mutation.apply(&mut env, &key)?;
			} else {
				env.insert(key, value);
			}
		}
		for string in options.env_values {
			let (key, value) = string
				.split_once('=')
				.ok_or_else(|| tg::error!("expected KEY=VALUE"))?;
			let key = key.to_owned();
			let value = value
				.parse::<tg::Value>()
				.map_err(|error| tg::error!(!error, "failed to parse the value"))?;
			if let Ok(mutation) = value.try_unwrap_mutation_ref() {
				mutation.apply(&mut env, &key)?;
			} else {
				env.insert(key, value);
			}
		}
		if !env.contains_key("TANGRAM_HOST") {
			let host = if let Some(host) = options.host {
				host
			} else {
				tg::host().to_owned()
			};
			env.insert("TANGRAM_HOST".to_owned(), host.into());
		}
		command = command.env(env);

		// Set the mounts.
		for mount in &options.mounts {
			if let tg::Either::Right(mount) = mount {
				command = command.mount(mount.clone());
			}
		}

		// Create the command and store it.
		let command = command.build();
		command
			.store(&handle)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the command"))?;

		// Determine if the network is enabled.
		let network = options.network.unwrap_or(!sandbox);

		// Determine the retry.
		let retry = options.retry;

		// Get the mounts.
		let mut mounts = Vec::new();
		if !sandbox {
			mounts.push(tg::process::data::Mount {
				source: "/".into(),
				target: "/".into(),
				readonly: false,
			});
		}
		for mount in &options.mounts {
			if let tg::Either::Left(mount) = mount {
				let source = tokio::fs::canonicalize(&mount.source)
					.await
					.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
				mounts.push(tg::process::data::Mount {
					source,
					target: mount.target.clone(),
					readonly: mount.readonly,
				});
			}
		}

		// Spawn the process.
		let arg = tg::process::spawn::Arg {
			cached: options.cached,
			checksum: options.checksum,
			command: tg::Referent::with_item(command.id()),
			local: options.local.local,
			mounts,
			network,
			parent: None,
			pty,
			remotes: options.remotes.remotes.clone(),
			retry,
			stderr,
			stdin,
			stdout,
		};
		let process = tg::Process::spawn_with_progress(&handle, arg, |stream| {
			self.render_progress_stream(stream)
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;

		// Tag the process if requested.
		if let Some(tag) = options.tag {
			let item = tg::Either::Right(process.id().clone());
			let arg = tg::tag::put::Arg {
				force: false,
				item,
				local: options.local.local,
				remotes: options.remotes.remotes.clone(),
			};
			handle
				.put_tag(&tag, arg)
				.await
				.map_err(|source| tg::error!(!source, %tag, "failed to tag the process"))?;
		}

		let process = referent.replace(process).0;

		Ok(process)
	}
}
