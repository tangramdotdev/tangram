use {
	crate::Cli, crossterm as ct, futures::prelude::*, std::path::PathBuf,
	tangram_client::prelude::*,
};

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

	/// Set the path to use for the executable.
	#[arg(long, short = 'x')]
	pub executable_path: Option<PathBuf>,

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
	pub mounts: Vec<tg::process::Mount>,

	#[command(flatten)]
	pub network: Network,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,

	/// Whether to retry failed processes.
	#[arg(long)]
	pub retry: bool,

	#[command(flatten)]
	pub sandbox: Sandbox,

	/// Set the stderr mode.
	#[arg(long)]
	pub stderr: Option<tg::process::Stdio>,

	/// Set the stdin mode.
	#[arg(long)]
	pub stdin: Option<tg::process::Stdio>,

	/// Set the stdout mode.
	#[arg(long)]
	pub stdout: Option<tg::process::Stdio>,

	/// Tag the process.
	#[arg(long)]
	pub tag: Option<tg::Tag>,

	#[command(flatten)]
	pub tty: Tty,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Network {
	/// Whether to enable the network.
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_network",
		require_equals = true,
	)]
	network: Option<bool>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "network",
		require_equals = true,
	)]
	no_network: Option<bool>,
}

impl Network {
	pub fn new(network: bool) -> Self {
		Self {
			network: Some(network),
			no_network: None,
		}
	}

	pub fn get(&self) -> Option<bool> {
		self.network.or(self.no_network.map(|v| !v))
	}
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
	/// Whether to allocate a tty for the process. Optionally set the size as rows,cols.
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_tty",
		require_equals = true,
		value_name = "ROWS,COLS",
	)]
	pub(crate) tty: Option<tg::Either<bool, tg::process::tty::Size>>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "tty",
		require_equals = true,
	)]
	pub(crate) no_tty: Option<bool>,
}

#[derive(Clone, Debug)]
pub(crate) struct Output {
	pub cached: bool,
	pub process: tg::Referent<tg::Process>,
}

impl Tty {
	pub fn new_disabled() -> Self {
		Self {
			tty: None,
			no_tty: Some(true),
		}
	}
}

impl Cli {
	pub async fn command_process_spawn(&mut self, mut args: Args) -> tg::Result<()> {
		if args.options.sandbox.get().is_some_and(|v| !v) {
			return Err(tg::error!("a spawn must be sandboxed"));
		}
		args.options.sandbox = crate::process::spawn::Sandbox::new(Some(true));

		match args.options.stdin {
			None => {
				args.options.stdin = Some(tg::process::Stdio::Null);
			},
			Some(tg::process::Stdio::Blob(_) | tg::process::Stdio::Null) => (),
			Some(_) => {
				return Err(tg::error!("invalid stdin for a spawn"));
			},
		}

		match args.options.stdout {
			None => {
				args.options.stdout = Some(tg::process::Stdio::Log);
			},
			Some(tg::process::Stdio::Log | tg::process::Stdio::Null) => (),
			Some(_) => {
				return Err(tg::error!("invalid stdout for a spawn"));
			},
		}

		match args.options.stderr {
			None => {
				args.options.stderr = Some(tg::process::Stdio::Log);
			},
			Some(tg::process::Stdio::Log) => (),
			Some(_) => {
				return Err(tg::error!("invalid stderr for a spawn"));
			},
		}

		let output = self
			.spawn(args.options, args.reference, args.trailing)
			.boxed()
			.await?;

		if args.verbose {
			let output = tg::process::spawn::Output {
				cached: output.cached,
				process: output.process.item().id().clone(),
				remote: output.process.item().remote().cloned(),
				token: output.process.item().token().cloned(),
				wait: None,
			};
			self.print_serde(output, args.print).await?;
		} else {
			Self::print_display(output.process.item().id());
		}

		Ok(())
	}

	pub(crate) async fn spawn(
		&mut self,
		options: Options,
		reference: tg::Reference,
		trailing: Vec<String>,
	) -> tg::Result<Output> {
		let handle = self.handle().await?;

		// Determine whether to sandbox.
		let sandbox = match options.sandbox.get() {
			None => {
				options.cached.is_some_and(|v| v)
					|| options.checksum.is_some()
					|| !options.mounts.is_empty()
					|| options.network.get().is_some()
					|| options.remotes.get().is_some()
					|| matches!(
						options.tty.tty,
						Some(tg::Either::Left(true) | tg::Either::Right(_))
					)
			},
			Some(true) => true,
			Some(false) => false,
		};

		// Handle the executable path.
		let reference = if let Some(path) = &options.executable_path {
			let mut options = reference.options().clone();
			if let Some(reference_path) = &mut options.path {
				*reference_path = reference_path.join(path);
			} else {
				options.path = Some(path.clone());
			}
			tg::Reference::new(
				reference.item().clone(),
				options,
				reference.export().map(ToOwned::to_owned),
			)
		} else {
			reference
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
		if let Some(tg::process::Stdio::Blob(blob)) = &options.stdin {
			command = command.stdin(Some(tg::Blob::with_id(blob.clone())));
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
			if !env.contains_key("TANGRAM_URL")
				&& let tg::Either::Left(client) = &handle
			{
				env.insert("TANGRAM_URL".to_owned(), client.url().to_string().into());
			}
			if !env.contains_key("TANGRAM_TOKEN")
				&& let Some(token) = &self.args.token
			{
				env.insert("TANGRAM_TOKEN".to_owned(), token.clone().into());
			}
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

		// Create the command and store it.
		let command = command.build();
		command
			.store(&handle)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the command"))?;

		// Determine if the network is enabled.
		let network = options.network.get().unwrap_or(!sandbox);

		// Get the mounts.
		let mut mounts = Vec::new();
		for mount in &options.mounts {
			let source = tokio::fs::canonicalize(&mount.source)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
			mounts.push(tg::process::data::Mount {
				source,
				target: mount.target.clone(),
				readonly: mount.readonly,
			});
		}

		// Get the tty.
		let tty = match (options.tty.tty.clone(), options.tty.no_tty) {
			(Some(tg::Either::Left(true)), _) => {
				let tty = std::fs::OpenOptions::new()
					.read(true)
					.write(true)
					.open("/dev/tty")
					.ok();
				let tty_fd = tty.as_ref().map(std::os::fd::AsRawFd::as_raw_fd);
				let size = if let Some(fd) = tty_fd {
					let mut size = unsafe { std::mem::zeroed::<libc::winsize>() };
					if unsafe { libc::ioctl(fd, libc::TIOCGWINSZ, &mut size) } < 0
						|| size.ws_col == 0
						|| size.ws_row == 0
					{
						None
					} else {
						Some(tg::process::tty::Size {
							rows: size.ws_row,
							cols: size.ws_col,
						})
					}
				} else {
					ct::terminal::size().ok().and_then(|(cols, rows)| {
						(cols != 0 && rows != 0).then_some(tg::process::tty::Size { rows, cols })
					})
				};
				let default = tg::process::tty::Size { rows: 64, cols: 64 };
				let size = size.unwrap_or(default);
				let tty = tg::process::Tty { size };
				Some(tg::Either::Right(tty))
			},
			(Some(tg::Either::Right(size)), _) => {
				let tty = tg::process::Tty { size };
				Some(tg::Either::Right(tty))
			},
			(Some(tg::Either::Left(false)), _) | (_, Some(true)) => Some(tg::Either::Left(false)),
			_ => None,
		};

		// Spawn the process.
		let arg = tg::process::spawn::Arg {
			cached: options.cached,
			checksum: options.checksum,
			command: tg::Referent::with_item(command.id()),
			local: options.local.get(),
			mounts,
			network,
			parent: None,
			remotes: options.remotes.get(),
			retry: options.retry,
			sandbox,
			stderr: options.stderr.unwrap_or_default(),
			stdin: options.stdin.unwrap_or_default(),
			stdout: options.stdout.unwrap_or_default(),
			tty,
		};
		let output = tg::Process::spawn_with_progress(&handle, arg, |stream| {
			self.render_progress_stream(stream)
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;
		let cached = output.cached;
		let process = output.process;

		// Tag the process if requested.
		if let Some(tag) = options.tag {
			let item = tg::Either::Right(process.id().clone());
			let arg = tg::tag::put::Arg {
				force: false,
				item,
				local: options.local.get(),
				remotes: options.remotes.get(),
			};
			handle
				.put_tag(&tag, arg)
				.await
				.map_err(|source| tg::error!(!source, %tag, "failed to tag the process"))?;
		}

		let process = referent.replace(process).0;

		Ok(Output { cached, process })
	}
}
