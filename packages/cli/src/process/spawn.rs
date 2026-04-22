use {
	crate::Cli,
	crossterm as ct,
	futures::prelude::*,
	std::{fmt::Write as _, path::PathBuf},
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
	pub location: crate::location::Args,

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
pub struct Sandbox {
	/// Whether to sandbox the process, or an existing sandbox to use.
	#[arg(
		default_missing_value = "true",
		id = "sandbox",
		long = "sandbox",
		num_args = 0..=1,
		overrides_with = "no_sandbox",
		require_equals = true,
	)]
	value: Option<tg::Either<bool, tg::sandbox::Id>>,

	#[arg(
		default_missing_value = "true",
		id = "no_sandbox",
		long = "no-sandbox",
		num_args = 0..=1,
		overrides_with = "sandbox",
		require_equals = true,
	)]
	disabled: Option<bool>,

	#[command(flatten)]
	pub arg: crate::sandbox::Options,
}

impl Sandbox {
	pub fn new(sandbox: Option<tg::Either<bool, tg::sandbox::Id>>) -> Self {
		Self {
			value: sandbox,
			disabled: None,
			arg: crate::sandbox::Options::default(),
		}
	}

	pub fn get(&self) -> Option<tg::Either<bool, tg::sandbox::Id>> {
		self.value
			.clone()
			.or(self.disabled.map(|v| tg::Either::Left(!v)))
	}

	pub fn set(&mut self, sandbox: Option<tg::Either<bool, tg::sandbox::Id>>) {
		self.value = sandbox;
		self.disabled = None;
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
		if matches!(args.options.sandbox.get(), Some(tg::Either::Left(false))) {
			return Err(tg::error!("a spawn must be sandboxed"));
		}
		if args.options.sandbox.get().is_none() {
			args.options.sandbox.set(Some(tg::Either::Left(true)));
		}

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
			.spawn(args.options, args.reference, args.trailing, false)
			.boxed()
			.await?;

		if args.verbose {
			let output = tg::process::spawn::Output {
				cached: output.item().cached().unwrap_or(false),
				location: output
					.item()
					.location()
					.and_then(|location| location.to_location()),
				process: output.item().id().clone(),
				token: output.item().token().cloned(),
				wait: None,
			};
			self.print_serde(output, args.print).await?;
		} else {
			Self::print_display(output.item().id());
		}

		Ok(())
	}

	pub(crate) async fn spawn(
		&mut self,
		options: Options,
		reference: tg::Reference,
		trailing: Vec<String>,
		print: bool,
	) -> tg::Result<tg::Referent<tg::Process>> {
		let handle = self.handle().await?;
		let location = options.location.get();

		// Determine whether to sandbox.
		let sandbox = match options.sandbox.get() {
			None => {
				options.cached.is_some_and(|v| v)
					|| options.checksum.is_some()
					|| !options.sandbox.arg.is_empty()
					|| location
						.as_ref()
						.and_then(tg::location::Arg::to_location)
						.is_some_and(|location| location.is_remote())
					|| matches!(
						options.tty.tty,
						Some(tg::Either::Left(true) | tg::Either::Right(_))
					)
			},
			Some(tg::Either::Left(true) | tg::Either::Right(_)) => true,
			Some(tg::Either::Left(false)) => false,
		};
		if !sandbox && !options.sandbox.arg.is_empty() {
			return Err(tg::error!(
				"sandbox options are not supported without a sandbox"
			));
		}

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
		let referent = self.get_reference_with_arg(&reference, arg).await?;
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
				let object = command.object_with_handle(&handle).await?;
				command_env = Some(object.env.clone());
				tg::Command::builder()
					.host(object.host.clone())
					.executable(object.executable.clone())
					.args(object.args.clone())
					.cwd(object.cwd.clone())
					.stdin(object.stdin.clone())
			},

			tg::Object::Directory(directory) => {
				let root_module_file_name = tg::module::try_get_root_module_file_name_with_handle(
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
					.get_entry_edge_with_handle(&handle, root_module_file_name)
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
				tg::Command::builder().host(host).executable(executable)
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
					file.module_with_handle(&handle)
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
					tg::Command::builder().host(host).executable(executable)
				} else {
					let host = tg::host().to_owned();
					let executable =
						tg::command::Executable::Artifact(tg::command::ArtifactExecutable {
							artifact: file.clone().into(),
							path: None,
						});
					tg::Command::builder().host(host).executable(executable)
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
			env.extend(tg::process::env()?);
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
		let command = command.finish()?;
		command
			.store_with_handle(&handle)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the command"))?;

		// Determine if the network is enabled.
		let sandboxed = sandbox;
		let has_sandbox_arg = !options.sandbox.arg.is_empty();
		let network = options.sandbox.arg.network.get();

		// Get the mounts.
		let mut mounts = Vec::new();
		for mount in &options.sandbox.arg.mounts {
			let source = tokio::fs::canonicalize(&mount.source)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
			mounts.push(tg::sandbox::Mount {
				source,
				target: mount.target.clone(),
				readonly: mount.readonly,
			});
		}
		let sandbox = if sandboxed {
			match options.sandbox.get() {
				Some(tg::Either::Right(id)) => {
					if has_sandbox_arg {
						return Err(tg::error!(
							"sandbox options are not supported for existing sandboxes"
						));
					}
					Some(tg::Either::Right(id))
				},
				_ => Some(tg::Either::Left(tg::sandbox::create::Arg {
					cpu: options.sandbox.arg.cpu,
					hostname: options.sandbox.arg.hostname.clone(),
					location: None,
					memory: options.sandbox.arg.memory,
					mounts,
					network,
					ttl: options.sandbox.arg.ttl.unwrap_or(0),
					user: options.sandbox.arg.user.clone(),
				})),
			}
		} else {
			None
		};

		// Get the tty.
		let tty = match (options.tty.tty.clone(), options.tty.no_tty) {
			(Some(tg::Either::Left(true)), _) => {
				let size = if let Some(size) = tangram_util::tty::get_controlling_tty_size() {
					Some(tg::process::tty::Size {
						rows: size.rows,
						cols: size.cols,
					})
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
			cache_location: None,
			checksum: options.checksum,
			command: tg::Referent::with_item(command.id()),
			location: location.clone(),
			parent: None,
			retry: options.retry,
			sandbox,
			stderr: options.stderr.unwrap_or_default(),
			stdin: options.stdin.unwrap_or_default(),
			stdout: options.stdout.unwrap_or_default(),
			tty,
		};
		let quiet = self.args.quiet;
		let process = tg::Process::spawn_with_progress_with_handle(&handle, arg, |stream| {
			self.render_progress_stream_with_output(stream, |output| {
				if print && sandboxed && !quiet {
					let mut message = output.process.to_string();
					if let Some(token) = &output.token {
						write!(message, " {token}").unwrap();
					}
					Self::print_info_message(&message);
				}
			})
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;

		// Tag the process if requested.
		if let Some(tag) = options.tag {
			let item = tg::Either::Right(process.id().clone());
			let arg = tg::tag::put::Arg {
				force: false,
				item,
				location: location.clone(),
				replicate: false,
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
