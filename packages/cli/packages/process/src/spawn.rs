use {
	futures::prelude::*,
	std::{fmt::Write as _, net::ToSocketAddrs as _, path::PathBuf, time::Duration},
	tangram_cli_core::{ArgumentKind, Cli},
	tangram_client::prelude::*,
	tg::Client,
};

/// Spawn a process.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub options: Options,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	/// The reference to the command, artifact, or process.
	pub reference: Option<tg::Reference>,

	#[usage(flatten)]
	pub trailing: crate::Trailing,

	/// Print the full spawn output instead of just the process ID.
	#[usage(long, short)]
	pub verbose: bool,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Options {
	/// Set arguments as strings.
	#[usage(allow_hyphen_values = true, long = "arg-string", short = 'a')]
	pub arg_strings: Vec<String>,

	/// Set arguments as values.
	#[usage(allow_hyphen_values = true, long = "arg-value", short = 'A')]
	pub arg_values: Vec<String>,

	/// If this flag is set, then build the specified target and run its output.
	#[usage(long = "build", short = 'b')]
	pub build: bool,

	/// Wait for the process to start.
	#[usage(long = "wait")]
	pub wait: bool,

	/// Set this flag to true to require a cached process. Set this flag to false to require a new process to be created. Omit this flag to use a cached process if possible, and create a new process if not.
	#[usage(default_missing = "true", long = "cached", require_equals = true)]
	pub cached: Option<bool>,

	#[usage(flatten)]
	pub checkin: tangram_cli_checkin::Options,

	/// The output's expected checksum.
	#[usage(long = "checksum")]
	pub checksum: Option<tg::Checksum>,

	/// Set the working directory for the process.
	#[usage(long = "cwd", short = 'C')]
	pub cwd: Option<PathBuf>,

	#[usage(flatten)]
	pub debug: Debug,

	/// Set environment variables as strings.
	#[usage(long = "env-string", short = 'e')]
	pub env_strings: Vec<String>,

	/// Set environment variables as values.
	#[usage(long = "env-value", short = 'E')]
	pub env_values: Vec<String>,

	/// The executable to run.
	#[usage(long = "executable", short = 'x')]
	pub executable: Option<tg::command::data::Executable>,

	/// Set the host.
	#[usage(long = "host")]
	pub host: Option<String>,

	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	/// Grant temporary public read access to the process so that its result is reusable by anyone.
	#[usage(long = "public")]
	pub public: bool,

	/// Whether to retry failed processes.
	#[usage(long = "retry")]
	pub retry: bool,

	#[usage(flatten)]
	pub sandbox: Sandbox,

	/// Set the stderr mode.
	#[usage(long = "stderr")]
	pub stderr: Option<tg::process::Stdio>,

	/// Set the stdin mode.
	#[usage(long = "stdin")]
	pub stdin: Option<tg::process::Stdio>,

	/// Set the stdout mode.
	#[usage(long = "stdout")]
	pub stdout: Option<tg::process::Stdio>,

	#[usage(flatten)]
	pub tag: tangram_cli_tag::Tag,

	#[usage(flatten)]
	pub tty: Tty,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Debug {
	#[usage(env = "TANGRAM_JAVASCRIPT_DEBUG_ADDR", long = "debug-addr")]
	addr: Option<std::net::SocketAddr>,

	#[usage(env = "TANGRAM_JAVASCRIPT_DEBUG", long = "debug")]
	enabled: bool,

	#[usage(
		long = "inspect",
		default_missing = "127.0.0.1:9229",
		require_equals = true
	)]
	inspect: Option<InspectAddress>,

	#[usage(
		long = "inspect-brk",
		default_missing = "127.0.0.1:9229",
		require_equals = true
	)]
	inspect_brk: Option<InspectAddress>,

	#[usage(
		long = "inspect-wait",
		default_missing = "127.0.0.1:9229",
		require_equals = true
	)]
	inspect_wait: Option<InspectAddress>,

	#[usage(env = "TANGRAM_JAVASCRIPT_DEBUG_MODE", long = "debug-mode")]
	mode: Option<tg::process::debug::Mode>,
}

#[derive(Clone, Copy, Debug)]
struct InspectAddress(std::net::SocketAddr);

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Sandbox {
	#[usage(flatten)]
	pub arg: tangram_cli_sandbox::Options,
	#[usage(
		default_missing = "true",
		long = "no-sandbox",
		overrides("--sandbox"),
		require_equals = true
	)]
	disabled: Option<bool>,
	#[usage(flatten)]
	pub time_to_live: TimeToLive,
	#[usage(flatten)]
	pub time_to_stop: TimeToStop,
	/// Whether to sandbox the process, or an existing sandbox to use.
	#[usage(
		default_missing = "true",
		long = "sandbox",
		overrides("--no-sandbox"),
		require_equals = true
	)]
	value: Option<tg::Either<bool, tg::Referent<tg::sandbox::Id>>>,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct TimeToLive {
	/// Keep the sandbox until it is destroyed.
	#[usage(
		alias = "no-ttl",
		long = "no-time-to-live",
		overrides("--time-to-live")
	)]
	pub no_time_to_live: bool,

	/// The time that the sandbox may go unused before it is destroyed.
	#[usage(alias = "ttl", long = "time-to-live", overrides("--no-time-to-live"))]
	pub time_to_live: Option<humantime::Duration>,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct TimeToStop {
	/// Keep the sandbox running until it is stopped.
	#[usage(long = "no-time-to-stop", overrides("--time-to-stop"))]
	pub no_time_to_stop: bool,

	/// The time that the sandbox may run without processes before it is stopped.
	#[usage(long = "time-to-stop", overrides("--no-time-to-stop"))]
	pub time_to_stop: Option<humantime::Duration>,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Tty {
	/// Whether to allocate a tty for the process. Optionally set the size as rows,cols.
	#[usage(
		default_missing = "true",
		long = "tty",
		overrides("--no-tty"),
		require_equals = true,
		value_name = "ROWS,COLS"
	)]
	pub(crate) tty: Option<tg::Either<bool, tg::process::tty::Size>>,

	#[usage(
		default_missing = "true",
		long = "no-tty",
		overrides("--tty"),
		require_equals = true
	)]
	pub(crate) no_tty: Option<bool>,
}

pub(crate) struct InnerOutput {
	pub arg: tg::process::Arg,
	pub client: Client,
	pub location: Option<tg::location::Arg>,
	pub referent: tg::Referent<tg::graph::Edge<tg::Object>>,
	pub sandboxed: bool,
	pub tag: tangram_cli_tag::Tag,
}

pub async fn run(cli: &mut Cli, mut args: Args) -> tg::Result<()> {
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

	let output = spawn(
		cli,
		args.options,
		args.reference,
		args.trailing.into_values(),
		None,
		tg::process::attach::Mode::Spawn,
	)
	.boxed()
	.await?;

	output.node().detach().await?;

	if args.verbose {
		let output = tg::process::spawn::Output {
			cached: output.node().cached().unwrap_or(false),
			command: None,
			lease: output.node().lease().copied(),
			location: output
				.node()
				.location()
				.and_then(|location| location.to_location()),
			outcome: output.node().outcome_data(),
			process: output.node().id().cloned(),
			tokens: output.node().tokens(),
		};
		cli.print_serde(output, args.print).await?;
	} else {
		Cli::print_referent(&output.node().to_referent(), &args.print);
	}

	Ok(())
}

pub(crate) async fn spawn(
	cli: &mut Cli,
	options: Options,
	reference: Option<tg::Reference>,
	trailing: Vec<String>,
	print: Option<tangram_cli_core::print::Options>,
	mode: tg::process::attach::Mode,
) -> tg::Result<tg::Referent<tg::Process>> {
	let InnerOutput {
		arg,
		client,
		location,
		referent,
		sandboxed,
		tag,
	} = spawn_inner(cli, options, reference, trailing).await?;

	let options = tg::process::spawn::Options { mode };
	let process = tg::Process::spawn_with_progress_with_instance(&client, arg, options, |stream| {
		cli.render_progress_stream_with_output(stream, |cli, output| {
			if let Some(print) = &print
				&& sandboxed
			{
				let options = tg::referent::Options {
					location: output.location.clone(),
					tokens: output.tokens.clone(),
					..Default::default()
				};
				let referent = tg::Referent::new(output.process.clone(), options);
				let mut message = Cli::format_referent(&referent, false, print.tokens.get());
				if let Some(lease) = &output.lease {
					write!(message, " {lease}").unwrap();
				}
				cli.print_info_message(&message);
			}
		})
	})
	.await
	.map_err(|error| tg::error!(!error, "failed to spawn the process"))?;

	// Tag the process if requested.
	let ancestors = tag.ancestors();
	let force = tag.force;
	if let Some(specifier) = tag.specifier {
		let id = process
			.id()
			.right()
			.cloned()
			.ok_or_else(|| tg::error!("a tag requires a sandboxed process"))?;
		let arg = tg::tag::put::Arg {
			ancestors,
			force,
			location: location.clone(),
			public: false,
			specifier: specifier.clone(),
			target: id.into(),
			tokens: referent.options.tokens.clone(),
		};
		client
			.put_tag(arg)
			.await
			.map_err(|error| tg::error!(!error, %specifier, "failed to tag the process"))?;
	}

	let process = referent.replace(process).0;

	Ok(process)
}

pub(crate) async fn spawn_inner(
	cli: &mut Cli,
	options: Options,
	reference: Option<tg::Reference>,
	trailing: Vec<String>,
) -> tg::Result<InnerOutput> {
	let client = cli.client().await?;
	let location = options.location.get();
	let debug = options.debug.get();
	if location
		.as_ref()
		.is_some_and(|location| location.to_location().is_none())
	{
		return Err(tg::error!("expected exactly one location"));
	}

	// Handle the build flag.
	let reference = if options.build {
		let build_args = crate::build::Args {
			options: crate::build::Options {
				spawn: crate::spawn::Options {
					location: options.location.clone(),
					retry: options.retry,
					..Default::default()
				},
				..Default::default()
			},
			reference,
			trailing: crate::Trailing::default(),
		};
		let output = Box::pin(crate::build::build(cli, build_args)).await?;
		let object = output
			.try_unwrap_object()
			.ok()
			.ok_or_else(|| tg::error!("expected the build to output an object"))?;
		Some(tg::Reference::with_object(object.id()))
	} else {
		reference
	};

	// Determine whether to sandbox.
	let sandbox = options.sandboxed();

	let mut executable = options.executable;
	let executable_path = match (&reference, &executable) {
		(Some(_), Some(executable)) if executable.artifact.is_none() => executable.path.clone(),
		_ => None,
	};
	if executable_path.is_some() {
		executable = None;
	}

	let reference = if let Some(reference) = reference {
		reference
	} else {
		tg::Reference::with_path(".".into())
	};

	if !sandbox && location.is_some() {
		return Err(tg::error!("a location is not supported without a sandbox"));
	}
	if !sandbox && !options.sandbox.arg.is_empty() {
		return Err(tg::error!(
			"sandbox options are not supported without a sandbox"
		));
	}

	// Get the reference.
	let mut reference = reference;
	let mut reference_options = reference.options().clone();
	reference_options.follow = true;
	reference.set_options(reference_options);
	let arg = tg::get::Arg {
		checkin: options.checkin.to_options(),
		..Default::default()
	};
	let referent = tangram_cli_reference::get_with_arg(cli, &reference, arg)
		.await?
		.referent;
	let mut referent = match referent.node.clone() {
		tg::get::Node::Id(id) if id.kind() == tg::id::Kind::Process => {
			let id = tg::process::Id::try_from(id)?;
			let process = tg::Process::<tg::Value>::with_referent(referent.map(|_| id));
			let state = process
				.load_with_instance(&client)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the process's command"))?;
			let command = process.command_with_instance(&client).await?;
			let command = match command {
				tg::Either::Left(command) => {
					let arg = tg::process::spawn::CommandArg {
						args: command.args,
						cwd: command.cwd,
						env: command.env,
						executable: command.executable,
						host: Some(command.host),
						stdin: command.stdin,
						user: command.user,
					};
					tg::command::Builder::try_with_spawn_arg(arg)?.build()?
				},
				tg::Either::Right(command) => command,
			};
			let node = tg::graph::Edge::Object(tg::Object::Command(command));
			tg::Referent::new(node, state.command.options.clone())
		},
		_ => referent.into_graph_edge()?,
	};

	// Set the args.
	let mut args_: Vec<tg::command::Value> = Vec::new();
	for (kind, value) in cli.arguments(options.arg_strings, options.arg_values) {
		let value = match kind {
			ArgumentKind::String => value.into(),
			ArgumentKind::Value => {
				let value = value
					.parse()
					.map_err(|error| tg::error!(!error, "failed to parse the arg"))?;
				tg::command::Value::Value(value)
			},
		};
		args_.push(value);
	}
	for arg in trailing {
		args_.push(arg.into());
	}

	// Create the command builder.
	let mut command_env = None;
	let mut command_options = None;
	let mut command = match referent.node.clone() {
		tg::graph::Edge::Index(_) => return Err(tg::error!("missing graph")),

		tg::graph::Edge::Object(tg::Object::Command(_)) if executable_path.is_some() => {
			return Err(tg::error!("expected an artifact for the executable path"));
		},

		tg::graph::Edge::Object(tg::Object::Command(command)) => {
			let object = command.object_with_instance(&client).await?;
			command_env = Some(object.env.clone());
			command_options = Some(referent.options.clone());
			tg::command::Builder::with_object(&object)
		},

		_ if executable.is_some() => tg::Command::builder(),

		edge => {
			let artifact = match edge.clone() {
				tg::graph::Edge::Index(_) => return Err(tg::error!("missing graph")),
				tg::graph::Edge::Pointer(pointer) => tg::Artifact::with_pointer(pointer),
				tg::graph::Edge::Object(tg::Object::Directory(directory)) => directory.into(),
				tg::graph::Edge::Object(tg::Object::File(file)) => file.into(),
				tg::graph::Edge::Object(tg::Object::Symlink(symlink)) => symlink.into(),
				tg::graph::Edge::Object(_) => {
					return Err(tg::error!("expected a command or an artifact"));
				},
			};

			if let Some(path) = executable_path {
				let executable = tg::command::Executable {
					artifact: Some(artifact),
					path: Some(path),
				};
				tg::Command::builder().executable(executable)
			} else {
				match artifact {
					tg::Artifact::Directory(directory) => {
						let root_module_file_name =
							tg::module::try_get_root_module_file_name_with_instance(
								&client,
								tg::Either::Left(&directory),
							)
							.await
							.map_err(|error| {
								tg::error!(!error, "failed to get the root module name")
							})?
							.ok_or_else(
								|| tg::error!(directory = %directory.id(), "failed to find a root module"),
							)?;
						let kind = tg::module::module_kind_for_path(root_module_file_name).unwrap();
						if let Some(path) = &mut referent.options.path {
							*path = path.join(root_module_file_name);
						} else {
							referent.options.path.replace(root_module_file_name.into());
						}
						let edge = directory
							.get_entry_edge_with_instance(&client, root_module_file_name)
							.await
							.map_err(|error| tg::error!(!error, "failed to get the root module"))?;
						let source = tg::module::Source::Edge(edge.into());
						command_options = Some(referent.options.clone());
						let mut referent = referent.clone().map(|_| source);
						referent.options.name.take();
						referent.options.id.take();
						referent.options.path.take();
						referent.options.tag.take();
						let module = tg::Module { kind, referent };
						let export = reference.export().unwrap_or("default").to_owned();
						tg::command::Builder::with_module(
							module,
							Some(export),
							std::mem::take(&mut args_),
						)
					},

					tg::Artifact::File(file) => {
						let kind = referent.path().and_then(|path| {
							tg::module::module_kind_for_path(path)
								.ok()
								.or_else(|| tg::file::xattrs::read_module(path).ok().flatten())
						});
						let kind = if kind.is_some() {
							kind
						} else {
							file.module_with_instance(&client).await.map_err(|error| {
								tg::error!(!error, "failed to get the module kind")
							})?
						};
						if let Some(kind) = kind {
							let source = tg::module::Source::Edge(edge);
							command_options = Some(referent.options.clone());
							let mut referent = referent.clone().map(|_| source);
							referent.options.name.take();
							referent.options.id.take();
							referent.options.path.take();
							referent.options.tag.take();
							let module = tg::Module { kind, referent };
							let export = reference.export().unwrap_or("default").to_owned();
							tg::command::Builder::with_module(
								module,
								Some(export),
								std::mem::take(&mut args_),
							)
						} else {
							let executable = tg::command::Executable {
								artifact: Some(file.clone().into()),
								path: None,
							};
							tg::Command::builder().executable(executable)
						}
					},

					tg::Artifact::Symlink(_) => {
						return Err(tg::error!("unimplemented"));
					},
				}
			}
		},
	};
	if let Some(executable) = executable {
		command = command.executable(tg::command::Executable::try_from_data(executable)?);
		if let Some(host) = options.host.clone() {
			command = command.host(host);
		}
	} else if let Some(host) = options.host.clone() {
		command = command.host(host);
	}
	if let Some(tg::process::Stdio::Blob(blob)) = &options.stdin {
		command = command.stdin(Some(tg::Blob::with_referent(*blob.clone())));
	}

	command = command.args(args_);

	// Set the cwd.
	if let Some(cwd) = options.cwd {
		command = command.cwd(cwd);
	}

	// Set the env.
	let mut env = command_env.unwrap_or_default();
	for string in options.env_strings {
		let (key, value) = string
			.split_once('=')
			.ok_or_else(|| tg::error!("expected KEY=VALUE"))?;
		let key = key.to_owned();
		let value = value.to_owned().into();
		env.insert(key, value);
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
			let current = env.remove(&key);
			let value_mode = matches!(current, Some(tg::command::Value::Value(_)));
			let mut values = tg::value::Map::new();
			if let Some(current) = current {
				values.insert(key.clone(), current.into_value());
			}
			mutation.apply(&mut values, &key)?;
			if let Some(value) = values.remove(&key) {
				let value = if value_mode {
					tg::command::Value::Value(value)
				} else {
					tg::command::Value::String(value)
				};
				env.insert(key, value);
			}
		} else {
			env.insert(key, tg::command::Value::Value(value));
		}
	}
	command = command.env(env);

	// Create the command.
	let sandboxed = sandbox;
	let (command, command_stdin) = if sandboxed {
		let objects = command
			.objects()
			.into_iter()
			.map(tg::Value::Object)
			.collect();
		tg::Value::Array(objects)
			.store_with_instance(&client)
			.await?;
		let command = command.build_spawn_arg()?;
		let stdin = command.stdin.clone();
		(tg::Either::Left(command), stdin)
	} else {
		let command = command.build()?;
		let object = command.object_with_instance(&client).await?;
		let stdin = object.stdin.as_ref().map(tg::Blob::to_referent);
		(tg::Either::Right(command), stdin)
	};

	// Determine if the network is enabled.
	let has_sandbox_arg = !options.sandbox.arg.is_empty()
		|| options.sandbox.time_to_live.time_to_live.is_some()
		|| options.sandbox.time_to_live.no_time_to_live
		|| options.sandbox.time_to_stop.time_to_stop.is_some()
		|| options.sandbox.time_to_stop.no_time_to_stop;
	let ports = options.sandbox.arg.ports.clone();
	let network = tangram_cli_sandbox::normalize_network(&options.sandbox.arg.network, ports)?;

	// Get the mounts.
	let mut mounts = Vec::new();
	for mount in &options.sandbox.arg.mounts {
		let source = tokio::fs::canonicalize(&mount.source)
			.await
			.map_err(|error| tg::error!(!error, "failed to canonicalize the path"))?;
		mounts.push(tg::sandbox::Mount {
			readonly: mount.readonly,
			source,
			target: mount.target.clone(),
		});
	}
	let sandbox = if sandboxed {
		if let Some(tg::Either::Right(id)) = options.sandbox.get() {
			if has_sandbox_arg {
				return Err(tg::error!(
					"sandbox options are not supported for existing sandboxes"
				));
			}
			Some(tg::process::SandboxArg::Referent(id))
		} else {
			let owner =
				tangram_cli_sandbox::resolve_owner(&client, &options.sandbox.arg.owner).await?;
			Some(tg::process::SandboxArg::Arg(
				tg::process::SandboxCreateArg {
					cpu: tangram_cli_sandbox::cpu(&options.sandbox.arg),
					hostname: options.sandbox.arg.hostname.clone(),
					location: None,
					memory: tangram_cli_sandbox::resource(
						options.sandbox.arg.memory_min,
						options.sandbox.arg.memory,
					),
					mounts,
					network,
					owner,
					storage: tangram_cli_sandbox::resource(
						options.sandbox.arg.storage_min,
						options.sandbox.arg.storage,
					),
					time_to_live: Some(options.sandbox.time_to_live.get().map_or(
						tg::process::SandboxExpiration::Disabled,
						tg::process::SandboxExpiration::After,
					)),
					time_to_stop: options.sandbox.time_to_stop.get(),
				},
			))
		}
	} else {
		None
	};

	// Get the tty.
	let tty = match (options.tty.tty.clone(), options.tty.no_tty) {
		(Some(tg::Either::Left(true)), _) => tangram_util::tty::get_controlling_tty_size()
			.map(|size| tg::process::tty::Size {
				cols: size.cols,
				rows: size.rows,
			})
			.map(|size| tg::Either::Right(tg::process::Tty { size })),
		(Some(tg::Either::Right(size)), _) => {
			let tty = tg::process::Tty { size };
			Some(tg::Either::Right(tty))
		},
		(Some(tg::Either::Left(false)), _) | (_, Some(true)) => Some(tg::Either::Left(false)),
		_ => None,
	};

	let stdin = match (options.stdin.clone(), command_stdin) {
		(None | Some(tg::process::Stdio::Null), Some(blob)) => {
			tg::process::Stdio::Blob(Box::new(blob))
		},
		(Some(stdin), _) => stdin,
		(None, None) => tg::process::Stdio::default(),
	};

	let arg = tg::process::Arg {
		args: Vec::new(),
		cached: options.cached,
		checksum: options.checksum,
		command: Some(tg::Referent::new(
			command,
			command_options.unwrap_or_default(),
		)),
		debug: debug.map(tg::Either::Right),
		location: location.clone(),
		name: None,
		public: options.public,
		retry: options.retry,
		sandbox,
		stderr: options.stderr.unwrap_or_default(),
		stdin,
		stdout: options.stdout.unwrap_or_default(),
		tty,
		wait: options.wait,
		..Default::default()
	};

	Ok(InnerOutput {
		arg,
		client,
		location,
		referent,
		sandboxed,
		tag: options.tag,
	})
}

impl Options {
	/// Determine whether the process runs in a sandbox.
	pub(crate) fn sandboxed(&self) -> bool {
		match self.sandbox.get() {
			None => {
				self.cached.is_some_and(|v| v)
					|| self.checksum.is_some()
					|| !self.sandbox.arg.is_empty()
					|| self.location.get().is_some()
					|| matches!(
						self.tty.tty,
						Some(tg::Either::Left(true) | tg::Either::Right(_))
					)
			},
			Some(tg::Either::Left(true) | tg::Either::Right(_)) => true,
			Some(tg::Either::Left(false)) => false,
		}
	}
}

impl Debug {
	pub fn get(&self) -> Option<tg::process::Debug> {
		let mut debug = self.enabled.then(tg::process::Debug::default);
		if let Some(addr) = self.addr {
			debug.get_or_insert_with(tg::process::Debug::default).addr = Some(addr);
		}
		if let Some(mode) = self.mode {
			debug.get_or_insert_with(tg::process::Debug::default).mode = mode;
		}
		if let Some(InspectAddress(addr)) = self.inspect {
			let debug = debug.get_or_insert_with(tg::process::Debug::default);
			debug.addr = Some(addr);
			debug.mode = tg::process::debug::Mode::Normal;
		}
		if let Some(InspectAddress(addr)) = self.inspect_brk {
			let debug = debug.get_or_insert_with(tg::process::Debug::default);
			debug.addr = Some(addr);
			debug.mode = tg::process::debug::Mode::Break;
		}
		if let Some(InspectAddress(addr)) = self.inspect_wait {
			let debug = debug.get_or_insert_with(tg::process::Debug::default);
			debug.addr = Some(addr);
			debug.mode = tg::process::debug::Mode::Wait;
		}
		debug
	}
}

impl Sandbox {
	#[must_use]
	pub fn new(sandbox: Option<tg::Either<bool, tg::Referent<tg::sandbox::Id>>>) -> Self {
		Self {
			arg: tangram_cli_sandbox::Options::default(),
			disabled: None,
			time_to_live: TimeToLive::default(),
			time_to_stop: TimeToStop::default(),
			value: sandbox,
		}
	}

	#[must_use]
	pub fn get(&self) -> Option<tg::Either<bool, tg::Referent<tg::sandbox::Id>>> {
		self.value
			.clone()
			.or(self.disabled.map(|v| tg::Either::Left(!v)))
	}

	pub fn set(&mut self, sandbox: Option<tg::Either<bool, tg::Referent<tg::sandbox::Id>>>) {
		self.value = sandbox;
		self.disabled = None;
	}
}

impl TimeToLive {
	fn get(&self) -> Option<Duration> {
		if self.no_time_to_live {
			None
		} else {
			self.time_to_live.map(Into::into).or(Some(Duration::ZERO))
		}
	}
}

impl TimeToStop {
	fn get(&self) -> Option<Duration> {
		if self.no_time_to_stop {
			None
		} else {
			self.time_to_stop.map(Into::into)
		}
	}
}

impl Tty {
	#[must_use]
	pub fn new_disabled() -> Self {
		Self {
			no_tty: Some(true),
			tty: None,
		}
	}
}

impl std::str::FromStr for InspectAddress {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		parse_inspect_addr(s).map(Self)
	}
}

fn parse_inspect_addr(s: &str) -> Result<std::net::SocketAddr, String> {
	if let Ok(addr) = s.parse() {
		return Ok(addr);
	}
	if let Ok(port) = s.parse() {
		return Ok(std::net::SocketAddr::new(
			std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
			port,
		));
	}
	s.to_socket_addrs()
		.map_err(|_| format!("invalid inspect address `{s}`"))?
		.next()
		.ok_or_else(|| format!("invalid inspect address `{s}`"))
}
