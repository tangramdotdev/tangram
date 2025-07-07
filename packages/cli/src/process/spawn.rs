use crate::Cli;
use futures::FutureExt as _;
use std::path::{Path, PathBuf};
use tangram_client::{self as tg, prelude::*};
use tangram_either::Either;

/// Spawn a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	/// The reference to the command.
	#[arg(index = 1)]
	pub reference: Option<tg::Reference>,

	/// Set arguments.
	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Options {
	/// Set arguments as strings.
	#[arg(short = 'a', long = "arg-string", num_args = 1, action = clap::ArgAction::Append)]
	pub arg_strings: Vec<String>,

	/// Set arguments as values.
	#[arg(short = 'A', long = "arg-value", num_args = 1, action = clap::ArgAction::Append)]
	pub arg_values: Vec<String>,

	/// Set this flag to true to require a cached process. Set this flag to false to require a new process to be created. Omit this flag to use a cached process if possible, and create a new process if not.
	#[arg(long, action = clap::ArgAction::Set)]
	pub cached: Option<bool>,

	/// Whether to check out the output. The output must be an artifact. A path to check out to may be provided.
	#[arg(long)]
	pub checksum: Option<tg::Checksum>,

	/// Set the working directory for the process.
	#[arg(short = 'C', long)]
	pub cwd: Option<PathBuf>,

	/// Set environment variables as strings.
	#[arg(short = 'e', long = "env-string", num_args = 1, action = clap::ArgAction::Append)]
	pub env_strings: Vec<String>,

	/// Set environment variables as values.
	#[arg(short = 'E', long = "env-value", num_args = 1, action = clap::ArgAction::Append)]
	pub env_values: Vec<String>,

	/// Set the host.
	#[arg(long)]
	pub host: Option<String>,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	/// Configure mounts.
	#[arg(short, long = "mount", num_args = 1, action = clap::ArgAction::Append)]
	pub mounts: Vec<Either<tg::process::Mount, tg::command::Mount>>,

	/// Enable network access.
	#[arg(long)]
	pub network: bool,

	/// The remote to use.
	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,

	/// Whether to retry failed processes.
	#[arg(long)]
	pub retry: bool,

	/// Whether the process should be sandboxed.
	#[arg(long)]
	pub sandbox: bool,

	/// Tag the process.
	#[arg(long)]
	pub tag: Option<tg::Tag>,

	/// Allocate a terminal when running the process.
	#[arg(long, default_value = "true", action = clap::ArgAction::Set)]
	pub tty: bool,
}

impl Cli {
	pub async fn command_process_spawn(&mut self, args: Args) -> tg::Result<()> {
		let reference = args.reference.unwrap_or_else(|| ".".parse().unwrap());
		let (_, process) = self
			.spawn(args.options, reference, args.trailing, None, None, None)
			.boxed()
			.await?;
		println!("{}", process.id());
		Ok(())
	}

	pub(crate) async fn spawn(
		&mut self,
		options: Options,
		reference: tg::Reference,
		trailing: Vec<String>,
		stderr: Option<tg::process::Stdio>,
		stdin: Option<tg::process::Stdio>,
		stdout: Option<tg::process::Stdio>,
	) -> tg::Result<(tg::Referent<tg::object::Id>, tg::Process)> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = options
			.remote
			.clone()
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));

		// Determine if the process is sandboxed.
		let sandbox = options.sandbox || remote.is_some();

		// If the reference is a path to a directory and the path does not contain a root module, then init.
		if let Ok(path) = reference.item().try_unwrap_path_ref() {
			let metadata = tokio::fs::metadata(&path).await.map_err(
				|source| tg::error!(!source, ?path = path.display(), "failed to get the metadata"),
			)?;
			if metadata.is_dir() {
				let mut exists = false;
				for name in tg::package::ROOT_MODULE_FILE_NAMES {
					let module_path = path.join(name);
					exists = tokio::fs::try_exists(&module_path)
						.await
						.map_err(|source| {
							tg::error!(!source, ?path, "failed to check if the path exists")
						})?;
					if exists {
						break;
					}
				}
				if !exists {
					self.command_init(crate::init::Args {
						path: Some(path.clone()),
					})
					.await?;
				}
			}
		}

		// Get the reference.
		let referent = self.get_reference(&reference).await?;
		let item = referent
			.item
			.right()
			.ok_or_else(|| tg::error!("expected an object"))?;
		let mut referent = tg::Referent {
			item,
			path: referent.path,
			tag: referent.tag,
		};

		// If the reference's path is relative, then make the referent's path relative to the current working directory.
		referent.path = referent
			.path
			.take()
			.map(|path| {
				if reference.path().is_none_or(Path::is_absolute) {
					Ok(path)
				} else {
					let current_dir = std::env::current_dir()
						.map_err(|source| tg::error!(!source, "failed to get current dir"))?;
					crate::util::path_diff(&current_dir, &path)
				}
			})
			.transpose()?;

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
				let root_module_file_name = tg::package::try_get_root_module_file_name(
					&handle,
					Either::Left(&directory.clone().into()),
				)
				.await?
				.ok_or_else(|| tg::error!("could not determine the executable"))?;
				if let Some(path) = &mut referent.path {
					*path = path.join(root_module_file_name);
				} else {
					referent.path.replace(root_module_file_name.into());
				}
				let kind = if Path::new(root_module_file_name)
					.extension()
					.is_some_and(|extension| extension == "js")
				{
					tg::module::Kind::Js
				} else if Path::new(root_module_file_name)
					.extension()
					.is_some_and(|extension| extension == "ts")
				{
					tg::module::Kind::Ts
				} else {
					unreachable!();
				};
				let item = directory.get(&handle, root_module_file_name).await?;
				let item = tg::module::Item::Object(item.into());
				let referent = tg::Referent::with_item(item);
				let module = tg::Module { kind, referent };
				let export = reference
					.uri()
					.fragment()
					.map_or("default", |fragment| fragment)
					.to_owned();
				let host = "js".to_owned();
				let executable = tg::command::Executable::Module(tg::command::ModuleExecutable {
					module,
					export: Some(export),
				});
				tg::Command::builder(host, executable)
			},

			tg::Object::File(file) => {
				let path = referent.path.as_ref().and_then(|path| {
					if tg::package::is_module_path(path) {
						Some(path)
					} else {
						None
					}
				});
				if let Some(path) = path {
					let kind = if path.extension().is_some_and(|extension| extension == "js") {
						tg::module::Kind::Js
					} else if path.extension().is_some_and(|extension| extension == "ts") {
						tg::module::Kind::Ts
					} else {
						unreachable!()
					};
					let item = tg::module::Item::Object(file.clone().into());
					let referent = tg::Referent::with_item(item);
					let module = tg::Module { kind, referent };
					let export = reference
						.uri()
						.fragment()
						.map_or("default", |fragment| fragment)
						.to_owned();
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

		// Set the args.
		let mut args_: Vec<tg::Value> = Vec::new();
		let mut matches = &self.matches;
		while let Some((_, matches_)) = matches.subcommand() {
			matches = matches_;
		}
		let arg_string_indices = matches.indices_of("arg_strings").unwrap_or_default();
		let arg_value_indices = matches.indices_of("arg_values").unwrap_or_default();
		let mut indexed: Vec<(usize, tg::Value)> = Vec::new();
		for (index, value) in arg_string_indices.zip(options.arg_strings) {
			let value = tg::Value::String(value);
			indexed.push((index, value));
		}
		for (index, value) in arg_value_indices.zip(options.arg_values) {
			let value = value
				.parse()
				.map_err(|error| tg::error!(!error, "failed to parse the arg"))?;
			indexed.push((index, value));
		}
		indexed.sort_by_key(|&(index, _)| index);
		args_.extend(indexed.into_iter().map(|(_, value)| value));
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
			env.insert("TANGRAM_HOST".to_owned(), host.to_string().into());
		}
		command = command.env(env);

		// Set the mounts.
		for mount in &options.mounts {
			if let Either::Right(mount) = mount {
				command = command.mount(mount.clone());
			}
		}

		// Create the command and store it.
		let command = command.build();
		command.store(&handle).await?;

		// Determine if the network is enabled.
		let network = !sandbox;

		// Determine the retry.
		let retry = options.retry;

		// If the remote is set, then push the command.
		if let Some(remote) = remote.clone() {
			let id = command.id();
			let arg = tg::push::Arg {
				items: vec![Either::Right(id.into())],
				remote: Some(remote),
				..Default::default()
			};
			let stream = handle.push(arg).await?;
			self.render_progress_stream(stream)
				.await
				.map_err(|source| tg::error!(!source, "failed to push the command"))?;
		}

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
			if let Either::Left(mount) = mount {
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
			command: Some(command.id()),
			mounts,
			network,
			parent: None,
			path: None,
			remote: remote.clone(),
			retry,
			stderr,
			stdin,
			stdout,
			tag: None,
		};
		let process = tg::Process::spawn(&handle, arg).await?;

		// Tag the process if requested.
		if let Some(tag) = options.tag {
			let item = Either::Left(process.id().clone());
			let arg = tg::tag::put::Arg {
				force: false,
				item,
				remote: remote.clone(),
			};
			handle.put_tag(&tag, arg).await?;
		}

		// Get the referent.
		let referent = tg::Referent {
			item: referent.item.id(),
			path: referent.path,
			tag: referent.tag,
		};

		Ok((referent, process))
	}
}

impl Default for Options {
	fn default() -> Self {
		Self {
			arg_strings: vec![],
			arg_values: vec![],
			cached: None,
			checksum: None,
			cwd: None,
			env_strings: vec![],
			env_values: vec![],
			host: None,
			locked: false,
			mounts: vec![],
			network: false,
			remote: None,
			retry: false,
			sandbox: false,
			tag: None,
			tty: true,
		}
	}
}
