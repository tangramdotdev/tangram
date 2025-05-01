use crate::Cli;
use itertools::Itertools as _;
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
	/// Set arguments.
	#[arg(short, long, num_args = 1.., action = clap::ArgAction::Append)]
	pub arg: Vec<String>,

	/// Set this flag to true to require a cached process. Set this flag to false to require a new process to be created.
	#[arg(long, action = clap::ArgAction::Set)]
	pub cached: Option<bool>,

	/// Whether to check out the output. The output must be an artifact. A path to check out to may be provided.
	#[arg(long)]
	pub checksum: Option<tg::Checksum>,

	/// Set the working directory for the process.
	#[arg(short = 'C', long)]
	pub cwd: Option<PathBuf>,

	/// Set environment variables.
	#[arg(short, long, num_args = 1.., action = clap::ArgAction::Append)]
	pub env: Vec<Vec<String>>,

	/// Set the host.
	#[arg(long)]
	pub host: Option<String>,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(long)]
	pub mount: Vec<Either<tg::process::Mount, tg::command::Mount>>,

	/// Enable network access if sandboxed
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
	#[arg(long)]
	pub no_tty: bool,
}

impl Cli {
	pub async fn command_process_spawn(&mut self, args: Args) -> tg::Result<()> {
		let reference = args.reference.unwrap_or_else(|| ".".parse().unwrap());
		let process = self
			.spawn_process(args.options, reference, args.trailing, None, None, None)
			.await?;
		println!("{}", process.id());
		Ok(())
	}

	pub(crate) async fn spawn_process(
		&mut self,
		options: Options,
		reference: tg::Reference,
		trailing: Vec<String>,
		stderr: Option<tg::process::Stdio>,
		stdin: Option<tg::process::Stdio>,
		stdout: Option<tg::process::Stdio>,
	) -> tg::Result<tg::Process> {
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
			let path = if let Some(subpath) = reference
				.options()
				.and_then(|options| options.subpath.as_ref())
			{
				path.join(subpath)
			} else {
				path.clone()
			};
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
		let Either::Right(object) = referent.item else {
			return Err(tg::error!("expected an object"));
		};
		let object = if let Some(subpath) = &referent.subpath {
			let directory = object
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			directory.get(&handle, subpath).await?.into()
		} else {
			object
		};

		// Create the command builder.
		let mut command_env = None;
		let mut command = if let tg::Object::Command(command) = object {
			let object = command.object(&handle).await?;
			command_env = Some(object.env.clone());
			let mut builder = tg::Command::builder(object.host.clone(), object.executable.clone())
				.args(object.args.clone())
				.cwd(object.cwd.clone())
				.stdin(object.stdin.clone());
			if let Some(mounts) = &object.mounts {
				builder = builder.mounts(mounts.clone());
			}
			builder
		} else {
			// Get the target.
			let target = reference
				.uri()
				.fragment()
				.map_or("default", |fragment| fragment)
				.to_owned();

			// Create the executable.
			let executable = match object {
				tg::Object::Directory(directory) => {
					let mut name = None;
					for name_ in tg::package::ROOT_MODULE_FILE_NAMES {
						if directory.try_get_entry(&handle, name_).await?.is_some() {
							name = Some(name_);
							break;
						}
					}
					let name = name.ok_or_else(|| tg::error!("no root module found"))?;
					let kind = if Path::new(name)
						.extension()
						.is_some_and(|extension| extension == "js")
					{
						tg::module::Kind::Js
					} else if Path::new(name)
						.extension()
						.is_some_and(|extension| extension == "ts")
					{
						tg::module::Kind::Ts
					} else {
						unreachable!();
					};
					let item = tg::module::Item::Object(directory.clone().into());
					let subpath = Some(name.parse().unwrap());
					let referent = tg::Referent {
						item,
						path: referent.path,
						subpath,
						tag: referent.tag,
					};
					let module = tg::Module { kind, referent };
					let target = Some(target);
					let module = tg::command::ModuleExecutable { module, target };
					tg::command::Executable::Module(module)
				},

				tg::Object::File(file) => {
					let kind = if let Ok(path) = reference.item().try_unwrap_path_ref() {
						let path = if let Some(subpath) = reference
							.options()
							.and_then(|options| options.subpath.as_ref())
						{
							path.join(subpath)
						} else {
							path.clone()
						};
						if path.extension().is_some_and(|extension| extension == "js") {
							tg::module::Kind::Js
						} else if path.extension().is_some_and(|extension| extension == "ts") {
							tg::module::Kind::Ts
						} else {
							return Err(tg::error!("invalid file extension"));
						}
					} else {
						return Err(tg::error!("cannot determine the file's kind"));
					};
					let item = tg::module::Item::Object(file.into());
					let referent = tg::Referent::with_item(item);
					let module = tg::Module { kind, referent };
					let target = Some(target);
					let module = tg::command::ModuleExecutable { module, target };
					tg::command::Executable::Module(module)
				},

				_ => {
					return Err(tg::error!("expected a directory or a file"));
				},
			};

			// Choose the host.
			let host = "js";

			// Create the command.
			tg::Command::builder(host, executable)
		};

		// Get the args.
		let args_: Vec<tg::Value> = options
			.arg
			.into_iter()
			.map(|arg| arg.parse())
			.chain(trailing.into_iter().map(tg::Value::String).map(Ok))
			.try_collect::<tg::Value, _, _>()?;
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
		for string in options.env.into_iter().flatten() {
			let map = string
				.parse::<tg::Value>()?
				.try_unwrap_map()
				.map_err(|_| tg::error!("expected a map"))?;
			for (key, value) in map {
				if let Ok(mutation) = value.try_unwrap_mutation_ref() {
					mutation.apply(&mut env, &key)?;
				} else {
					env.insert(key, value);
				}
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
		for mount in &options.mount {
			if let Either::Right(mount) = mount {
				command = command.mount(mount.clone());
			}
		}

		// Create the command.
		let command = command.build();

		// Determine if the network is enabled.
		let network = !sandbox;

		// Determine the retry.
		let retry = options.retry;

		// If the remote is set, then push the commnad.
		if let Some(remote) = remote.clone() {
			let id = command.id(&handle).await?;
			let arg = tg::push::Arg {
				items: vec![Either::Right(id.into())],
				remote: Some(remote),
				..Default::default()
			};
			let stream = handle.push(arg).await?;
			self.render_progress_stream(stream).await?;
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
		for mount in &options.mount {
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
			command: Some(command.id(&handle).await?.clone()),
			mounts,
			network,
			parent: None,
			remote: remote.clone(),
			retry,
			stderr,
			stdin,
			stdout,
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

		Ok(process)
	}
}
