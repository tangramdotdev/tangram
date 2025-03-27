use crate::Cli;
use itertools::Itertools as _;
use std::path::{Path, PathBuf};
use tangram_client::{self as tg, Handle as _};
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

	/// Whether to check out the output. The output must be an artifact. A path to check out to may be provided.
	#[arg(long)]
	pub checksum: Option<tg::Checksum>,

	/// If false, don't create a new process.
	#[arg(default_value = "true", long, action = clap::ArgAction::Set)]
	pub create: bool,

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
	#[arg(short, long)]
	pub tty: bool,
}

impl Cli {
	pub async fn command_process_spawn(&self, args: Args) -> tg::Result<()> {
		let reference = args.reference.unwrap_or_else(|| ".".parse().unwrap());
		let process = self
			.spawn_process(args.options, reference, args.trailing, None, None, None)
			.await?;
		println!("{}", process.id());
		Ok(())
	}

	pub(crate) async fn spawn_process(
		&self,
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
					self.command_package_init(crate::package::init::Args {
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
			tg::Command::builder(object.host.clone())
				.args(object.args.clone())
				.cwd(object.cwd.clone())
				.executable(object.executable.clone())
				.mounts(object.mounts.clone())
				.stdin(object.stdin.clone())
		} else {
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
					let item = directory.clone().into();
					let subpath = Some(name.parse().unwrap());
					let referent = tg::Referent {
						item,
						path: referent.path,
						subpath,
						tag: referent.tag,
					};
					let module = tg::command::Module { kind, referent };
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
					let referent = tg::Referent::with_item(file.into());
					tg::command::Executable::Module(tg::command::Module { kind, referent })
				},

				_ => {
					return Err(tg::error!("expected a directory or a file"));
				},
			};

			// Get the target.
			let target = reference
				.uri()
				.fragment()
				.map_or("default", |fragment| fragment);

			// Choose the host.
			let host = "js";

			// Create the command.
			tg::Command::builder(host)
				.arg(target.into())
				.executable(Some(executable))
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
			// let user = std::env::who
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
			insert_env(&mut env, key, value)?;
		}
		for string in options.env.into_iter().flatten() {
			let map = string
				.parse::<tg::Value>()?
				.try_unwrap_map()
				.map_err(|_| tg::error!("expected a map"))?;
			for (key, value) in map {
				insert_env(&mut env, key, value)?;
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
				remote,
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
			checksum: options.checksum,
			command: Some(command.id(&handle).await?.clone()),
			create: options.create,
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

/// Produce a single environment map by applying the mutations from each in order to a base map.
fn insert_env(env: &mut tg::value::Map, key: String, value: tg::Value) -> tg::Result<()> {
	if let Ok(mutation) = value.clone().try_unwrap_mutation() {
		match mutation {
			tg::Mutation::Unset => {
				env.remove(&key);
			},
			tg::Mutation::Set { value } => {
				env.insert(key, *value.clone());
			},
			tg::Mutation::SetIfUnset { value } => {
				let existing = env.get(&key);
				if existing.is_none() {
					env.insert(key, *value.clone());
				}
			},
			tg::Mutation::Prepend { values } => {
				if let Some(existing) = env.get(&key).cloned() {
					let existing = existing
						.try_unwrap_array()
						.map_err(|source| tg::error!(!source, "cannot prepend to a non-array"))?;
					let mut combined_values = values.clone();
					combined_values.extend(existing.iter().cloned());
					env.insert(key, tg::Value::Array(combined_values));
				} else {
					env.insert(key, tg::Value::Array(values));
				}
			},
			tg::Mutation::Append { values } => {
				if let Some(existing) = env.get(&key).cloned() {
					let existing = existing
						.try_unwrap_array()
						.map_err(|source| tg::error!(!source, "cannot apppend to a non-array"))?;
					let mut combined_values = existing.clone();
					combined_values.extend(values.iter().cloned());
					env.insert(key, tg::Value::Array(combined_values));
				} else {
					env.insert(key, tg::Value::Array(values));
				}
			},
			tg::Mutation::Prefix {
				separator,
				template,
			} => {
				if let Some(existing) = env.get(&key).cloned() {
					let existing_components = match existing {
						tg::Value::String(s) => {
							vec![tg::template::Component::String(s)]
						},
						tg::Value::Object(obj) => {
							let artifact = match obj {
								tangram_client::Object::Directory(directory) => directory.into(),
								tangram_client::Object::File(file) => file.into(),
								tangram_client::Object::Symlink(symlink) => symlink.into(),
								_ => {
									return Err(tg::error!("expected directory, file, or symlink"));
								},
							};
							vec![tg::template::Component::Artifact(artifact)]
						},
						tg::Value::Template(template) => template.components().to_vec(),
						_ => {
							return Err(tg::error!("expected string, artifact, or template"));
						},
					};
					let template_components = template.components();
					let mut combined_template = template_components.to_vec();
					if let Some(sep) = separator {
						combined_template.push(tg::template::Component::String(sep));
					}
					combined_template.extend(existing_components);
					env.insert(key, tg::Value::Template(combined_template.into()));
				} else {
					env.insert(key, tg::Value::Template(template));
				}
			},
			tg::Mutation::Suffix {
				separator,
				template,
			} => {
				if let Some(existing) = env.get(&key).cloned() {
					let existing_components = match existing {
						tg::Value::String(s) => {
							vec![tg::template::Component::String(s)]
						},
						tg::Value::Object(obj) => {
							let artifact = match obj {
								tangram_client::Object::Directory(directory) => directory.into(),
								tangram_client::Object::File(file) => file.into(),
								tangram_client::Object::Symlink(symlink) => symlink.into(),
								_ => {
									return Err(tg::error!("expected directory, file, or symlink"));
								},
							};
							vec![tg::template::Component::Artifact(artifact)]
						},
						tg::Value::Template(template) => template.components().to_vec(),
						_ => {
							return Err(tg::error!("expected string, artifact, or template"));
						},
					};
					let mut combined_template = existing_components.clone();
					if let Some(separator) = separator {
						combined_template.push(tg::template::Component::String(separator));
					}
					combined_template.extend(template.components().iter().cloned());
					env.insert(key, tg::Value::Template(combined_template.into()));
				} else {
					env.insert(key, tg::Value::Template(template));
				}
			},
		}
	} else {
		env.insert(key, value);
	}
	Ok(())
}
