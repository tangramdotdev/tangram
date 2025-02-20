use crate::Cli;
use itertools::Itertools as _;
use std::path::Path;
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

	/// Set environment variables.
	#[arg(short, long, num_args = 1.., action = clap::ArgAction::Append)]
	pub env: Vec<Vec<String>>,

	/// Set the host.
	#[arg(long)]
	pub host: Option<String>,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

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
}

impl Cli {
	pub async fn command_process_spawn(&self, args: Args) -> tg::Result<()> {
		let reference = args.reference.unwrap_or_else(|| ".".parse().unwrap());
		let process = self
			.spawn_process(args.options, reference, args.trailing)
			.await?;
		println!("{}", process.id());
		Ok(())
	}

	pub(crate) async fn spawn_process(
		&self,
		options: Options,
		reference: tg::Reference,
		trailing: Vec<String>,
	) -> tg::Result<tg::Process> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = options
			.remote
			.clone()
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));

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

		// Create the command.
		let command = if let tg::Object::Command(command) = object {
			// If the object is a command, then use it.
			command
		} else {
			// Otherwise, the object must be a directory containing a root module, or a file.
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

			// Get the args.
			let mut args_: Vec<tg::Value> = options
				.arg
				.into_iter()
				.map(|arg| arg.parse())
				.chain(trailing.into_iter().map(tg::Value::String).map(Ok))
				.try_collect::<tg::Value, _, _>()?;
			args_.insert(0, target.into());

			// Get the env.
			let mut env: tg::value::Map = options
				.env
				.into_iter()
				.flatten()
				.map(|env| {
					let map = env
						.parse::<tg::Value>()?
						.try_unwrap_map()
						.map_err(|_| tg::error!("expected a map"))?
						.into_iter();
					Ok::<_, tg::Error>(map)
				})
				.try_fold(tg::value::Map::new(), |mut map, item| {
					map.extend(item?);
					Ok::<_, tg::Error>(map)
				})?;

			// Set the TANGRAM_HOST environment variable if it is not set.
			if !env.contains_key("TANGRAM_HOST") {
				let host = if let Some(host) = options.host {
					host
				} else {
					tg::host().to_owned()
				};
				env.insert("TANGRAM_HOST".to_owned(), host.to_string().into());
			}

			// Choose the host.
			let host = "js";

			// Create the command.
			tg::command::Builder::new(host)
				.executable(Some(executable))
				.args(args_)
				.env(env)
				.build()
		};

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

		// Handle build vs run.
		let (cwd, env, network) = if options.sandbox {
			let cwd = None;
			let env = None;
			let network = false;
			(cwd, env, network)
		} else {
			let cwd =
				Some(std::env::current_dir().map_err(|source| {
					tg::error!(!source, "failed to get the working directory")
				})?);
			let env = Some(std::env::vars().collect());
			let network = true;
			(cwd, env, network)
		};

		// Spawn the process.
		let arg = tg::process::spawn::Arg {
			checksum: options.checksum,
			command: Some(command.id(&handle).await?.clone()),
			create: options.create,
			cwd,
			env,
			network,
			parent: None,
			remote: remote.clone(),
			retry,
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
