use crate::Cli;
use crossterm::style::Stylize as _;
use futures::TryStreamExt as _;
use itertools::Itertools as _;
use std::{
	io::IsTerminal as _,
	path::{Path, PathBuf},
};
use tangram_client::{self as tg, handle::Ext as _, Handle as _};
use tangram_either::Either;
use tangram_futures::task::Task;

/// Run a command.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub inner: crate::command::run::InnerArgs,

	/// The reference to the command to run.
	#[arg(index = 1)]
	pub reference: Option<tg::Reference>,

	/// Arguments to pass to the executable.
	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

pub enum InnerKind {
	Build,
	Run,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct InnerArgs {
	/// Set the arguments.
	#[arg(short, long, num_args = 1.., action = clap::ArgAction::Append)]
	pub arg: Vec<String>,

	/// Whether to check out the output. The output must be an artifact. A path to check out to may be provided.
	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub checkout: Option<Option<PathBuf>>,

	/// If false, don't create a new process.
	#[arg(default_value = "true", long, action = clap::ArgAction::Set)]
	pub create: bool,

	/// If this flag is set, then exit immediately instead of waiting for the process to finish.
	#[arg(short, long, conflicts_with = "checkout")]
	pub detach: bool,

	/// Set the environment variables.
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

	/// Tag the process.
	#[arg(long)]
	pub tag: Option<tg::Tag>,

	/// Choose the view.
	#[arg(long, default_value = "inline")]
	pub view: View,
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum View {
	None,
	#[default]
	Inline,
	Fullscreen,
}

#[derive(Clone, Debug, derive_more::Unwrap)]
pub enum InnerOutput {
	Detached(tg::process::Id),
	Path(PathBuf),
	Value(tg::Value),
}

impl Cli {
	pub async fn command_command_run(&self, args: Args) -> tg::Result<()> {
		// Get the reference.
		let reference = args
			.reference
			.clone()
			.unwrap_or_else(|| ".".parse().unwrap());

		// Run the command.
		let kind = InnerKind::Run;
		let output = self.command_run_inner(reference, kind, args.inner).await?;

		// Print the output.
		match output {
			crate::command::run::InnerOutput::Detached(process) => {
				println!("{process}");
			},
			crate::command::run::InnerOutput::Path(path) => {
				println!("{}", path.display());
			},
			crate::command::run::InnerOutput::Value(value) => {
				if !value.is_null() {
					let stdout = std::io::stdout();
					let value = if stdout.is_terminal() {
						let options = tg::value::print::Options {
							recursive: false,
							style: tg::value::print::Style::Pretty { indentation: "  " },
						};
						value.print(options)
					} else {
						value.to_string()
					};
					println!("{value}");
				}
			},
		}

		Ok(())
	}

	pub(crate) async fn command_run_inner(
		&self,
		reference: tg::Reference,
		kind: InnerKind,
		args: InnerArgs,
	) -> tg::Result<InnerOutput> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
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

			// Get the command.
			let command = reference
				.uri()
				.fragment()
				.map_or("default", |fragment| fragment);

			// Get the args.
			let mut args_: Vec<tg::Value> = args
				.arg
				.into_iter()
				.map(|arg| arg.parse())
				.try_collect::<tg::Value, _, _>()?;
			args_.insert(0, command.into());

			// Get the env.
			let mut env: tg::value::Map = args
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
				let host = if let Some(host) = args.host {
					host
				} else {
					tg::host().to_owned()
				};
				env.insert("TANGRAM_HOST".to_owned(), host.to_string().into());
			}

			// Choose the host.
			let host = "js";

			// Choose the cwd and network.
			let (cwd, sandbox) = match kind {
				InnerKind::Build => (None, Some(tg::command::Sandbox::default())),
				InnerKind::Run => {
					let cwd = std::env::current_dir().map_err(|source| {
						tg::error!(!source, "failed to get the working directory")
					})?;
					(Some(cwd), None)
				},
			};

			// Create the command.
			tg::command::Builder::new(host)
				.cwd(cwd)
				.executable(Some(executable))
				.args(args_)
				.env(env)
				.sandbox(sandbox)
				.build()
		};

		// Determine the retry.
		let retry = args.retry;

		// Print the command.
		eprintln!(
			"{} command {}",
			"info".blue().bold(),
			command.id(&handle).await?
		);

		// If the remote is set, then push the commnad.
		if let Some(remote) = remote.clone() {
			let id = command.id(&handle).await?;
			let arg = tg::object::push::Arg { remote };
			let stream = handle.push_object(&id.into(), arg).await?;
			self.render_progress_stream(stream).await?;
		}

		// Spawn the process.
		let id = command.id(&handle).await?;
		let arg = tg::command::spawn::Arg {
			create: args.create,
			parent: None,
			remote: remote.clone(),
			retry,
		};
		let output = handle.spawn_command(&id, arg).await?;
		let process = tg::Process::with_id(output.process);

		// Tag the process if requested.
		if let Some(tag) = args.tag {
			let item = Either::Left(process.id().clone());
			let arg = tg::tag::put::Arg {
				force: false,
				item,
				remote: remote.clone(),
			};
			handle.put_tag(&tag, arg).await?;
		}

		// If the detach flag is set, then return the process.
		if args.detach {
			return Ok(InnerOutput::Detached(process.id().clone()));
		}

		// Print the process.
		eprintln!("{} process {}", "info".blue().bold(), process.id());

		// Get the process's status.
		let status = process
			.status(&handle)
			.await?
			.try_next()
			.await?
			.ok_or_else(|| tg::error!("failed to get the status"))?;

		// If the process is finished, then get the process's output.
		let output = if status.is_finished() {
			let output = process
				.output(&handle)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the output"))?;
			Some(output)
		} else {
			None
		};

		// If the process is not finished, then wait for it to finish while showing the viewer if enabled.
		let result = if let Some(output) = output {
			Ok(output)
		} else {
			// Spawn the view task.
			let view_task = {
				let handle = handle.clone();
				let process = process.clone();
				let task = Task::spawn_blocking(move |stop| {
					let local_set = tokio::task::LocalSet::new();
					let runtime = tokio::runtime::Builder::new_current_thread()
						.worker_threads(1)
						.enable_all()
						.build()
						.unwrap();
					local_set
						.block_on(&runtime, async move {
							let options = crate::viewer::Options {
								condensed_processes: true,
								expand_on_create: true,
							};
							let item = crate::viewer::Item::Process(process);
							let mut viewer = crate::viewer::Viewer::new(&handle, item, options);

							match args.view {
								View::None => (),
								View::Inline => {
									viewer.run_inline(stop).await?;
								},
								View::Fullscreen => {
									viewer.run_fullscreen(stop).await?;
								},
							}
							Ok::<_, tg::Error>(())
						})
						.unwrap();
				});
				Some(task)
			};

			// Spawn a task to attempt to cancel the process on the first interrupt signal and exit the process on the second.
			let cancel_task = tokio::spawn({
				let handle = handle.clone();
				let process = process.clone();
				async move {
					tokio::signal::ctrl_c().await.unwrap();
					tokio::spawn(async move {
						let arg = tg::process::finish::Arg {
							error: Some(tg::error!("the process was explicitly canceled")),
							exit: None,
							output: None,
							remote,
							status: tg::process::Status::Canceled,
						};
						process
							.finish(&handle, arg)
							.await
							.inspect_err(|error| {
								tracing::error!(?error, "failed to cancel the process");
							})
							.ok();
					});
					tokio::signal::ctrl_c().await.unwrap();
					std::process::exit(130);
				}
			});

			// Wait for the process's output.
			let result = process.output(&handle).await;

			// Abort the cancel task.
			cancel_task.abort();

			// Stop and await the view task.
			if let Some(view_task) = view_task {
				view_task.stop();
				view_task.wait().await.unwrap();
			}

			result
		};

		let output = result?;

		// Check out the output if requested.
		if let Some(path) = args.checkout {
			// Get the artifact.
			let artifact = tg::Artifact::try_from(output)
				.map_err(|source| tg::error!(!source, "expected the output to be an artifact"))?;

			// Get the path.
			let path = if let Some(path) = path {
				let path = std::path::absolute(path)
					.map_err(|source| tg::error!(!source, "failed to get the path"))?;
				Some(path)
			} else {
				None
			};

			// Check out the artifact.
			let arg = tg::artifact::checkout::Arg {
				dependencies: path.is_some(),
				force: false,
				lockfile: false,
				path,
			};
			let stream = handle
				.check_out_artifact(&artifact.id(&handle).await?, arg)
				.await?;
			let output = self
				.render_progress_stream(stream)
				.await
				.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;

			return Ok(InnerOutput::Path(output.path));
		}

		Ok(InnerOutput::Value(output))
	}
}

impl Default for InnerArgs {
	fn default() -> Self {
		Self {
			arg: vec![],
			checkout: None,
			create: true,
			detach: false,
			env: vec![],
			host: None,
			locked: false,
			remote: None,
			retry: false,
			tag: None,
			view: View::default(),
		}
	}
}
