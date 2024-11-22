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

/// Build a target.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Set the arguments.
	#[arg(short, long, num_args = 1.., action = clap::ArgAction::Append)]
	pub arg: Vec<Vec<String>>,

	/// Whether to check out the output. The output must be an artifact. A path to check out to may be provided.
	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub checkout: Option<Option<PathBuf>>,

	/// If this flag is set, then the command will exit immediately instead of waiting for the build to finish.
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

	/// Whether to suppress printing the tree.
	#[arg(short, long)]
	pub quiet: bool,

	/// The reference to the target to build.
	#[arg(index = 1)]
	pub reference: Option<tg::Reference>,

	/// Whether to build on a remote.
	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,

	/// The retry strategy to use.
	#[allow(clippy::option_option)]
	#[arg(long)]
	pub retry: Option<Option<tg::build::Retry>>,

	/// Create a tag for this build.
	#[arg(long)]
	pub tag: Option<tg::Tag>,
}

#[derive(Clone, Debug, derive_more::Unwrap)]
pub enum InnerOutput {
	Detached(tg::build::Id),
	Path(PathBuf),
	Value(tg::Value),
}

impl Cli {
	pub async fn command_target_build(&self, args: Args) -> tg::Result<()> {
		// Build.
		let output = self.command_target_build_inner(args).await?;

		// Print the output.
		match output {
			InnerOutput::Detached(build) => {
				println!("{build}");
			},
			InnerOutput::Path(path) => {
				println!("{}", path.display());
			},
			InnerOutput::Value(value) => {
				let stdout = std::io::stdout();
				let value = if stdout.is_terminal() {
					let options = tg::value::print::Options {
						recursive: false,
						style: tg::value::print::Style::Pretty { indentation: "\t" },
					};
					value.print(options)
				} else {
					value.to_string()
				};
				println!("{value}");
			},
		}

		Ok(())
	}

	pub(crate) async fn command_target_build_inner(&self, args: Args) -> tg::Result<InnerOutput> {
		let handle = self.handle().await?;

		// Get the reference.
		let reference = args
			.reference
			.clone()
			.unwrap_or_else(|| ".".parse().unwrap());

		// Get the remote.
		let remote = args
			.remote
			.clone()
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));

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

		// Create the target.
		let target = if let tg::Object::Target(target) = object {
			// If the object is a target, then use it.
			target
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
					if name.is_none() {
						tracing::info!("no root module found, performing init");
						self.command_package_init(crate::package::init::Args {
							path: Some(reference.as_str().into()),
						})
						.await?;
						return Box::pin(self.command_target_build_inner(args)).await;
					}
					let name = name.unwrap();
					let kind = if Path::new(name)
						.extension()
						.map_or(false, |extension| extension == "js")
					{
						tg::module::Kind::Js
					} else if Path::new(name)
						.extension()
						.map_or(false, |extension| extension == "ts")
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
					let module = tg::target::Module { kind, referent };
					tg::target::Executable::Module(module)
				},

				tg::Object::File(file) => tg::target::Executable::Artifact(file.into()),

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
			let mut args_: Vec<tg::Value> = args
				.arg
				.into_iter()
				.map(|arg| {
					arg.into_iter()
						.map(|arg| arg.parse())
						.collect::<Result<tg::value::Array, tg::Error>>()
						.map(Into::into)
				})
				.try_collect()?;
			args_.insert(0, target.into());

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

			// Create the target.
			tg::target::Builder::new(host)
				.executable(Some(executable))
				.args(args_)
				.env(env)
				.build()
		};

		// Determine the retry.
		let retry = match args.retry {
			None => tg::build::Retry::default(),
			Some(None) => tg::build::Retry::Succeeded,
			Some(Some(retry)) => retry,
		};

		// Print the target.
		eprintln!(
			"{} target {}",
			"info".blue().bold(),
			target.id(&handle).await?
		);

		// Build the target.
		let id = target.id(&handle).await?;
		let arg = tg::target::build::Arg {
			create: true,
			parent: None,
			remote: remote.clone(),
			retry,
		};
		let output = handle.build_target(&id, arg).await?;
		let build = tg::Build::with_id(output.build);

		// Tag the build if requested.
		if let Some(tag) = args.tag {
			let item = Either::Left(build.id().clone());
			let arg = tg::tag::put::Arg {
				force: false,
				item,
				remote: remote.clone(),
			};
			handle.put_tag(&tag, arg).await?;
		}

		// If the detach flag is set, then return the build.
		if args.detach {
			return Ok(InnerOutput::Detached(build.id().clone()));
		}

		// Print the build.
		eprintln!("{} build {}", "info".blue().bold(), build.id());

		// Get the build's status.
		let status = build
			.status(&handle)
			.await?
			.try_next()
			.await?
			.ok_or_else(|| tg::error!("failed to get the status"))?;

		// If the build is finished, then get the build's outcome.
		let outcome = if status == tg::build::Status::Finished {
			let outcome = build
				.outcome(&handle)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the outcome"))?;
			Some(outcome)
		} else {
			None
		};

		// If the build is not finished, then wait for it to finish while showing the TUI if enabled.
		let outcome = if let Some(outcome) = outcome {
			outcome
		} else {
			// Spawn the tree task.
			let tree_task = (!args.quiet).then(|| {
				let handle = handle.clone();
				let build = build.clone();
				let options = crate::view::tree::Options {
					depth: None,
					objects: false,
					builds: true,
					collapse_builds_on_success: true,
				};
				tokio::spawn(Self::tree_inner(handle, Either::Left(build), options))
			});

			// Spawn a task to attempt to cancel the build on the first interrupt signal and exit the process on the second.
			let cancel_task = tokio::spawn({
				let handle = handle.clone();
				let build = build.clone();
				async move {
					tokio::signal::ctrl_c().await.unwrap();
					tokio::spawn(async move {
						let outcome = tg::build::outcome::Data::Canceled;
						let arg = tg::build::finish::Arg { outcome, remote };
						build.finish(&handle, arg).await.ok();
					});
					tokio::signal::ctrl_c().await.unwrap();
					std::process::exit(130);
				}
			});

			// Wait for the build's outcome.
			let outcome = build.outcome(&handle).await;

			// Abort the cancel task.
			cancel_task.abort();

			// Wait for the tree task to finish.
			if let Some(tree_task) = tree_task {
				tree_task.await.unwrap()?;
			}

			outcome.map_err(|source| tg::error!(!source, "failed to get the build outcome"))?
		};

		// Handle a failed build.
		let output = outcome
			.into_result()
			.map_err(|source| tg::error!(!source, "the build failed"))?;

		// Check out the output if requested.
		if let Some(path) = args.checkout {
			// Get the artifact.
			let artifact = tg::Artifact::try_from(output.clone())
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
			let dependencies = path.is_some();
			let arg = tg::artifact::checkout::Arg {
				force: false,
				dependencies,
				path,
			};
			let output = artifact
				.check_out(&handle, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;

			return Ok(InnerOutput::Path(output));
		}

		Ok(InnerOutput::Value(output))
	}
}
