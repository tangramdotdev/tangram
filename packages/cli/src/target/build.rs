use crate::{tui::Tui, Cli};
use crossterm::style::Stylize as _;
use either::Either;
use itertools::Itertools as _;
use std::path::PathBuf;
use tangram_client as tg;
use tg::Handle as _;

/// Build a target.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	inner: InnerArgs,

	/// If this flag is set, then the command will exit immediately instead of waiting for the build to finish.
	#[arg(short, long, conflicts_with = "checkout")]
	pub detach: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct InnerArgs {
	/// Set the arguments.
	#[arg(short, long, num_args = 1.., action = clap::ArgAction::Append)]
	pub arg: Vec<Vec<String>>,

	/// Whether to check out the output. The output must be an artifact. A path to check out to may be provided.
	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub checkout: Option<Option<PathBuf>>,

	/// Set the environment variables.
	#[arg(short, long, num_args = 1.., action = clap::ArgAction::Append)]
	pub env: Vec<Vec<String>>,

	/// Set the host.
	#[arg(long)]
	pub host: Option<String>,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	/// The package to build.
	#[arg(short, long)]
	pub package: Option<tg::Dependency>,

	/// Whether to build on a remote.
	#[arg(long, default_value_t)]
	pub remote: bool,

	/// The retry strategy to use.
	#[arg(long, default_value_t)]
	pub retry: tg::build::Retry,

	/// Create a root for this build. If a name is not provided, the package's name will be used.
	#[arg(long)]
	pub root: Option<String>,

	/// The name or ID of the target to build.
	#[arg(short, long)]
	pub target: Option<String>,

	/// Choose the view.
	#[arg(long, default_value = "tui")]
	pub view: View,
}

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum View {
	None,
	Tui,
}

pub enum InnerOutput {
	Path(tg::Path),
	Value(tg::Value),
}

impl Cli {
	pub async fn command_target_build(&self, args: Args) -> tg::Result<()> {
		// Build.
		let output = self
			.command_target_build_inner(args.inner, args.detach)
			.await?;

		// Handle the output.
		if let Some(output) = output {
			match output {
				InnerOutput::Path(path) => {
					println!("{path}");
				},
				InnerOutput::Value(value) => {
					println!("{value}");
				},
			}
		}

		Ok(())
	}

	pub(crate) async fn command_target_build_inner(
		&self,
		args: InnerArgs,
		detach: bool,
	) -> tg::Result<Option<InnerOutput>> {
		let target = if let Some(Ok(id)) = args.target.as_ref().map(|target| target.parse()) {
			tg::Target::with_id(id)
		} else {
			// Get the dependency.
			let mut dependency = args.package.unwrap_or(".".parse().unwrap());

			// Get the target.
			let target = args.target.unwrap_or("default".parse().unwrap());

			// Canonicalize the path.
			if let Some(path) = dependency.path.as_mut() {
				*path = tokio::fs::canonicalize(&path)
					.await
					.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
					.try_into()?;
			}

			// Create the package.
			let (package, lock) = tg::package::get_with_lock(&self.handle, &dependency).await?;

			// Create the target.
			let mut env: tg::value::Map = args
				.env
				.into_iter()
				.flatten()
				.map(|env| {
					let (key, value) = env
						.split_once('=')
						.ok_or_else(|| tg::error!("expected `key=value`"))?;
					Ok::<_, tg::Error>((key.to_owned(), value.to_owned().into()))
				})
				.try_collect()?;
			if !env.contains_key("TANGRAM_HOST") {
				let host = if let Some(host) = args.host {
					host
				} else {
					Cli::host().to_owned()
				};
				env.insert("TANGRAM_HOST".to_owned(), host.to_string().into());
			}
			let mut args_: Vec<tg::Value> = args
				.arg
				.into_iter()
				.map(|arg| {
					arg.into_iter()
						.map(|arg| {
							let (key, value) = arg
								.split_once('=')
								.ok_or_else(|| tg::error!("expected `key=value`"))?;
							Ok::<_, tg::Error>((key.to_owned(), value.to_owned().into()))
						})
						.collect::<Result<tg::value::Map, tg::Error>>()
						.map(Into::into)
				})
				.try_collect()?;
			args_.insert(0, target.into());
			let host = "js".to_owned();
			let path = tg::package::get_root_module_path(&self.handle, &package).await?;
			let executable = tg::Symlink::new(Some(package), Some(path)).into();
			tg::target::Builder::new(host, executable)
				.args(args_)
				.env(env)
				.lock(lock)
				.build()
		};

		// Print the target.
		eprintln!(
			"{}: target {}",
			"info".blue().bold(),
			target.id(&self.handle, None).await?
		);

		// Build the target.
		let id = target.id(&self.handle, None).await?;
		let arg = tg::target::build::Arg {
			parent: None,
			remote: args.remote,
			retry: args.retry,
		};
		let output = self.handle.build_target(&id, arg).await?;
		let build = tg::Build::with_id(output.build);

		// Add the root if requested.
		if let Some(name) = args.root {
			let build_or_object = Either::Left(build.id().clone());
			let arg = tg::root::put::Arg { build_or_object };
			self.handle.put_root(&name, arg).await?;
		}

		// If the detach flag is set, then print the build and exit.
		if detach {
			println!("{}", build.id());
			return Ok(None);
		}

		// Print the build.
		eprintln!("{}: build {}", "info".blue().bold(), build.id());

		// Attempt to get the build's outcome with zero timeout.
		let arg = tg::build::outcome::Arg {
			timeout: Some(std::time::Duration::ZERO),
		};
		let outcome = build
			.get_outcome(&self.handle, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the build outcome"))?;

		// If the outcome is not immediatey available, then wait for it while showing the TUI if enabled.
		let outcome = if let Some(outcome) = outcome {
			outcome
		} else {
			// Start the TUI.
			let tui = match args.view {
				View::Tui => Tui::start(&self.handle, &build).await.ok(),
				View::None => None,
			};

			// Spawn a task to cancel the build on the first interrupt signal and exit the process on the second.
			tokio::spawn({
				let handle = self.handle.clone();
				let build = build.clone();
				async move {
					tokio::signal::ctrl_c().await.unwrap();
					build.cancel(&handle).await.ok();
					tokio::signal::ctrl_c().await.unwrap();
					std::process::exit(130);
				}
			});

			// Wait for the build's outcome.
			let outcome = build.outcome(&self.handle).await;

			// Stop the TUI.
			if let Some(tui) = tui {
				tui.stop();
				tui.wait().await?;
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

			// If a path was provided, then ensure its parent directory exists and canonicalize it.
			let path = if let Some(path) = path {
				let current = std::env::current_dir()
					.map_err(|source| tg::error!(!source, "failed to get the working directory"))?;
				let path = current.join(&path);
				let parent = path
					.parent()
					.ok_or_else(|| tg::error!("the path must have a parent directory"))?;
				let file_name = path
					.file_name()
					.ok_or_else(|| tg::error!("the path must have a file name"))?;
				tokio::fs::create_dir_all(parent).await.map_err(|source| {
					tg::error!(!source, "failed to create the parent directory")
				})?;
				let path = parent
					.canonicalize()
					.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
					.join(file_name);
				Some(path.try_into()?)
			} else {
				None
			};

			// Check out the artifact.
			let arg = tg::artifact::checkout::Arg {
				bundle: path.is_some(),
				path,
				force: false,
			};
			let output = artifact
				.check_out(&self.handle, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;

			return Ok(Some(InnerOutput::Path(output.path)));
		}

		Ok(Some(InnerOutput::Value(output)))
	}
}
