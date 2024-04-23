use crate::{tui::Tui, Cli};
use crossterm::style::Stylize;
use either::Either;
use itertools::Itertools as _;
use std::{collections::BTreeMap, path::PathBuf};
use tangram_client as tg;
use tg::Handle;

pub mod get;
pub mod pull;
pub mod push;
pub mod put;
pub mod tree;

/// Build a target or manage builds.
#[derive(Debug, clap::Args)]
#[clap(args_conflicts_with_subcommands = true)]
pub struct Args {
	#[clap(flatten)]
	pub args: GetOrCreateArgs,
	#[clap(subcommand)]
	pub command: Option<Command>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, clap::Subcommand)]
pub enum Command {
	#[clap(hide = true)]
	GetOrCreate(GetOrCreateArgs),
	Get(self::get::Args),
	Put(self::put::Args),
	Push(self::push::Args),
	Pull(self::pull::Args),
	Tree(self::tree::Args),
}

/// Build a target.
#[derive(Debug, clap::Args)]
pub struct GetOrCreateArgs {
	#[clap(flatten)]
	inner: GetOrCreateInnerArgs,

	/// If this flag is set, then the command will exit immediately instead of waiting for the build to finish.
	#[clap(short, long, conflicts_with = "checkout")]
	pub detach: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, clap::Args)]
pub struct GetOrCreateInnerArgs {
	/// Set the arguments.
	#[clap(short, long, num_args = 1.., action = clap::ArgAction::Append)]
	pub arg: Vec<String>,

	/// Whether to check out the output. The output must be an artifact. A path to may be provided.
	#[allow(clippy::option_option)]
	#[clap(short, long)]
	pub checkout: Option<Option<PathBuf>>,

	/// Set the environment variables.
	#[clap(short, long, num_args = 1.., action = clap::ArgAction::Append)]
	pub env: Vec<String>,

	/// Set the host.
	#[clap(long)]
	pub host: Option<String>,

	/// If this flag is set, the package's lockfile will not be updated.
	#[clap(long)]
	pub locked: bool,

	/// Disable the TUI.
	#[clap(long, default_value = "false")]
	pub no_tui: bool,

	/// The package to build.
	#[clap(short, long)]
	pub package: Option<tg::Dependency>,

	/// Whether to build on a remote.
	#[clap(long, default_value_t)]
	pub remote: bool,

	/// The retry strategy to use.
	#[clap(long, default_value_t)]
	pub retry: tg::build::Retry,

	/// Create a root for this build. If a name is not provided, the package's name will be used.
	#[clap(long)]
	pub root: Option<String>,

	/// The name or ID of the target to build.
	#[clap(short, long)]
	pub target: Option<String>,
}

pub enum GetOrCreateInnerOutput {
	Path(tg::Path),
	Value(tg::Value),
}

impl Cli {
	pub async fn command_build(&self, args: Args) -> tg::Result<()> {
		match args.command.unwrap_or(Command::GetOrCreate(args.args)) {
			Command::GetOrCreate(args) => {
				self.command_build_get_or_create(args).await?;
			},
			Command::Get(args) => {
				self.command_build_get(args).await?;
			},
			Command::Put(args) => {
				self.command_build_put(args).await?;
			},
			Command::Push(args) => {
				self.command_build_push(args).await?;
			},
			Command::Pull(args) => {
				self.command_build_pull(args).await?;
			},
			Command::Tree(args) => {
				self.command_build_tree(args).await?;
			},
		}
		Ok(())
	}

	pub async fn command_build_get_or_create(&self, args: GetOrCreateArgs) -> tg::Result<()> {
		// Build.
		let output = self
			.command_build_get_or_create_inner(args.inner, args.detach)
			.await?;

		// Handle the output.
		if let Some(output) = output {
			match output {
				GetOrCreateInnerOutput::Path(path) => {
					println!("{path}");
				},
				GetOrCreateInnerOutput::Value(value) => {
					println!("{value}");
				},
			}
		}

		Ok(())
	}

	pub async fn command_build_get_or_create_inner(
		&self,
		args: GetOrCreateInnerArgs,
		detach: bool,
	) -> tg::Result<Option<GetOrCreateInnerOutput>> {
		let target = if let Some(Ok(id)) = args.target.as_ref().map(|target| target.parse()) {
			tg::Target::with_id(id)
		} else {
			let mut dependency = args.package.unwrap_or(".".parse().unwrap());
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
			let mut env: BTreeMap<String, tg::Value> = args
				.env
				.into_iter()
				.map(|env| {
					let (key, value) = env
						.split_once('=')
						.ok_or_else(|| tg::error!("expected `key=value`"))?;
					Ok::<_, tg::Error>((key.to_owned(), tg::Value::String(value.to_owned())))
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
			let args_: BTreeMap<String, tg::Value> = args
				.arg
				.into_iter()
				.map(|arg| {
					let (key, value) = arg
						.split_once('=')
						.ok_or_else(|| tg::error!("expected `key=value`"))?;
					Ok::<_, tg::Error>((key.to_owned(), tg::Value::String(value.to_owned())))
				})
				.try_collect()?;
			let args_ = vec![args_.into()];
			let host = "js".to_owned();
			let path = tg::package::get_root_module_path(&self.handle, &package).await?;
			let executable = tg::Symlink::new(Some(package.into()), Some(path)).into();
			tg::target::Builder::new(host, executable)
				.lock(lock)
				.name(target.clone())
				.env(env)
				.args(args_)
				.build()
		};

		// Print the target.
		eprintln!(
			"{}: target {}",
			"info".blue().bold(),
			target.id(&self.handle, None).await?
		);

		// Create the build.
		let arg = tg::build::GetOrCreateArg {
			parent: None,
			remote: args.remote,
			retry: args.retry,
			target: target.id(&self.handle, None).await?,
		};
		let build = tg::Build::new(&self.handle, arg).await?;

		// Add the root if requested.
		if let Some(root) = args.root {
			let id = Either::Left(build.id().clone());
			let arg = tg::root::AddArg { name: root, id };
			self.handle.add_root(arg).await?;
		}

		// If the detach flag is set, then exit.
		if detach {
			println!("{}", build.id());
			return Ok(None);
		}

		// Print the build.
		eprintln!("{}: build {}", "info".blue().bold(), build.id());

		// Attempt to get the build's outcome with zero timeout.
		let arg = tg::build::outcome::GetArg {
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
			// Create the TUI.
			let tui = !args.no_tui;
			let tui = if tui {
				Tui::start(&self.handle, &build).await.ok()
			} else {
				None
			};

			// Wait for the build's outcome.
			let outcome = build.outcome(&self.handle).await;

			// Stop the TUI.
			if let Some(tui) = tui {
				tui.stop();
				tui.join().await?;
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
			let arg = tg::artifact::CheckOutArg { path, force: false };
			let output = artifact
				.check_out(&self.handle, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;

			return Ok(Some(GetOrCreateInnerOutput::Path(output.path)));
		}

		Ok(Some(GetOrCreateInnerOutput::Value(output)))
	}
}
