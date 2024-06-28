use crate::{
	tui::{self, Tui},
	Cli,
};
use crossterm::style::Stylize as _;
use either::Either;
use futures::TryStreamExt as _;
use itertools::Itertools as _;
use std::path::PathBuf;
use tangram_client as tg;

/// Build a target.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// If this flag is set, then the command will exit immediately instead of waiting for the build to finish.
	#[arg(short, long, conflicts_with = "checkout")]
	pub detach: bool,

	#[command(flatten)]
	pub inner: InnerArgs,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, clap::Args)]
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

	/// Whether to build on a remote.
	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,

	/// The retry strategy to use.
	#[allow(clippy::option_option)]
	#[arg(long)]
	pub retry: Option<Option<tg::build::Retry>>,

	/// Create a root for this build. If a name is not provided, the package's name will be used.
	#[arg(long)]
	pub root: Option<String>,

	/// The specifier of the target to build.
	#[arg(long, conflicts_with_all = ["target", "arg_"])]
	pub specifier: Option<Specifier>,

	/// The target to build.
	#[arg(long, conflicts_with_all = ["specifier", "arg_"])]
	pub target: Option<tg::target::Id>,

	/// Choose the view.
	#[arg(long, default_value = "tui")]
	pub view: View,

	/// The target to build.
	#[arg(conflicts_with_all = ["specifier", "target"])]
	pub arg_: Option<Arg>,
}

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum View {
	None,
	Tui,
}

#[derive(Clone, Debug)]
pub enum Arg {
	Specifier(Specifier),
	Target(tg::target::Id),
}

#[derive(Clone, Debug)]
pub struct Specifier {
	dependency: tg::Dependency,
	target: String,
}

#[derive(Clone, Debug, derive_more::Unwrap)]
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
		let client = self.client().await?;

		// Get the arg.
		let arg = if let Some(specifier) = args.specifier {
			Arg::Specifier(specifier)
		} else if let Some(target) = args.target {
			Arg::Target(target)
		} else {
			args.arg_.unwrap_or_default()
		};

		// Create the target.
		let target = match arg {
			Arg::Specifier(mut specifier) => {
				// Canonicalize the path.
				if let Some(path) = specifier.dependency.path.as_mut() {
					*path = tokio::fs::canonicalize(&path)
						.await
						.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
						.try_into()?;
				}

				// Create the package.
				let (package, lock) =
					tg::package::get_with_lock(&client, &specifier.dependency, args.locked).await?;

				// Create the target.
				let host = "js";
				let path = tg::package::get_root_module_path(&client, &package).await?;
				let executable = tg::Symlink::new(Some(package), Some(path));
				let mut args_: tg::value::Array = args
					.arg
					.into_iter()
					.map(|arg| {
						arg.into_iter()
							.map(|arg| arg.parse())
							.collect::<Result<tg::value::Array, tg::Error>>()
							.map(Into::into)
					})
					.try_collect()?;
				args_.insert(0, specifier.target.into());
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
				if !env.contains_key("TANGRAM_HOST") {
					let host = if let Some(host) = args.host {
						host
					} else {
						Cli::host().to_owned()
					};
					env.insert("TANGRAM_HOST".to_owned(), host.to_string().into());
				}
				tg::target::Builder::new(host)
					.executable(tg::Artifact::from(executable))
					.args(args_)
					.env(env)
					.lock(lock)
					.build()
			},

			Arg::Target(target) => tg::Target::with_id(target),
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
			target.id(&client).await?
		);

		// Build the target.
		let id = target.id(&client).await?;
		let remote = args
			.remote
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::target::build::Arg {
			create: true,
			parent: None,
			remote: remote.clone(),
			retry,
		};
		let output = client.build_target(&id, arg).await?;
		let build = tg::Build::with_id(output.build);

		// Add the root if requested.
		if let Some(name) = args.root {
			let item = Either::Left(build.id().clone());
			let arg = tg::root::put::Arg { item };
			client.put_root(&name, arg).await?;
		}

		// If the detach flag is set, then print the build and exit.
		if detach {
			println!("{}", build.id());
			return Ok(None);
		}

		// Print the build.
		eprintln!("{} build {}", "info".blue().bold(), build.id());

		// Get the build's status.
		let status = build
			.status(&client)
			.await?
			.try_next()
			.await?
			.ok_or_else(|| tg::error!("failed to get the status"))?;

		// If the build is finished, then get the build's outcome.
		let outcome = if status == tg::build::Status::Finished {
			let outcome = build
				.outcome(&client)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the outcome"))?;
			Some(outcome)
		} else {
			None
		};

		// If the outcome is not immediatey available, then wait for it while showing the TUI if enabled.
		let outcome = if let Some(outcome) = outcome {
			outcome
		} else {
			// Start the TUI.
			let tui = match args.view {
				View::Tui => {
					let item = tui::Item::Build {
						build: build.clone(),
						remote: remote.clone(),
					};
					Tui::start(&client, item).await.ok()
				},
				View::None => None,
			};

			// Spawn a task to attempt to cancel the build on the first interrupt signal and exit the process on the second.
			tokio::spawn({
				let handle = client.clone();
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
			let outcome = build.outcome(&client).await;

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
				force: false,
				path,
				references: true,
			};
			let output = artifact
				.check_out(&client, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;

			return Ok(Some(InnerOutput::Path(output)));
		}

		Ok(Some(InnerOutput::Value(output)))
	}
}

impl Default for InnerArgs {
	fn default() -> Self {
		Self {
			arg: vec![],
			checkout: None,
			env: vec![],
			host: None,
			locked: false,
			remote: None,
			retry: None,
			root: None,
			specifier: None,
			target: None,
			view: crate::target::build::View::Tui,
			arg_: None,
		}
	}
}

impl Default for Arg {
	fn default() -> Self {
		Self::Specifier(Specifier::default())
	}
}

impl std::str::FromStr for Arg {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(target) = s.parse() {
			return Ok(Arg::Target(target));
		}
		if let Ok(specifier) = s.parse() {
			return Ok(Arg::Specifier(specifier));
		}
		Err(tg::error!(%s, "expected a target specifier or target ID"))
	}
}

impl Default for Specifier {
	fn default() -> Self {
		Self {
			dependency: ".".parse().unwrap(),
			target: "default".to_owned(),
		}
	}
}

impl std::str::FromStr for Specifier {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		// Split the string by the colon character.
		let (package, target) = s.split_once(':').unwrap_or((s, ""));

		// Get the dependency.
		let dependency = if package.is_empty() {
			".".parse().unwrap()
		} else {
			package.parse()?
		};

		// Get the target.
		let target = if target.is_empty() {
			"default".to_owned()
		} else {
			target.to_owned()
		};

		Ok(Self { dependency, target })
	}
}
