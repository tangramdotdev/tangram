use crate::{
	tree::Tree,
	tui::{self, Tui},
	Cli,
};
use async_recursion::async_recursion;
use crossterm::style::Stylize;
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use itertools::Itertools;
use std::{collections::BTreeMap, fmt::Write, path::PathBuf};
use tangram_client as tg;
use tangram_error::{error, Error, Result};
use tg::Handle;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Build a target and manage builds.
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
	Get(GetArgs),
	Put(PutArgs),
	Push(PushArgs),
	Pull(PullArgs),
	Tree(TreeArgs),
}

/// Build a target.
#[derive(Debug, clap::Args)]
pub struct GetOrCreateArgs {
	/// Set the arguments.
	#[clap(short, long, action = clap::ArgAction::Append)]
	pub arg: Vec<String>,

	/// If this flag is set, then the command will exit immediately instead of waiting for the build to finish.
	#[clap(short, long, conflicts_with = "output")]
	pub detach: bool,

	/// Set the environment variables.
	#[clap(short, long, action = clap::ArgAction::Append)]
	pub env: Vec<String>,

	/// Set the host.
	#[clap(long)]
	pub host: Option<tg::Triple>,

	/// If this flag is set, the package's lockfile will not be updated.
	#[clap(long)]
	pub locked: bool,

	/// Disable the TUI.
	#[clap(long, default_value = "false")]
	pub no_tui: bool,

	/// The path to check out the output to.
	#[clap(short, long)]
	pub output: Option<PathBuf>,

	/// The package to build.
	#[clap(short, long)]
	pub package: Option<tg::Dependency>,

	/// The retry strategy to use.
	#[clap(long, default_value_t)]
	pub retry: tg::build::Retry,

	/// The name of the target to build.
	#[clap(short, long)]
	pub target: Option<String>,

	/// The ID of an existing target to build.
	#[clap(long, conflicts_with_all = &["target", "package"])]
	pub target_id: Option<tg::target::Id>,
}

/// Get a build.
#[derive(Debug, clap::Args)]
pub struct GetArgs {
	pub id: tg::build::Id,
}

/// Put a build.
#[derive(Debug, clap::Args)]
pub struct PutArgs {
	#[clap(long)]
	pub json: Option<String>,
}

/// Push a build.
#[derive(Debug, clap::Args)]
pub struct PushArgs {
	pub id: tg::build::Id,
}

/// Pull a build.
#[derive(Debug, clap::Args)]
pub struct PullArgs {
	pub id: tg::build::Id,
}

/// Display the build tree.
#[derive(Debug, clap::Args)]
pub struct TreeArgs {
	pub id: tg::build::Id,
	#[clap(long)]
	pub depth: Option<u32>,
}

impl Cli {
	pub async fn command_build(&self, args: Args) -> Result<()> {
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

	pub async fn command_build_get_or_create(&self, args: GetOrCreateArgs) -> Result<()> {
		let client = &self.client().await?;

		let target = if let Some(id) = args.target_id {
			tg::Target::with_id(id)
		} else {
			let mut package = args.package.unwrap_or(".".parse().unwrap());
			let target = args.target.unwrap_or("default".parse().unwrap());

			// Canonicalize the path.
			if let Some(path) = package.path.as_mut() {
				*path = tokio::fs::canonicalize(&path)
					.await
					.map_err(|error| error!(source = error, "failed to canonicalize the path"))?
					.try_into()?;
			}

			// Create the package.
			let (package, lock) = tg::package::get_with_lock(client, &package).await?;

			// Create the target.
			let mut env: BTreeMap<String, tg::Value> = args
				.env
				.into_iter()
				.map(|env| {
					let (key, value) = env
						.split_once('=')
						.ok_or_else(|| error!("expected `KEY=value`"))?;
					Ok::<_, Error>((key.to_owned(), tg::Value::String(value.to_owned())))
				})
				.try_collect()?;
			if !env.contains_key("TANGRAM_HOST") {
				let host = if let Some(host) = args.host {
					host
				} else {
					tg::Triple::host()?
				};
				env.insert("TANGRAM_HOST".to_owned(), host.to_string().into());
			}
			let args_: BTreeMap<String, tg::Value> = args
				.arg
				.into_iter()
				.map(|arg| {
					let (key, value) = arg
						.split_once('=')
						.ok_or_else(|| error!("expected `key=value`"))?;
					Ok::<_, Error>((key.to_owned(), tg::Value::String(value.to_owned())))
				})
				.try_collect()?;
			let args_ = vec![args_.into()];
			let host = tg::Triple::js();
			let path = tg::package::get_root_module_path(client, &package).await?;
			let executable = tg::Symlink::new(Some(package.into()), Some(path.to_string())).into();
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
			target.id(client).await?
		);

		// Build the target.
		let arg = tg::build::GetOrCreateArg {
			parent: None,
			remote: false,
			retry: args.retry,
			target: target.id(client).await?.clone(),
		};
		let build = tg::Build::new(client, arg).await?;

		// If the detach flag is set, then exit.
		if args.detach {
			println!("{}", build.id());
			return Ok(());
		}

		// Print the build.
		eprintln!("{}: build {}", "info".blue().bold(), build.id());

		// Attempt to get the build's outcome with zero timeout.
		let arg = tg::build::outcome::GetArg {
			timeout: Some(std::time::Duration::ZERO),
		};
		let outcome = build
			.get_outcome(client, arg)
			.await
			.map_err(|error| error!(source = error, "failed to get the build outcome"))?;

		// If the outcome is not immediatey available, then wait for it while showing the TUI if enabled.
		let outcome = if let Some(outcome) = outcome {
			outcome
		} else {
			// Create the TUI.
			let tui = !args.no_tui;
			let tui = if tui {
				Tui::start(client, &build, tui::Options::default())
					.await
					.ok()
			} else {
				None
			};

			// Wait for the build's outcome.
			let outcome = build.outcome(client).await;

			// Stop the TUI.
			if let Some(tui) = tui {
				tui.stop();
				tui.join().await?;
			}

			outcome.map_err(|error| error!(source = error, "failed to get the build outcome"))?
		};

		// Handle a failed build.
		let output = outcome
			.into_result()
			.map_err(|error| error!(source = error, "the build failed"))?;

		// Check out the output if requested.
		if let Some(path) = args.output {
			let artifact = tg::Artifact::try_from(output.clone())
				.map_err(|error| error!(source = error, "expected the output to be an artifact"))?;
			artifact
				.check_out(client, Some(&path.try_into()?))
				.await
				.map_err(|error| error!(source = error, "failed to check out the artifact"))?;
		}

		// Print the output.
		println!("{output}");

		Ok(())
	}

	pub async fn command_build_get(&self, args: GetArgs) -> Result<()> {
		let client = &self.client().await?;
		let arg = tg::build::GetArg::default();
		let output = client.get_build(&args.id, arg).await?;
		let json = serde_json::to_string(&output)
			.map_err(|error| error!(source = error, "failed to serialize the output"))?;
		tokio::io::stdout()
			.write_all(json.as_bytes())
			.await
			.map_err(|error| error!(source = error, "failed to write the data"))?;
		Ok(())
	}

	pub async fn command_build_put(&self, args: PutArgs) -> Result<()> {
		let client = &self.client().await?;
		let json = if let Some(json) = args.json {
			json
		} else {
			let mut json = String::new();
			tokio::io::stdin()
				.read_to_string(&mut json)
				.await
				.map_err(|error| error!(source = error, "failed to read stdin"))?;
			json
		};
		let arg: tg::build::PutArg = serde_json::from_str(&json)
			.map_err(|error| error!(source = error, "failed to deseralize"))?;
		client.put_build(None, &arg.id, &arg).await?;
		Ok(())
	}

	pub async fn command_build_push(&self, args: PushArgs) -> Result<()> {
		let client = &self.client().await?;
		client.push_build(None, &args.id).await?;
		Ok(())
	}

	pub async fn command_build_pull(&self, args: PullArgs) -> Result<()> {
		let client = &self.client().await?;
		client.pull_build(&args.id).await?;
		Ok(())
	}

	pub async fn command_build_tree(&self, args: TreeArgs) -> Result<()> {
		let client = &self.client().await?;
		let build = tg::Build::with_id(args.id);
		let tree = get_build_tree(client, &build, 1, args.depth).await?;
		tree.print();
		Ok(())
	}
}

#[async_recursion]
async fn get_build_tree(
	tg: &dyn tg::Handle,
	build: &tg::Build,
	current_depth: u32,
	max_depth: Option<u32>,
) -> Result<Tree> {
	// Get the build's metadata.
	let id = build.id().clone();
	let status = build
		.status(tg, tg::build::status::GetArg::default())
		.await
		.map_err(|error| error!(source = error, %id, "failed to get the build's status"))?
		.next()
		.await
		.unwrap()
		.map_err(|error| error!(source = error, %id, "failed to get the build's status"))?;
	let target = build
		.target(tg)
		.await
		.map_err(|error| error!(source = error, %id, "failed to get build's target"))?;
	let package = target
		.package(tg)
		.await
		.map_err(|error| error!(source = error, %target, "failed to get target's package"))?;
	let name = target
		.name(tg)
		.await
		.map_err(|error| error!(source = error, %target, "failed to get target's name"))?
		.clone()
		.unwrap_or_else(|| "<unknown>".into());

	// Render the title
	let mut title = String::new();
	match status {
		tg::build::Status::Created | tg::build::Status::Queued => {
			write!(title, "{}", "⟳".yellow()).unwrap();
		},
		tg::build::Status::Started => write!(title, "{}", "⠿".blue()).unwrap(),
		tg::build::Status::Finished => {
			let outcome = build
				.outcome(tg)
				.await
				.map_err(|error| error!(source = error, %id, "failed to get the build outcome"))?;
			match outcome {
				tg::build::Outcome::Canceled => {
					write!(title, "{}", "⦻ ".yellow()).unwrap();
				},
				tg::build::Outcome::Succeeded(_) => {
					write!(title, "{}", "✓ ".green()).unwrap();
				},
				tg::build::Outcome::Failed(_) => {
					write!(title, "{}", "✗ ".red()).unwrap();
				},
			}
		},
	}
	write!(title, "{} ", id.to_string().blue()).unwrap();
	if let Some(package) = package {
		if let Ok(metadata) = tg::package::get_metadata(tg, package).await {
			if let Some(name) = metadata.name {
				write!(title, "{}", name.magenta()).unwrap();
			} else {
				write!(title, "{}", "<unknown>".blue()).unwrap();
			}
			if let Some(version) = metadata.version {
				write!(title, "@{}", version.yellow()).unwrap();
			}
		} else {
			write!(title, "{}", "<unknown>".magenta()).unwrap();
		}
		write!(title, ":").unwrap();
	}
	write!(title, "{}", name.white()).unwrap();

	// Get the build's children.
	let children = if max_depth.map_or(true, |max_depth| current_depth < max_depth) {
		let arg = tg::build::children::GetArg {
			position: Some(std::io::SeekFrom::Start(0)),
			timeout: Some(std::time::Duration::ZERO),
			..Default::default()
		};
		build
			.children(tg, arg)
			.await
			.map_err(|error| error!(source = error, %id, "failed to get the build's children"))?
			.map(
				|child| async move { get_build_tree(tg, &child?, current_depth + 1, max_depth).await },
			)
			.collect::<FuturesUnordered<_>>()
			.await
			.try_collect::<Vec<_>>()
			.await?
	} else {
		Vec::new()
	};

	let tree = Tree { title, children };

	Ok(tree)
}
