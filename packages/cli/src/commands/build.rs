use crate::{
	host,
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
	pub host: Option<String>,

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

		let target = if let Some(Ok(id)) = args.target.as_ref().map(|target| target.parse()) {
			tg::Target::with_id(id)
		} else {
			let mut dependency = args.package.unwrap_or(".".parse().unwrap());
			let target = args.target.unwrap_or("default".parse().unwrap());

			// Canonicalize the path.
			if let Some(path) = dependency.path.as_mut() {
				*path = tokio::fs::canonicalize(&path)
					.await
					.map_err(|source| error!(!source, "failed to canonicalize the path"))?
					.try_into()?;
			}

			// Create the package.
			let (package, lock) = tg::package::get_with_lock(client, &dependency).await?;

			// Write the lock file.
			if dependency.path.is_some() {
				let path = dependency.path.as_ref().unwrap();
				lock.write(client, path, false)
					.await
					.map_err(|source| error!(!source, "failed to write the lock file"))?;
			}

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
					host().to_owned()
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
			let host = "js".to_owned();
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
			.map_err(|source| error!(!source, "failed to get the build outcome"))?;

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

			outcome.map_err(|source| error!(!source, "failed to get the build outcome"))?
		};

		// Handle a failed build.
		let output = outcome
			.into_result()
			.map_err(|source| error!(!source, "the build failed"))?;

		// Check out the output if requested.
		if let Some(path) = args.output {
			// Get the artifact.
			let artifact = tg::Artifact::try_from(output.clone())
				.map_err(|source| error!(!source, "expected the output to be an artifact"))?;

			// Get the path.
			let current = std::env::current_dir()
				.map_err(|source| error!(!source, "failed to get the working directory"))?;
			let path = current.join(&path);
			let parent = path
				.parent()
				.ok_or_else(|| error!("the path must have a parent directory"))?;
			let file_name = path
				.file_name()
				.ok_or_else(|| error!("the path must have a file name"))?;
			tokio::fs::create_dir_all(parent)
				.await
				.map_err(|source| error!(!source, "failed to create the parent directory"))?;
			let path = parent
				.canonicalize()
				.map_err(|source| error!(!source, "failed to canonicalize the path"))?
				.join(file_name);
			let path = path.try_into()?;

			// Check out the artifact.
			let arg = tg::artifact::CheckOutArg {
				path: Some(path),
				force: false,
			};
			artifact
				.check_out(client, arg)
				.await
				.map_err(|source| error!(!source, "failed to check out the artifact"))?;
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
			.map_err(|source| error!(!source, "failed to serialize the output"))?;
		tokio::io::stdout()
			.write_all(json.as_bytes())
			.await
			.map_err(|source| error!(!source, "failed to write the data"))?;
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
				.map_err(|source| error!(!source, "failed to read stdin"))?;
			json
		};
		let arg: tg::build::PutArg = serde_json::from_str(&json)
			.map_err(|source| error!(!source, "failed to deseralize"))?;
		client.put_build(None, &arg.id, &arg).await?;
		println!("{}", arg.id);
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
	client: &tg::Client,
	build: &tg::Build,
	current_depth: u32,
	max_depth: Option<u32>,
) -> Result<Tree> {
	// Get the build's metadata.
	let id = build.id().clone();
	let status = build
		.status(client, tg::build::status::GetArg::default())
		.await
		.map_err(|source| error!(!source, %id, "failed to get the build's status"))?
		.next()
		.await
		.unwrap()
		.map_err(|source| error!(!source, %id, "failed to get the build's status"))?;
	let target = build
		.target(client)
		.await
		.map_err(|source| error!(!source, %id, "failed to get build's target"))?;
	let package = target
		.package(client)
		.await
		.map_err(|source| error!(!source, %target, "failed to get target's package"))?;
	let name = target
		.name(client)
		.await
		.map_err(|source| error!(!source, %target, "failed to get target's name"))?
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
				.outcome(client)
				.await
				.map_err(|source| error!(!source, %id, "failed to get the build outcome"))?;
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
		if let Ok(metadata) = tg::package::get_metadata(client, package).await {
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
			.children(client, arg)
			.await
			.map_err(|source| error!(!source, %id, "failed to get the build's children"))?
			.map(|child| async move {
				get_build_tree(client, &child?, current_depth + 1, max_depth).await
			})
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
