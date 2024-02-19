use crate::{
	tui::{self, Tui},
	Cli,
};
use std::path::PathBuf;
use tangram_client as tg;
use tangram_error::{Result, WrapErr};
use tg::Handle;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Manage builds.
#[derive(Debug, clap::Args)]
#[command(args_conflicts_with_subcommands = true)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[clap(flatten)]
	pub args: NewArgs,
	#[command(subcommand)]
	pub command: Option<Command>,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
	/// Get or create a build.
	New(NewArgs),

	/// Get a build.
	Get(GetArgs),

	/// Put a build.
	Put(PutArgs),

	/// Push a build.
	Push(PushArgs),

	/// Pull a build.
	Pull(PullArgs),
}

/// Build a target.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct NewArgs {
	/// If this flag is set, then the command will exit immediately instead of waiting for the build's output.
	#[arg(short, long, conflicts_with = "output")]
	pub detach: bool,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	/// Disable the TUI.
	#[arg(long, default_value = "false")]
	pub no_tui: bool,

	/// The path to check out the output to.
	#[arg(short, long)]
	pub output: Option<PathBuf>,

	/// The package to build.
	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,

	/// The retry strategy to use.
	#[arg(long, default_value_t)]
	pub retry: tg::build::Retry,

	/// The name of the target to build.
	#[arg(short, long, default_value = "default")]
	pub target: String,
}

#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct GetArgs {
	pub id: tg::build::Id,
}

#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct PutArgs {
	#[clap(long)]
	pub json: Option<String>,
}

#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct PushArgs {
	pub id: tg::build::Id,
}

#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct PullArgs {
	pub id: tg::build::Id,
}

impl Cli {
	pub async fn command_build(&self, args: Args) -> Result<()> {
		match args.command.unwrap_or(Command::New(args.args)) {
			Command::New(args) => {
				self.command_build_build(args).await?;
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
		}
		Ok(())
	}

	pub async fn command_build_build(&self, args: NewArgs) -> Result<()> {
		let client = &self.client().await?;

		// Canonicalize the path.
		let mut package = args.package;
		if let Some(path) = package.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.wrap_err("Failed to canonicalize the path.")?
				.try_into()?;
		}

		// Create the package.
		let (package, lock) = tg::package::get_with_lock(client, &package).await?;

		// Create the target.
		let env = [(
			"TANGRAM_HOST".to_owned(),
			tg::System::host()?.to_string().into(),
		)]
		.into();
		let args_ = Vec::new();
		let host = tg::System::js();
		let path = tg::package::get_root_module_path(client, &package).await?;
		let executable = tg::Symlink::new(Some(package.into()), Some(path.to_string())).into();
		let target = tg::target::Builder::new(host, executable)
			.lock(lock)
			.name(args.target.clone())
			.env(env)
			.args(args_)
			.build();

		// Print the target ID.
		eprintln!("{}", target.id(client).await?);

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

		// Print the build ID.
		eprintln!("{}", build.id());

		// Attempt to get the build's outcome with zero timeout.
		let arg = tg::build::outcome::GetArg {
			timeout: Some(std::time::Duration::ZERO),
		};
		let outcome = build
			.get_outcome(client, arg)
			.await
			.wrap_err("Failed to get the build outcome.")?;

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

			outcome.wrap_err("Failed to get the build outcome.")?
		};

		// Handle a failed build.
		let output = outcome.into_result().wrap_err("The build failed.")?;

		// Check out the output if requested.
		if let Some(path) = args.output {
			let artifact = tg::Artifact::try_from(output.clone())
				.wrap_err("Expected the output to be an artifact.")?;
			artifact
				.check_out(client, Some(&path.try_into()?))
				.await
				.wrap_err("Failed to check out the artifact.")?;
		}

		// Print the output.
		println!("{output}");

		Ok(())
	}

	pub async fn command_build_get(&self, args: GetArgs) -> Result<()> {
		let client = &self.client().await?;
		let output = client.get_build(&args.id).await?;
		let json = serde_json::to_string(&output).wrap_err("Failed to serialize the output.")?;
		tokio::io::stdout()
			.write_all(json.as_bytes())
			.await
			.wrap_err("Failed to write the data.")?;
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
				.wrap_err("Failed to read stdin.")?;
			json
		};
		let arg: tg::build::PutArg =
			serde_json::from_str(&json).wrap_err("Failed to deseralize.")?;
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
}
