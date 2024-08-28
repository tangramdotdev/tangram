use crate::Cli;
use tangram_client::{self as tg, Handle as _};

/// Pull a build.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub build: tg::build::Id,

	#[arg(long)]
	pub logs: bool,

	#[arg(long)]
	pub recursive: bool,

	#[arg(short, long)]
	pub remote: Option<String>,

	#[arg(long)]
	pub targets: bool,
}

impl Cli {
	pub async fn command_build_pull(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());

		// Pull the build.
		let arg = tg::build::pull::Arg {
			logs: args.logs,
			outcomes: true,
			recursive: args.recursive,
			remote,
			targets: args.targets,
		};
		let stream = handle.pull_build(&args.build, arg).await?;
		self.drain_progress_stream(stream).await?;
		Ok(())
	}
}
