use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Pull a build.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_build_pull(&self, args: Args) -> tg::Result<()> {
		self.handle.pull_build(&args.build).await?;
		Ok(())
	}
}
