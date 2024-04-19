use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Push a build.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub id: tg::build::Id,
}

impl Cli {
	pub async fn command_build_push(&self, args: Args) -> tg::Result<()> {
		self.handle.push_build(&args.id).await?;
		Ok(())
	}
}
