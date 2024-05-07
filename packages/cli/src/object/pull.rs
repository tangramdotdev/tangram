use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Pull an object.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub object: tg::object::Id,
}

impl Cli {
	pub async fn command_object_pull(&self, args: Args) -> tg::Result<()> {
		self.handle.pull_object(&args.object).await?;
		Ok(())
	}
}
