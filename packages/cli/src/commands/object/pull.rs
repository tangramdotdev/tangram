use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Pull an object.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub id: tg::object::Id,
}

impl Cli {
	pub async fn command_object_pull(&self, args: Args) -> tg::Result<()> {
		self.handle.pull_object(&args.id).await?;
		Ok(())
	}
}
