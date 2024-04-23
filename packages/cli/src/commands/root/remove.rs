use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Remove a root.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub name: String,
}

impl Cli {
	pub async fn command_root_remove(&self, args: Args) -> tg::Result<()> {
		self.handle.remove_root(&args.name).await?;
		Ok(())
	}
}
