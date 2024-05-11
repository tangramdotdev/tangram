use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Remove a root.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub name: String,
}

impl Cli {
	pub async fn command_root_remove(&self, args: Args) -> tg::Result<()> {
		self.handle.delete_root(&args.name).await?;
		Ok(())
	}
}
