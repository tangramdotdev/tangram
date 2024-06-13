use crate::Cli;
use tangram_client as tg;

/// Delete a root.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub name: String,
}

impl Cli {
	pub async fn command_root_delete(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		client.delete_root(&args.name).await?;
		Ok(())
	}
}
