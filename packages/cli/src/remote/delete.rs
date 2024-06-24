use crate::Cli;
use tangram_client as tg;

/// Delete a remote.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub name: String,
}

impl Cli {
	pub async fn command_remote_delete(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		client.delete_remote(&args.name).await?;
		Ok(())
	}
}
