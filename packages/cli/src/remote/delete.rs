use crate::Cli;
use tangram_client::{self as tg, Handle as _};

/// Delete a remote.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub name: String,
}

impl Cli {
	pub async fn command_remote_delete(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		handle.delete_remote(&args.name).await?;
		Ok(())
	}
}
