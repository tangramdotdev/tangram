use crate::Cli;
use tangram_client::{self as tg, Handle as _};

/// Index processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_index(&self, _args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		handle.index().await?;
		Ok(())
	}
}
