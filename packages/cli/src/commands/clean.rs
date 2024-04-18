use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Remove unused objects.
#[derive(Debug, clap::Args)]
pub struct Args {}

impl Cli {
	pub async fn command_clean(&self, _args: Args) -> tg::Result<()> {
		// Clean.
		self.handle.clean().await?;

		Ok(())
	}
}
