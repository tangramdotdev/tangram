use crate::Cli;
use tangram_client as tg;

/// Remove unused objects.
#[derive(Debug, clap::Args)]
pub struct Args {}

impl Cli {
	pub async fn command_clean(&self, _args: Args) -> tg::Result<()> {
		let client = &self.client().await?;

		// Clean.
		client.clean().await?;

		Ok(())
	}
}
