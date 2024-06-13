use crate::Cli;
use tangram_client as tg;

/// Remove unused objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_server_clean(&self, _args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		client.clean().await?;
		Ok(())
	}
}
