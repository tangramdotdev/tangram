use crate::Cli;
use tangram_client as tg;

/// Stop the server.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_server_stop(&self, _args: Args) -> tg::Result<()> {
		Self::stop_server(self.config.as_ref()).await?;
		Ok(())
	}
}
