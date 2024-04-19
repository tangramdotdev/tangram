use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Stop the server.
#[derive(Debug, clap::Args)]
pub struct Args {}

impl Cli {
	pub async fn command_server_stop(&self, _args: Args) -> tg::Result<()> {
		self.handle.stop().await?;
		Ok(())
	}
}
