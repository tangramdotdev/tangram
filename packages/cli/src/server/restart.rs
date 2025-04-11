use crate::Cli;
use tangram_client as tg;

/// Stop the server.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_server_restart(&mut self, _args: Args) -> tg::Result<()> {
		self.stop_server().await?;
		self.start_server().await?;
		Ok(())
	}
}
