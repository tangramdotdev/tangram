use crate::Cli;
use tangram_client as tg;

/// Start the server.
#[derive(Debug, clap::Args)]
pub struct Args {}

impl Cli {
	pub async fn command_server_start(&self, _args: Args) -> tg::Result<()> {
		Self::start_server().await?;
		Ok(())
	}
}
