use crate::Cli;
use tangram_client as tg;

/// Start the server.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_server_start(&self, _args: Args) -> tg::Result<()> {
		Self::start_server(&self.args, self.config.as_ref()).await?;
		Ok(())
	}
}
