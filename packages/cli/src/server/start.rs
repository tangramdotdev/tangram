use {crate::Cli, tangram_client::prelude::*};

/// Start the server.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_server_start(&mut self, _args: Args) -> tg::Result<()> {
		self.start_server().await?;
		Ok(())
	}
}
