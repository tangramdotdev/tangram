use {crate::Cli, tangram_client as tg};

/// Stop the server.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_server_stop(&mut self, _args: Args) -> tg::Result<()> {
		self.stop_server().await?;
		Ok(())
	}
}
