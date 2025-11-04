use {crate::Cli, crossterm::style::Stylize as _, tangram_client as tg};

/// Stop the server.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_server_restart(&mut self, _args: Args) -> tg::Result<()> {
		if let Err(error) = self.stop_server().await {
			eprintln!("{}: failed to stop existing server. {error}", "warning".yellow());
		}
		self.start_server().await?;
		Ok(())
	}
}
