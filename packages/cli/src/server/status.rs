use {crate::Cli, tangram_client as tg};

/// Get the server's health.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_server_status(&mut self, _args: Args) -> tg::Result<()> {
		if self.client().await.is_ok() {
			println!("started");
		} else {
			println!("stopped");
		}
		Ok(())
	}
}
