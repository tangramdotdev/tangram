use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Get the server's health.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_server_status(&self, _args: Args) -> tg::Result<()> {
		if self.handle.health().await.is_ok() {
			println!("started");
		} else {
			println!("stopped");
		}
		Ok(())
	}
}
