use crate::Cli;
use tangram_client::{self as tg, Handle as _};

/// Get the server's health.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_server_status(&mut self, _args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		if handle.health().await.is_ok() {
			println!("started");
		} else {
			println!("stopped");
		}
		Ok(())
	}
}
