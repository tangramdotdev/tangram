use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Get the server's health.
#[derive(Debug, clap::Args)]
pub struct Args {}

impl Cli {
	pub async fn command_server_health(&self, _args: Args) -> tg::Result<()> {
		let health = self.handle.health().await?;
		let health = serde_json::to_string_pretty(&health)
			.map_err(|source| tg::error!(!source, "failed to serialize"))?;
		println!("{health}");
		Ok(())
	}
}
