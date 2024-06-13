use crate::Cli;
use tangram_client as tg;

/// Get the server's health.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_server_health(&self, _args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let health = client.health().await?;
		let health = serde_json::to_string_pretty(&health)
			.map_err(|source| tg::error!(!source, "failed to serialize"))?;
		println!("{health}");
		Ok(())
	}
}
