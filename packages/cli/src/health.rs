use crate::Cli;
use tangram_client::{self as tg, prelude::*};

/// Get the server's health.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_health(&mut self, _args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let health = handle.health().await?;
		let health = serde_json::to_string_pretty(&health)
			.map_err(|source| tg::error!(!source, "failed to serialize"))?;
		println!("{health}");
		Ok(())
	}
}
