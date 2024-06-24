use crate::Cli;
use tangram_client::{self as tg, Handle as _};

/// Remove unused objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_server_clean(&self, _args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		handle.clean().await?;
		Ok(())
	}
}
