use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
};

/// Get the server's health.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_health(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let output = handle.health().await?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
