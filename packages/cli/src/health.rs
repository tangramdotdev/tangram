use {crate::Cli, tangram_client::prelude::*};

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
		let output = handle
			.health()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the health"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
