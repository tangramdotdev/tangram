use {crate::Cli, tangram_client::prelude::*};

/// Get an organization's usage.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub organization: tg::organization::Selector,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_organization_usage(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let usage = client
			.try_get_organization_usage(&args.organization)
			.await?
			.ok_or_else(|| tg::error!("failed to find the organization"))?;
		self.print_serde(usage, args.print).await?;

		Ok(())
	}
}
