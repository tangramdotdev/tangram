use {crate::Cli, tangram_client::prelude::*};

/// List organization members.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub organization: tg::organization::Selector,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_organization_members_list(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let output = client
			.list_organization_members(&args.organization)
			.await
			.map_err(
				|error| tg::error!(!error, organization = %args.organization, "failed to list the organization members"),
			)?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
