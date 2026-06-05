use {crate::Cli, tangram_client::prelude::*};

/// Get an organization.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub organization: tg::organization::Selector,

	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_organization_get(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::organization::get::Arg {
			location: args.location.get(),
		};
		let organization = client
			.try_get_organization(&args.organization, arg)
			.await
			.map_err(
				|error| tg::error!(!error, organization = %args.organization, "failed to get the organization"),
			)?
			.ok_or_else(|| tg::error!("failed to find the organization"))?;
		self.print_serde(organization, args.print).await?;
		Ok(())
	}
}
