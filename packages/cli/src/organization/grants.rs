use {crate::Cli, tangram_client::prelude::*};

/// List grants for an organization.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(index = 1)]
	pub organization: tg::organization::Selector,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_organization_grants(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::organization::grants::Arg {
			location: args.location.get(),
		};
		let output = client
			.try_get_organization_grants(&args.organization, arg)
			.await
			.map_err(
				|error| tg::error!(!error, organization = %args.organization, "failed to list the grants"),
			)?
			.ok_or_else(
				|| tg::error!(organization = %args.organization, "failed to find the organization"),
			)?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
