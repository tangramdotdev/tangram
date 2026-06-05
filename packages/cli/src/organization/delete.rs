use {crate::Cli, tangram_client::prelude::*};

/// Delete an organization.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(index = 1)]
	pub organization: tg::organization::Selector,
}

impl Cli {
	pub async fn command_organization_delete(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::organization::delete::Arg {
			location: args.location.get(),
		};
		client.delete_organization(&args.organization, arg).await.map_err(
			|error| tg::error!(!error, organization = %args.organization, "failed to delete the organization"),
		)?;
		Ok(())
	}
}
