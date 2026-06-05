use {crate::Cli, tangram_client::prelude::*};

/// Create an organization.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub specifier: tg::Specifier,
}

impl Cli {
	pub async fn command_organization_create(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::organization::create::Arg {
			location: args.location.get(),
			specifier: args.specifier.clone(),
		};
		let output = client.create_organization(arg).await.map_err(
			|error| tg::error!(!error, specifier = %args.specifier, "failed to create the organization"),
		)?;
		self.print_serde(output.organization, args.print).await?;
		Ok(())
	}
}
