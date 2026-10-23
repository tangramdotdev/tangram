use {crate::Component, tangram_client::prelude::*};

/// Create an organization.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub output: tangram_cli_core::print::OutputOptions,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(index = 1)]
	pub specifier: tg::Specifier,
}

impl Component<'_> {
	pub async fn command_organization_create(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::organization::create::Arg {
			location: args.location.get(),
			specifier: args.specifier.clone(),
		};
		let output = client.create_organization(arg).await.map_err(
			|error| tg::error!(!error, specifier = %args.specifier, "failed to create the organization"),
		)?;
		if args.output.verbose {
			self.print_serde(output, args.print).await?;
		} else {
			self.print_serde(output.data, args.print).await?;
		}
		Ok(())
	}
}
