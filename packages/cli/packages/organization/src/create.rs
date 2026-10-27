use {tangram_cli_core::Cli, tangram_client::prelude::*};

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

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::organization::create::Arg {
		location: args.location.get(),
		specifier: args.specifier.clone(),
	};
	let output = client.create_organization(arg).await.map_err(
		|error| tg::error!(!error, specifier = %args.specifier, "failed to create the organization"),
	)?;
	if args.output.verbose {
		cli.print_serde(output, args.print).await?;
	} else {
		cli.print_serde(output.data, args.print).await?;
	}
	Ok(())
}
