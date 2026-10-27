use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Manage an organization's billing payment method.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[arg(index = 1)]
	pub organization: tg::Referent<tg::organization::Selector>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::organization::billing::manage::Arg {
		location: args.location.get_for_options(&args.organization),
		tokens: args.organization.options.tokens,
	};
	let output = client
		.manage_organization_billing(&args.organization.node, arg)
		.await
		.map_err(
			|error| tg::error!(!error, organization = %args.organization.node, "failed to manage the organization billing"),
		)?;
	cli.open_url(&output.url);

	Ok(())
}
