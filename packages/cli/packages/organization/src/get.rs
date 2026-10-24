use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get an organization.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Only use cached remote results. Do not fetch from remotes.
	#[arg(long)]
	pub cached: bool,

	#[arg(index = 1)]
	pub organization: tg::Referent<tg::organization::Selector>,

	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[command(flatten)]
	pub ttl: tangram_cli_get::Ttl,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::organization::get::Arg {
		cached: args.cached,
		location: args.location.get_for_options(&args.organization),
		tokens: args.organization.options.tokens,
		ttl: args.ttl.get(),
	};
	let organization = client
		.try_get_organization(&args.organization.node, arg)
		.await
		.map_err(
			|error| tg::error!(!error, organization = %args.organization.node, "failed to get the organization"),
		)?
		.ok_or_else(|| tg::error!("failed to find the organization"))?;
	let tg::organization::get::Output {
		data,
		location,
		tokens,
	} = organization;
	cli.print_location_and_tokens(location.as_ref(), &tokens)?;
	cli.print_serde(data, args.print).await?;
	Ok(())
}
