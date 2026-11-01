use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get a grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub grant: tg::Referent<tg::grant::Id>,

	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::grant::get::Arg {
		location: args.location.get_for_options(&args.grant),
	};
	let grant = args.grant.node;
	let output = client
		.try_get_grant(&grant, arg)
		.await
		.map_err(|error| tg::error!(!error, %grant, "failed to get the grant"))?
		.ok_or_else(|| tg::error!(%grant, "failed to find the grant"))?;
	let tg::grant::get::Output { data, location } = output;
	cli.print_location_and_tokens(location.as_ref(), &tg::authorization::Tokens::default())?;
	cli.print_serde(data, args.print).await?;

	Ok(())
}
