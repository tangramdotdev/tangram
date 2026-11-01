use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get a runner.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(index = 1)]
	pub runner: tg::Referent<tg::runner::Id>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::runner::get::Arg {
		location: args.location.get_for_options(&args.runner),
	};
	let runner = args.runner.node;
	let output = client
		.try_get_runner(&runner, arg)
		.await
		.map_err(|error| tg::error!(!error, %runner, "failed to get the runner"))?
		.ok_or_else(|| tg::error!(%runner, "failed to find the runner"))?;
	let tg::runner::get::Output { data, location } = output;
	cli.print_location_and_tokens(location.as_ref(), &tg::authorization::Tokens::default())?;
	cli.print_serde(data, args.print).await?;

	Ok(())
}
