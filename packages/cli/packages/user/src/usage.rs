use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get the current user's usage.
///
/// Usage includes all regions of the selected server unless a specific region is selected with --location.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub period: tangram_cli_usage::PeriodArgs,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let location = args.location.get();
	let arg = tg::usage::Arg {
		location,
		..args.period.into()
	};
	let usage = client
		.try_get_user_usage(None, arg)
		.await?
		.ok_or_else(|| tg::error!("failed to find the user"))?;
	cli.print_serde(usage, args.print).await?;

	Ok(())
}
