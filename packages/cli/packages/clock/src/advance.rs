use {std::time::Duration, tangram_cli_core::Cli, tangram_client::prelude::*};

/// Advance the server's virtual clock.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The duration to advance the clock by.
	#[arg(index = 1, value_parser = humantime::parse_duration)]
	pub duration: Duration,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::clock::advance::Arg {
		duration: args.duration,
	};
	let output = client
		.advance_clock(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to advance the clock"))?;
	cli.print_serde(output, tangram_cli_core::print::Options::default())
		.await?;
	Ok(())
}
