use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Set the time of the server's virtual clock.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The time as an RFC 3339 timestamp.
	#[arg(index = 1)]
	pub now: jiff::Timestamp,
}

pub async fn command_clock_set(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::clock::set::Arg { now: args.now };
	let output = client
		.set_clock(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to set the clock"))?;
	cli.print_serde(output, tangram_cli_core::print::Options::default())
		.await?;
	Ok(())
}
