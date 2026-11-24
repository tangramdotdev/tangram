use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Advance the server's virtual clock.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// The duration to advance the clock by.
	pub duration: humantime::Duration,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::clock::advance::Arg {
		duration: args.duration.into(),
	};
	let output = client
		.advance_clock(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to advance the clock"))?;
	cli.print_serde(output, tangram_cli_core::print::Options::default())
		.await?;
	Ok(())
}
