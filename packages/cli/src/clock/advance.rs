use {crate::Cli, std::time::Duration, tangram_client::prelude::*};

/// Advance the server's virtual clock.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The duration to advance the clock by.
	#[arg(index = 1, value_parser = humantime::parse_duration)]
	pub duration: Duration,
}

impl Cli {
	pub async fn command_clock_advance(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::clock::advance::Arg {
			duration: args.duration,
		};
		let output = client
			.advance_clock(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to advance the clock"))?;
		self.print_serde(output, crate::print::Options::default())
			.await?;
		Ok(())
	}
}
