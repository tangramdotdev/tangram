use {crate::Cli, futures::StreamExt as _, std::time::Duration, tangram_client::prelude::*};

/// Get a process's status.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[command(flatten)]
	pub timeout: Timeout,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Timeout {
	#[arg(long, overrides_with = "no_timeout", value_parser = humantime::parse_duration)]
	pub timeout: Option<Duration>,

	#[arg(long, overrides_with = "timeout")]
	pub no_timeout: bool,
}

impl Timeout {
	fn get(&self) -> Option<Duration> {
		if self.no_timeout {
			None
		} else {
			self.timeout.or(Some(Duration::ZERO))
		}
	}
}

impl Cli {
	pub async fn command_process_status(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let locations = args.locations.get();
		let arg = tg::process::status::Arg {
			location: locations,
			timeout: args.timeout.get(),
		};
		let stream = client
			.get_process_status(&args.process, arg)
			.await
			.map_err(
				|source| tg::error!(!source, id = %args.process, "failed to get the process status"),
			)?;
		self.print_serde_stream(stream.boxed(), args.print).await?;
		Ok(())
	}
}
