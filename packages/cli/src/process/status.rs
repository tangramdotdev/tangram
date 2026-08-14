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
	pub process: tg::Reference,

	#[command(flatten)]
	pub timeout: Timeout,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Timeout {
	#[arg(id = "status.timeout.timeout", long = "timeout", overrides_with = "status.timeout.no_timeout", value_parser = humantime::parse_duration)]
	pub timeout: Option<Duration>,

	#[arg(
		id = "status.timeout.no_timeout",
		long = "no-timeout",
		overrides_with = "status.timeout.timeout"
	)]
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
		let process = self
			.resolve_process_with_locations(&args.process, &args.locations)
			.await?;
		let id = process.node.clone();
		let process = tg::Process::<tg::Value>::with_referent(process);
		let options = tg::process::status::Options {
			timeout: args.timeout.get(),
		};
		let stream = process
			.status_with_handle(&client, options)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process status"))?;
		self.print_serde_stream(stream.boxed(), args.print).await?;
		Ok(())
	}
}
