use crate::Cli;
use tangram_client::{self as tg, prelude::*};

/// Signal a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[arg(long, short, default_value = "INT")]
	pub signal: tg::process::Signal,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_process_signal(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Signal the process.
		let arg = tg::process::signal::post::Arg {
			remote,
			signal: args.signal,
		};
		handle.signal_process(&args.process, arg).await?;

		Ok(())
	}
}
