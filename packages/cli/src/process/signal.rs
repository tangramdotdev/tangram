use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
};

/// Signal a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[arg(default_value = "INT", long, short)]
	pub signal: tg::process::Signal,

	#[expect(clippy::option_option)]
	#[arg(long, require_equals = true, short)]
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
