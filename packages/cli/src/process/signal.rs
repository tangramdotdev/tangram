use {crate::Cli, tangram_client::prelude::*};

/// Signal a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,

	#[arg(default_value = "INT", long, short)]
	pub signal: tg::process::Signal,
}

impl Cli {
	pub async fn command_process_signal(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Signal the process.
		let arg = tg::process::signal::post::Arg {
			local: args.local.local,
			remotes: args.remotes.remotes,
			signal: args.signal,
		};
		handle.signal_process(&args.process, arg).await?;

		Ok(())
	}
}
