use {crate::Cli, tangram_client::prelude::*};

/// Wait for a process to finish.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_process_wait(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::process::wait::Arg {
			local: args.local.local,
			remotes: args.remotes.remotes,
		};
		let output = handle.wait_process(&args.process, arg).await?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
