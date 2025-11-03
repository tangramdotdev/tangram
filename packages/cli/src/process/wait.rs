use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
};

/// Wait for a process to finish.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::process::Id,
}

impl Cli {
	pub async fn command_process_wait(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let output = handle.wait_process(&args.process).await?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
