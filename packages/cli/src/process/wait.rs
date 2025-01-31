use crate::Cli;
use tangram_client::{self as tg, handle::Ext as _};

/// Wait for a process to finish.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub pretty: Option<bool>,

	#[arg(index = 1)]
	pub process: tg::process::Id,
}

impl Cli {
	pub async fn command_process_wait(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let output = handle.wait_process(&args.process).await?;
		Self::output_json(&output, args.pretty).await?;
		Ok(())
	}
}
