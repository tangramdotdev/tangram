use crate::Cli;
use tangram_client::{self as tg, handle::Ext as _};

/// Get a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub pretty: Option<bool>,

	#[arg(index = 1)]
	pub process: tg::process::Id,
}

impl Cli {
	pub async fn command_process_get(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let output = handle.get_process(&args.process).await?;
		Self::output_json(&output, args.pretty).await?;
		Ok(())
	}
}
