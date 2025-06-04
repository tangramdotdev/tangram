use crate::Cli;
use tangram_client::{self as tg, prelude::*};

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
	pub async fn command_process_get(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let tg::process::get::Output { data, .. } = handle.get_process(&args.process).await?;
		Self::print_json(&data, args.pretty).await?;
		Ok(())
	}
}
