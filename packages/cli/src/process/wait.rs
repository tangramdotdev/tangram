use crate::Cli;
use std::pin::pin;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_futures::stream::Ext as _;

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
		let stream = handle.get_process_wait(&args.process).await?;
		let Some(tg::process::wait::Event::Output(output)) =
			pin!(stream).last().await.transpose()?
		else {
			return Err(tg::error!("failed to wait for the process"));
		};
		Self::output_json(&output, args.pretty).await?;
		Ok(())
	}
}
