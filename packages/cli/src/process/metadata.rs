use crate::Cli;
use tangram_client::{self as tg, handle::Ext as _};

/// Get process metadata.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[arg(long)]
	pub pretty: Option<bool>,
}

impl Cli {
	pub async fn command_process_metadata(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let metadata = handle.get_process_metadata(&args.process).await.map_err(
			|source| tg::error!(!source, %id = args.process, "failed to get the process metadata"),
		)?;
		Self::output_json(&metadata, args.pretty).await?;
		Ok(())
	}
}
