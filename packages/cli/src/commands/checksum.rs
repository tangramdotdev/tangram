use crate::Cli;
use tangram_client as tg;
use tangram_error::{error, Result};

/// Compute a checksum.
#[derive(Debug, clap::Args)]
pub struct Args {
	/// The checksum algorithm to use.
	#[clap(short, long)]
	pub algorithm: tg::checksum::Algorithm,
}

impl Cli {
	pub async fn command_checksum(&self, _args: Args) -> Result<()> {
		Err(error!("this command is not yet implemented"))
	}
}
