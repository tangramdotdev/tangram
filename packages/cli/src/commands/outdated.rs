use crate::Cli;
use tangram_error::{return_error, Result};

/// Check for outdated dependencies.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {}

impl Cli {
	#[allow(clippy::unused_async)]
	pub async fn command_outdated(&self, _args: Args) -> Result<()> {
		return_error!("This command is not yet implemented.");
	}
}
