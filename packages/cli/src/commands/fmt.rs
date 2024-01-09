use crate::Cli;
use std::path::PathBuf;
use tangram_error::{error, Result};

/// Format the files in a package.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[arg(default_value = ".")]
	pub path: PathBuf,
}

impl Cli {
	#[allow(clippy::unused_async)]
	pub async fn command_fmt(&self, _args: Args) -> Result<()> {
		Err(error!("Not yet implemented."))
	}
}
