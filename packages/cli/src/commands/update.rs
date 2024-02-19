use crate::Cli;
use std::path::PathBuf;
use tangram_error::{error, Result};

/// Update a package's dependencies.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[arg(short, long, default_value = ".")]
	pub path: PathBuf,
}

impl Cli {
	pub async fn command_update(&self, _args: Args) -> Result<()> {
		Err(error!("This command is not yet implemented."))
	}
}
