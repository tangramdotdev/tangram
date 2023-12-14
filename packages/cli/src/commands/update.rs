use crate::Cli;
use std::path::PathBuf;
use tangram_error::{return_error, Result};

/// Update a package's dependencies.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[arg(short, long, default_value = ".")]
	pub path: PathBuf,
}

impl Cli {
	#[allow(clippy::unused_async)]
	pub async fn command_update(&self, _args: Args) -> Result<()> {
		return_error!("This command is not yet implemented.");
	}
}
