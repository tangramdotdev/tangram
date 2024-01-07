use crate::Cli;
use tangram_client as tg;
use tangram_error::{error, Result};

/// Print the dependency tree of a package.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[arg(default_value = ".")]
	pub package: tg::Dependency,
}

impl Cli {
	#[allow(clippy::unused_async)]
	pub async fn command_tree(&self, _args: Args) -> Result<()> {
		Err(error!("This command is not yet implemented."))
	}
}
