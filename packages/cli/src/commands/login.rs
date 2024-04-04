use crate::Cli;
use tangram_error::{error, Result};

/// Log in to Tangram.
#[derive(Debug, clap::Args)]
pub struct Args {}

impl Cli {
	pub async fn command_login(&self, _args: Args) -> Result<()> {
		Err(error!("not yet implemented"))
	}
}
