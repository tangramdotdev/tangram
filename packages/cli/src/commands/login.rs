use crate::Cli;
use tangram_client as tg;

/// Log in to Tangram.
#[derive(Debug, clap::Args)]
pub struct Args {}

impl Cli {
	pub async fn command_login(&self, _args: Args) -> tg::Result<()> {
		Err(tg::error!("not yet implemented"))
	}
}
