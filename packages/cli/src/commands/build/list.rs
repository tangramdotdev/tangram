use crate::Cli;
use tangram_client as tg;

/// List builds.
#[derive(Debug, clap::Args)]
pub struct Args {}

impl Cli {
	pub async fn command_build_list(&self, _args: Args) -> tg::Result<()> {
		Err(tg::error!("unimplemented"))
	}
}
