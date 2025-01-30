use crate::Cli;
use std::io::IsTerminal as _;
use tangram_client as tg;

/// Spawn a process.
#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_process_spawn(&self, args: Args) -> tg::Result<()> {
		Ok(())
	}
}
