use crate::Cli;
use tangram_client as tg;

/// Get a package's outdated dependencies.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// If this flag is set, the lock will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(default_value = ".", index = 1)]
	pub package: tg::Reference,
}

impl Cli {
	pub async fn command_outdated(&mut self, _args: Args) -> tg::Result<()> {
		Err(tg::error!("unimplemented"))
	}
}
