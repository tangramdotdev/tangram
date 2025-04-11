use crate::Cli;
use std::path::PathBuf;
use tangram_client as tg;

/// Create a new package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub path: Option<PathBuf>,
}

impl Cli {
	pub async fn command_package_new(&mut self, args: Args) -> tg::Result<()> {
		let args = crate::package::init::Args { path: args.path };
		self.command_package_init(args).await?;
		Ok(())
	}
}
