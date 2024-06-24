use crate::Cli;
use tangram_client as tg;

/// Get a package's outdated dependencies.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Print as JSON.
	#[arg(long)]
	pub json: bool,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(index = 1, default_value = ".")]
	pub package: tg::Reference,
}

impl Cli {
	pub async fn command_package_outdated(&self, _args: Args) -> tg::Result<()> {
		todo!()
	}
}
