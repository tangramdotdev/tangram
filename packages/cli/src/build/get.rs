use crate::Cli;
use tangram_client::{self as tg, handle::Ext as _};

/// Get a build.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_build_get(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let output = handle.get_build(&args.build).await?;
		Self::output_json(&output).await?;
		Ok(())
	}
}
