use crate::Cli;
use std::path::PathBuf;
use tangram_client::{self as tg, prelude::*};

/// Format a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(default_value = ".", index = 1)]
	pub path: PathBuf,
}

impl Cli {
	pub async fn command_format(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the absolute path.
		let path = std::path::absolute(&args.path)
			.map_err(|source| tg::error!(!source, "failed to get the absolute path"))?;

		// Format.
		let arg = tg::format::Arg { path };
		handle.format(arg).await?;

		Ok(())
	}
}
