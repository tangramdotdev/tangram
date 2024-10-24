use crate::Cli;
use std::path::PathBuf;
use tangram_client::{self as tg, Handle as _};

/// Format a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1, default_value = ".")]
	pub path: PathBuf,
}

impl Cli {
	pub async fn command_package_format(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the absolute path.
		let path = std::path::absolute(&args.path)
			.map_err(|source| tg::error!(!source, "failed to get the absolute path"))?;

		// Format the package.
		let arg = tg::package::format::Arg { path };
		handle.format_package(arg).await?;

		Ok(())
	}
}
