use crate::Cli;
use tangram_client::{self as tg, Handle as _};

/// Format a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1, default_value = ".")]
	pub path: tg::Path,
}

impl Cli {
	pub async fn command_package_format(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Canonicalize the path.
		let path = tokio::fs::canonicalize(&args.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
			.try_into()?;

		// Format the package.
		let arg = tg::package::format::Arg { path };
		handle.format_package(arg).await?;

		Ok(())
	}
}
