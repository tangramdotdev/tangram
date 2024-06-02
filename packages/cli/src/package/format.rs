use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Format a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The package to format.
	pub package: tg::Dependency,
}

impl Cli {
	pub async fn command_package_format(&self, mut args: Args) -> tg::Result<()> {
		// Canonicalize the package path.
		if let Some(path) = args.package.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Format the package.
		self.handle.format_package(&args.package).await?;

		Ok(())
	}
}
