use crate::Cli;
use crossterm::style::Stylize as _;
use tangram_client::{self as tg, Handle as _};

/// Publish a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,

	#[arg(long, default_value = "false")]
	pub locked: bool,
}

impl Cli {
	pub async fn command_package_publish(&self, args: Args) -> tg::Result<()> {
		let mut dependency = args.package;

		// Canonicalize the path.
		if let Some(path) = dependency.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, %path, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Create the package.
		let (package, _) = tg::package::get_with_lock(&self.handle, &dependency, args.locked)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the package"))?;

		// Get the package ID.
		let id = package.id(&self.handle, None).await?;

		// Publish the package.
		self.handle
			.publish_package(&id)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the package"))?;

		// Display
		let metadata = tg::package::get_metadata(&self.handle, &package).await?;
		println!(
			"{}: published {}@{}",
			"info".blue(),
			metadata.name.unwrap().red(),
			metadata.version.unwrap().green()
		);
		Ok(())
	}
}
