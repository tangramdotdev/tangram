use crate::Cli;
use crossterm::style::Stylize as _;
use tangram_client::{self as tg, Handle as _};

/// Publish a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long, default_value = "false")]
	pub locked: bool,

	#[arg(default_value = ".")]
	pub package: tg::Dependency,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_package_publish(&self, mut args: Args) -> tg::Result<()> {
		// Canonicalize the path.
		if let Some(path) = args.package.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, %path, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Create the package.
		let (package, _) = tg::package::get_with_lock(&self.handle, &args.package, args.locked)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the package"))?;

		// Get the package ID.
		let id = package.id(&self.handle).await?;

		// Publish the package.
		let remote = args
			.remote
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::package::publish::Arg { remote };
		self.handle
			.publish_package(&id, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the package"))?;

		// Display
		let metadata = tg::package::get_metadata(&self.handle, &package).await?;
		println!(
			"{} published {}@{}",
			"info".blue().bold(),
			metadata.name.unwrap().red(),
			metadata.version.unwrap().green()
		);
		Ok(())
	}
}
