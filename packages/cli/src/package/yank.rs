use crate::Cli;
use crossterm::style::Stylize as _;
use tangram_client::{self as tg, Handle as _};

/// Yank a package.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,
}

impl Cli {
	pub async fn command_package_yank(&self, args: Args) -> tg::Result<()> {
		let mut dependency = args.package;

		// Canonicalize the path.
		if let Some(path) = dependency.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Return an error if there's an attempt to yank a package by version req
		if let Some(version) = dependency.version.as_ref() {
			if "<>^~*".chars().any(|ch| version.starts_with(ch)) {
				return Err(tg::error!(%dependency, "cannot yank a package using a version req"));
			}
		}

		// Create the package.
		let package = tg::package::get(&self.handle, &dependency)
			.await
			.map_err(|source| tg::error!(!source, %dependency, "failed to get the package"))?;

		// Get the package ID.
		let id = package
			.id(&self.handle, None)
			.await?
			.try_into()
			.ok()
			.ok_or_else(|| tg::error!("expected a directory"))?;

		// Yank the package.
		self.handle
			.yank_package(&id)
			.await
			.map_err(|source| tg::error!(!source, "failed to yank the package"))?;

		// Get the package metadata
		let metadata = tg::package::get_metadata(&self.handle, &package).await?;
		println!(
			"{}: {} {}@{}",
			"info".blue(),
			"YANKED".red().bold(),
			metadata.name.unwrap().red(),
			metadata.version.unwrap().green()
		);

		Ok(())
	}
}
