use crate::Cli;
use crossterm::style::Stylize as _;
use tangram_client as tg;

/// Yank a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(default_value = ".")]
	pub package: tg::Dependency,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_package_yank(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;

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
		let package = tg::package::get(&client, &dependency)
			.await
			.map_err(|source| tg::error!(!source, %dependency, "failed to get the package"))?;

		// Get the package ID.
		let id = package.id(&client).await?;

		// Yank the package.
		let remote = args
			.remote
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::package::yank::Arg { remote };
		client
			.yank_package(&id, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to yank the package"))?;

		// Get the package metadata
		let metadata = tg::package::get_metadata(&client, &package).await?;
		println!(
			"{} {} {}@{}",
			"info".blue().bold(),
			"YANKED".red().bold(),
			metadata.name.unwrap().red(),
			metadata.version.unwrap().green()
		);

		Ok(())
	}
}
