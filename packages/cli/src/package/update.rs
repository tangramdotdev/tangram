use crate::Cli;
use tangram_client as tg;

/// Update a package's lockfile.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1, default_value = ".")]
	pub path: tg::Path,
}

impl Cli {
	pub async fn command_package_update(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		// Canonicalize the path.
		let path = args.path;
		let path: tg::Path = tokio::fs::canonicalize(&path)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to canonicalize the path"))?
			.try_into()?;

		// Remove an existing lockfile.
		tokio::fs::remove_file(path.clone().join("tangram.lock"))
			.await
			.ok();

		// Create the package.
		let arg = tg::package::create::Arg {
			reference: tg::Reference::with_path(path),
			locked: false,
			remote: None,
		};
		client.create_package(arg).await?;

		Ok(())
	}
}
