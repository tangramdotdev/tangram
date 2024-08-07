use crate::Cli;
use tangram_client as tg;

/// Update a package's lockfile.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(default_value = ".")]
	pub path: tg::Path,
}

impl Cli {
	pub async fn command_package_update(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		let mut dependency = tg::Dependency::with_path(args.path);

		// Canonicalize the path.
		if let Some(path) = dependency.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, %path, "failed to canonicalize the path"))?
				.try_into()?;
			tokio::fs::remove_file(path.clone().join("tangram.lock"))
				.await
				.ok();
		}

		let _ = tg::package::get_with_lock(&client, &dependency, false)
			.await
			.map_err(|source| tg::error!(!source, %dependency, "failed to create a new lock"))?;

		Ok(())
	}
}
