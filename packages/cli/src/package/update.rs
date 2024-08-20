use crate::Cli;
use tangram_client::{self as tg, Handle as _};

/// Update a package's lockfile.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1, default_value = ".")]
	pub path: tg::Path,
}

impl Cli {
	pub async fn command_package_update(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Canonicalize the path.
		let path: tg::Path = tokio::fs::canonicalize(&args.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
			.try_into()?;

		// Remove an existing lockfile.
		tokio::fs::remove_file(path.clone().join("tangram.lock"))
			.await
			.ok();

		// Check in the package.
		let arg = tg::artifact::checkin::Arg {
			destructive: false,
			deterministic: false,
			locked: false,
			path,
		};
		handle.check_in_artifact(arg).await?;

		Ok(())
	}
}
