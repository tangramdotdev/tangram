use crate::Cli;
use futures::TryStreamExt as _;
use std::path::PathBuf;
use tangram_client::{self as tg, prelude::*};

/// Update a package's lockfile.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1, default_value = ".")]
	pub path: PathBuf,
}

impl Cli {
	pub async fn command_update(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the absolute path.
		let path = std::path::absolute(&args.path)
			.map_err(|source| tg::error!(!source, "failed to get the absolute path"))?;

		// Remove an existing lockfile.
		tokio::fs::remove_file(path.clone().join(tg::package::LOCKFILE_FILE_NAME))
			.await
			.ok();

		// Check in the package.
		let arg = tg::checkin::Arg {
			cache: false,
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			lockfile: true,
			path,
		};
		let stream = handle.checkin(arg).await?;
		stream.map_ok(|_| ()).try_collect::<()>().await?;

		Ok(())
	}
}
