use {
	crate::Cli,
	std::path::PathBuf,
	tangram_client::{self as tg, prelude::*},
};

/// Delete a watch.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub path: PathBuf,
}

impl Cli {
	pub async fn command_watch_delete(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let path = std::path::absolute(args.path)
			.map_err(|source| tg::error!(!source, "failed to get the path"))?;
		let arg = tg::watch::delete::Arg { path };
		handle.delete_watch(arg).await?;
		Ok(())
	}
}
