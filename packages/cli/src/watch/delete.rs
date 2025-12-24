use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

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
		let path = tangram_util::fs::canonicalize_parent(&args.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
		let arg = tg::watch::delete::Arg { path: path.clone() };
		handle.delete_watch(arg).await.map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to delete the watch"),
		)?;
		Ok(())
	}
}
