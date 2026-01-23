use {
	crate::Cli,
	futures::{TryStreamExt, stream::FuturesUnordered},
	std::path::PathBuf,
	tangram_client::prelude::*,
};

/// Touch paths
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The path of the watch to touch.
	path: PathBuf,

	/// Items to mark dirty.
	#[arg(short = 'p')]
	items: Vec<PathBuf>,
}

impl Cli {
	pub async fn command_watch_touch(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let path = tangram_util::fs::canonicalize_parent(&args.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
		let items = args
			.items
			.into_iter()
			.map(tangram_util::fs::canonicalize_parent)
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the paths"))?;
		let arg = tg::watch::touch::Arg { path, items };
		handle
			.touch_watch(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to list the watches"))?;
		Ok(())
	}
}
