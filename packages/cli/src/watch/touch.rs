use {
	crate::Cli,
	futures::{TryStreamExt, stream::FuturesUnordered},
	std::path::PathBuf,
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The paths to touch.
	#[arg(index = 2)]
	paths: Vec<PathBuf>,

	/// The file system event kind.
	#[arg(default_value = "any", long, value_enum)]
	kind: Kind,

	/// The watch path.
	#[arg(index = 1)]
	path: PathBuf,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum Kind {
	Any,
	Remove,
	Rename,
}

impl Cli {
	pub async fn command_watch_touch(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let path = tangram_util::fs::canonicalize_parent(&args.path)
			.await
			.map_err(|error| tg::error!(!error, "failed to canonicalize the path"))?;
		let paths = args
			.paths
			.into_iter()
			.map(tangram_util::fs::canonicalize_parent)
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await
			.map_err(|error| tg::error!(!error, "failed to canonicalize the paths"))?;
		let kind = match args.kind {
			Kind::Any => tg::watch::touch::Kind::Any,
			Kind::Remove => tg::watch::touch::Kind::Remove,
			Kind::Rename => tg::watch::touch::Kind::Rename,
		};
		let arg = tg::watch::touch::Arg { kind, path, paths };
		client
			.touch_watch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to touch the watch"))?;
		Ok(())
	}
}
