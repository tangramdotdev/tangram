use {
	futures::{TryStreamExt, stream::FuturesUnordered},
	std::path::PathBuf,
	tangram_cli_core::Cli,
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// The watch path.
	path: PathBuf,

	/// The file system event kind.
	#[usage(default = "any", long, value_enum)]
	kind: Kind,

	/// The paths to touch.
	paths: Vec<PathBuf>,
}

#[derive(Clone, Copy, Debug, usage_rs::ValueEnum)]
enum Kind {
	Any,
	Remove,
	Rename,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
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
