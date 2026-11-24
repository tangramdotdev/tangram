use {std::path::PathBuf, tangram_cli_core::Cli, tangram_client::prelude::*};

/// Delete a watch.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	pub path: PathBuf,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let path = tangram_util::fs::canonicalize_parent(&args.path)
		.await
		.map_err(|error| tg::error!(!error, "failed to canonicalize the path"))?;
	let arg = tg::watch::delete::Arg { path: path.clone() };
	client.delete_watch(arg).await.map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to delete the watch"),
	)?;
	Ok(())
}
