use {std::path::PathBuf, tangram_client::prelude::*};

/// Create a new package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(default_value = ".", index = 1)]
	pub path: PathBuf,
}

pub async fn command_new(args: Args) -> tg::Result<()> {
	let args = tangram_cli_init::Args { path: args.path };
	tangram_cli_init::command_init(args).await?;
	Ok(())
}
