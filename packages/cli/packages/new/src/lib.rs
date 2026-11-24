use {std::path::PathBuf, tangram_client::prelude::*};

/// Create a new package.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(default = ".")]
	pub path: PathBuf,
}

pub async fn run(args: Args) -> tg::Result<()> {
	let args = tangram_cli_init::Args { path: args.path };
	tangram_cli_init::run(args).await?;
	Ok(())
}
