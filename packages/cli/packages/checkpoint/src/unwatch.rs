use {tangram_cli_core::Cli, tangram_client::prelude::*};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub checkpoint: String,

	#[arg(index = 2)]
	pub watch: u64,
}

pub async fn command_checkpoint_unwatch(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	client
		.unwatch_checkpoint(&args.checkpoint, args.watch)
		.await
		.map_err(|error| tg::error!(!error, "failed to remove the checkpoint watch"))?;
	Ok(())
}
