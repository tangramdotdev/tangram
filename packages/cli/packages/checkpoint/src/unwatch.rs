use {tangram_cli_core::Cli, tangram_client::prelude::*};

#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	pub checkpoint: String,

	pub watch: u64,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	client
		.unwatch_checkpoint(&args.checkpoint, args.watch)
		.await
		.map_err(|error| tg::error!(!error, "failed to remove the checkpoint watch"))?;
	Ok(())
}
