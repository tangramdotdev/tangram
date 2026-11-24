use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	pub checkpoint: String,

	pub watch: u64,

	pub hit: u64,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	client
		.continue_checkpoint_hit(&args.checkpoint, args.watch, args.hit)
		.await
		.map_err(|error| tg::error!(!error, "failed to continue the checkpoint hit"))?;
	Ok(())
}
