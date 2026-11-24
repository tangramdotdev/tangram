use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	pub checkpoint: String,

	#[usage(default = "{}", long)]
	pub params: crate::Params,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::checkpoint::abort::Arg {
		params: args.params.0,
	};
	client
		.abort_checkpoint(&args.checkpoint, arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to register the checkpoint abort"))?;
	Ok(())
}
