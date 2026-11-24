use {tangram_cli_core::Cli, tangram_client::prelude::*};

#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	pub checkpoint: String,

	#[usage(default = "{}", long)]
	pub params: crate::Params,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::checkpoint::watch::Arg {
		params: args.params.0,
	};
	let output = client
		.watch_checkpoint(&args.checkpoint, arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to create the checkpoint watch"))?;
	cli.print_serde(output, tangram_cli_core::print::Options::default())
		.await?;
	Ok(())
}
