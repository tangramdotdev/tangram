use {crate::Cli, tangram_client::prelude::*};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub checkpoint: String,

	#[arg(default_value = "{}", long, value_parser = parse_params)]
	pub params: tg::checkpoint::Params,
}

impl Cli {
	pub async fn command_checkpoint_panic(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::checkpoint::panic::Arg {
			params: args.params,
		};
		client
			.panic_checkpoint(&args.checkpoint, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to register the checkpoint panic"))?;
		Ok(())
	}
}

fn parse_params(value: &str) -> Result<tg::checkpoint::Params, serde_json::Error> {
	serde_json::from_str(value)
}
