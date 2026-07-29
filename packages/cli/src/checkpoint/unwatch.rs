use {crate::Cli, tangram_client::prelude::*};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub checkpoint: String,

	#[arg(index = 2)]
	pub watch: u64,
}

impl Cli {
	pub async fn command_checkpoint_unwatch(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		client
			.unwatch_checkpoint(&args.checkpoint, args.watch)
			.await
			.map_err(|error| tg::error!(!error, "failed to remove the checkpoint watch"))?;
		Ok(())
	}
}
