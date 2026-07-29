use {crate::Cli, tangram_client::prelude::*};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub checkpoint: String,

	#[arg(index = 3)]
	pub hit: u64,

	#[arg(index = 2)]
	pub watch: u64,
}

impl Cli {
	pub async fn command_checkpoint_wait(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let output = client
			.wait_checkpoint_hit(&args.checkpoint, args.watch, args.hit)
			.await
			.map_err(|error| tg::error!(!error, "failed to wait for the checkpoint hit"))?;
		self.print_serde(output, crate::print::Options::default())
			.await?;
		Ok(())
	}
}
