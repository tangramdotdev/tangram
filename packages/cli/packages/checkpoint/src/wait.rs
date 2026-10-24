use {std::time::Duration, tangram_cli_core::Cli, tangram_client::prelude::*};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub checkpoint: String,

	#[arg(index = 3)]
	pub hit: u64,

	/// Fail if the hit does not occur within this duration.
	#[arg(long, value_parser = humantime::parse_duration)]
	pub timeout: Option<Duration>,

	#[arg(index = 2)]
	pub watch: u64,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let future = client.wait_checkpoint_hit(&args.checkpoint, args.watch, args.hit);
	let output = match args.timeout {
		Some(timeout) => tokio::time::timeout(timeout, future)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					checkpoint = %args.checkpoint,
					hit = %args.hit,
					"timed out waiting for the checkpoint hit"
				)
			})?,
		None => future.await,
	}
	.map_err(|error| tg::error!(!error, "failed to wait for the checkpoint hit"))?;
	cli.print_serde(output, tangram_cli_core::print::Options::default())
		.await?;
	Ok(())
}
