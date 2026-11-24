use {tangram_cli_core::Cli, tangram_client::prelude::*};

#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	pub checkpoint: String,

	pub watch: u64,

	/// Fail if the hit does not occur within this duration.
	#[usage(long)]
	pub timeout: Option<humantime::Duration>,

	pub hit: u64,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let future = client.wait_checkpoint_hit(&args.checkpoint, args.watch, args.hit);
	let output = match args.timeout {
		Some(timeout) => tokio::time::timeout(timeout.into(), future)
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
