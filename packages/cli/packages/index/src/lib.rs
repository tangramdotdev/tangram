use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Index processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Also wait for the permission capture queue to be empty, and for the index updates that the captures produce.
	#[arg(long)]
	pub capture: bool,

	/// Also do everything that --capture does, and aggregate usage for every hour before the server's current hour.
	#[arg(long)]
	pub usage: bool,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::index::Arg {
		capture: args.capture,
		usage: args.usage,
	};
	let stream = client
		.index(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to index"))?;
	cli.render_progress_stream(stream).await?;
	Ok(())
}
