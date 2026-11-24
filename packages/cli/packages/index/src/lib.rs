use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Index processes and objects.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// Also wait for the permission capture queue to be empty, and for the index updates that the captures produce.
	#[usage(long)]
	pub capture: bool,

	/// Also wait for the database index queue to be empty, so that the index reflects every database write.
	#[usage(long)]
	pub database: bool,

	/// Also do everything that --capture does, and aggregate usage for every hour before the server's current hour.
	#[usage(long)]
	pub usage: bool,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::index::Arg {
		capture: args.capture,
		database: args.database,
		usage: args.usage,
	};
	let stream = client
		.index(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to index"))?;
	cli.render_progress_stream(stream).await?;
	Ok(())
}
