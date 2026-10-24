use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Index processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

pub async fn command_index(cli: &mut Cli, _args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let stream = client
		.index()
		.await
		.map_err(|error| tg::error!(!error, "failed to index"))?;
	cli.render_progress_stream(stream).await?;
	Ok(())
}
