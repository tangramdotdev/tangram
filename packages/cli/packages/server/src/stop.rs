use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Stop the server.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

pub async fn stop(cli: &mut Cli, _args: Args) -> tg::Result<()> {
	cli.stop_server().await?;
	Ok(())
}
