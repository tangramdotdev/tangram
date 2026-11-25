use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Restart the server.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

pub async fn run(cli: &mut Cli, _args: Args) -> tg::Result<()> {
	let client = cli.create_client()?;
	cli.restart_server(&client).await?;
	Ok(())
}
