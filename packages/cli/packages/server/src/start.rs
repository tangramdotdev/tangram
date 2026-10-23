use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Start the server.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

pub async fn start(cli: &mut Cli, _args: Args) -> tg::Result<()> {
	let client = cli.create_client()?;
	cli.spawn_server(&client).await?;
	Ok(())
}
