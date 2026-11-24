use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Start the server.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {}

pub async fn run(cli: &mut Cli, _args: Args) -> tg::Result<()> {
	let client = cli.create_client()?;
	cli.spawn_server(&client).await?;
	Ok(())
}
