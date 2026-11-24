use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get the time of the server's virtual clock.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {}

pub async fn run(cli: &mut Cli, _args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let output = client
		.get_clock()
		.await
		.map_err(|error| tg::error!(!error, "failed to get the clock"))?;
	cli.print_serde(output, tangram_cli_core::print::Options::default())
		.await?;
	Ok(())
}
