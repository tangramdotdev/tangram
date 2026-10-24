use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Create a runner.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[arg(long)]
	pub owner: Option<tg::principal::Selector>,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn command_runner_create(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::runner::create::Arg {
		location: args.location.get(),
		owner: args.owner,
	};
	let output = client
		.create_runner(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to create the runner"))?;
	cli.print_serde(output, args.print).await?;

	Ok(())
}
