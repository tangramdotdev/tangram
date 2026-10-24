use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Delete a runner.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[arg(index = 1)]
	pub runner: tg::runner::Id,
}

pub async fn command_runner_delete(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::runner::delete::Arg {
		location: args.location.get(),
	};
	client
		.try_delete_runner(&args.runner, arg)
		.await
		.map_err(|error| tg::error!(!error, runner = %args.runner, "failed to delete the runner"))?
		.ok_or_else(|| tg::error!(runner = %args.runner, "failed to find the runner"))?;

	Ok(())
}
