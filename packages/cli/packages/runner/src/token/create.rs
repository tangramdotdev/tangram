use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Create a runner token.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(index = 1)]
	pub runner: tg::runner::Id,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::runner::token::create::Arg {
		location: args.location.get(),
	};
	let output = client
		.create_runner_token(&args.runner, arg)
		.await
		.map_err(
			|error| tg::error!(!error, runner = %args.runner, "failed to create a runner token"),
		)?;
	cli.print_serde(output.data, args.print).await?;

	Ok(())
}
