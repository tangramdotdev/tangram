use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Create a runner token.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

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
