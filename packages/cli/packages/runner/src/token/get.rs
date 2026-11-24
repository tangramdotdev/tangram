use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Get a runner token.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	pub runner: tg::runner::Id,

	pub token: tg::Referent<tg::token::Id>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::runner::token::get::Arg {
		location: args.location.get_for_options(&args.token),
	};
	let runner = args.runner;
	let token = args.token.node;
	let output = client
		.try_get_runner_token(&runner, &token, arg)
		.await
		.map_err(|error| tg::error!(!error, %runner, %token, "failed to get the runner token"))?
		.ok_or_else(|| tg::error!(%runner, %token, "failed to find the runner token"))?;
	let tg::runner::token::get::Output { data, location } = output;
	cli.print_location_and_tokens(location.as_ref(), &tg::authorization::Tokens::default())?;
	cli.print_serde(data, args.print).await?;

	Ok(())
}
