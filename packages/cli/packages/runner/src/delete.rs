use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Delete a runner.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	pub runner: tg::Referent<tg::runner::Id>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::runner::delete::Arg {
		location: args.location.get_for_options(&args.runner),
	};
	let runner = args.runner.node;
	client
		.try_delete_runner(&runner, arg)
		.await
		.map_err(|error| tg::error!(!error, %runner, "failed to delete the runner"))?
		.ok_or_else(|| tg::error!(%runner, "failed to find the runner"))?;

	Ok(())
}
