use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Create a runner.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[usage(long)]
	pub owner: Option<tg::principal::Selector>,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
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
