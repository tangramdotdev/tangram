use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Get the server's health.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(long, delimiter = ',')]
	pub fields: Option<Vec<String>>,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::health::Arg {
		fields: args.fields,
	};
	let output = client
		.health(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to get the health"))?;
	cli.print_serde(output, args.print).await?;
	Ok(())
}
