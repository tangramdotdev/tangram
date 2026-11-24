use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Create a group.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub ancestors: tangram_cli_core::node::Ancestors,

	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub output: tangram_cli_core::print::OutputOptions,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	pub specifier: tg::Specifier,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::group::create::Arg {
		ancestors: args.ancestors.get(),
		location: args.location.get(),
		specifier: args.specifier.clone(),
	};
	let output = client.create_group(arg).await.map_err(
		|error| tg::error!(!error, specifier = %args.specifier, "failed to create the group"),
	)?;
	if args.output.verbose {
		cli.print_serde(output, args.print).await?;
	} else {
		cli.print_serde(output.data, args.print).await?;
	}
	Ok(())
}
