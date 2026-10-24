use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Wait for a sandbox to be destroyed.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	#[arg(index = 1)]
	pub sandbox: tg::Referent<tg::sandbox::Id>,
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	#[command(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	/// Select the source of sandbox state.
	#[arg(long, default_value = "auto", value_parser = tangram_cli_core::source_parser())]
	pub source: tg::sandbox::Source,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let entry = tg::sandbox::Options {
		location: args.options.locations.get_for_options(&args.sandbox),
		tokens: args.sandbox.options.tokens.clone(),
		..tg::sandbox::Options::default()
	};
	let sandbox = tg::Sandbox::new(args.sandbox.node, entry);
	run_inner(cli, sandbox, args.options).await
}

pub async fn run_inner(cli: &mut Cli, sandbox: tg::Sandbox, options: Options) -> tg::Result<()> {
	let client = cli.client().await?;
	let id = sandbox.id().clone();
	let options_ = tg::sandbox::status::Options {
		source: options.source,
		..Default::default()
	};
	let output = sandbox
		.wait_with_instance(&client, options_)
		.await
		.map_err(|error| tg::error!(!error, %id, "failed to wait for the sandbox"))?;
	cli.print_serde(output, options.print).await?;
	Ok(())
}
