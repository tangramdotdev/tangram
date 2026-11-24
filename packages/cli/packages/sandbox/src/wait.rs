use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Wait for a sandbox to be destroyed.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub options: Options,

	pub sandbox: tg::Referent<tg::sandbox::Id>,
}

#[derive(Clone, Debug, Default, usage::Args)]
#[usage(args_override_self = false)]
pub struct Options {
	#[usage(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,
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
	let options_ = tg::sandbox::wait::Options { location: None };
	let outcome = sandbox
		.wait_with_instance(&client, options_)
		.await
		.map_err(|error| tg::error!(!error, %id, "failed to wait for the sandbox"))?;
	cli.print_serde(outcome.to_data(), options.print).await?;
	Ok(())
}
