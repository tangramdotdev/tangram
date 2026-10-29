use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get the interface of a module.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;

	// Get the module.
	let module = tangram_cli_reference::get_module(cli, &args.reference)
		.await?
		.to_data();

	// Get the interface.
	let arg = tg::module::interface::Arg { module };
	let output = client
		.module_interface(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to get the interface"))?;

	// Print the interface.
	cli.print_serde(output.interface, args.print).await?;

	Ok(())
}
