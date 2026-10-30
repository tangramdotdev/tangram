use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get the interface of a module.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,

	/// The syntax to print the interface in, which is javascript, json, or python.
	#[arg(default_value = "javascript", long)]
	pub syntax: tg::interface::Syntax,
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
	let text = tg::interface::print::print(&output.interface, args.syntax)?;
	print!("{text}");

	Ok(())
}
