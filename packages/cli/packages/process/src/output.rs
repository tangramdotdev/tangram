use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get a process's output.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	pub reference: tg::Reference,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let mut locations = args.locations;
	locations.set_from_reference_if_unset(&args.reference);
	let process =
		tangram_cli_reference::get_process_with_locations(cli, &args.reference, &locations).await?;
	let id = process.node.clone();
	let location = locations.get_for_options(&process);
	let process = tg::Process::<tg::Value>::with_referent(process);
	let options = tg::process::wait::Options { location };
	let output = process
		.output_with_instance(&client, options)
		.await
		.map_err(|error| tg::error!(!error, %id, "failed to get the process output"))?;
	cli.print_serde(output.to_data(), args.print).await?;
	Ok(())
}
