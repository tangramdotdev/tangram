use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	/// Get the process's availability.
	#[arg(long)]
	pub availability: bool,

	#[command(flatten)]
	pub locations: tangram_cli_core::location::Args,

	/// Get the process's metadata.
	#[arg(long)]
	pub metadata: bool,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	/// Select the source of process state.
	#[arg(long, default_value = "auto", value_parser = tangram_cli_core::source_parser())]
	pub source: tg::process::Source,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let mut options = args.options;
	options
		.locations
		.set_from_reference_if_unset(&args.reference);
	let process =
		tangram_cli_reference::get_process_with_locations(cli, &args.reference, &options.locations)
			.await?;
	run_inner(cli, process, options).await
}

pub async fn run_inner(
	cli: &mut Cli,
	process: tg::Referent<tg::process::Id>,
	options: Options,
) -> tg::Result<()> {
	let client = cli.client().await?;
	let id = process.node.clone();
	let location = options.locations.get_for_options(&process);
	let process = tg::Process::<tg::Value>::with_referent(process);
	let options_ = tg::process::get::Options {
		availability: options.availability,
		location,
		metadata: options.metadata,
		source: options.source,
	};
	let output = process
		.get_with_instance(&client, options_)
		.await
		.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
	if let Some(metadata) = output.metadata {
		let metadata = serde_json::to_string(&metadata)
			.map_err(|error| tg::error!(!error, "failed to serialize the metadata"))?;
		cli.print_info_message(&metadata);
	}
	if let Some(availability) = output.availability {
		let availability = serde_json::to_string(&availability)
			.map_err(|error| tg::error!(!error, "failed to serialize the availability"))?;
		cli.print_info_message(&availability);
	}
	let data = if options.print.tokens.get() {
		output.data
	} else {
		output.data.without_location_and_tokens()
	};
	cli.print_serde(data, options.print).await?;
	Ok(())
}
