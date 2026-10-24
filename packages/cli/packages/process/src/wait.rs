use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Wait for a process to finish.
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
	#[command(flatten)]
	pub locations: tangram_cli_core::location::Args,

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
		tangram_cli_get::get_process_with_locations(cli, &args.reference, &options.locations)
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
	let options_ = tg::process::wait::Options {
		location,
		source: options.source,
	};
	let outcome = process
		.wait_with_instance(&client, options_)
		.await
		.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?;
	cli.print_serde(outcome.to_data(), options.print).await?;
	Ok(())
}
