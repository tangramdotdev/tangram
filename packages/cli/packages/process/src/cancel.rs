use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Cancel a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 2)]
	pub lease: tg::process::Lease,

	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let mut location = args.location;
	location.set_from_reference_if_unset(&args.reference);
	let process =
		tangram_cli_get::get_process_with_locations(cli, &args.reference, &location).await?;
	let location = location.get_for_options(&process);
	let process = tg::Process::<tg::Value>::with_referent(process);
	let options = tg::process::cancel::Options {
		lease: Some(args.lease),
		location,
	};
	process
		.cancel_with_instance(&client, options)
		.await
		.map_err(|error| tg::error!(!error, id = %process.id(), "failed to cancel the process"))?;
	Ok(())
}
