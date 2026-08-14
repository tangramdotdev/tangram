use {crate::Cli, tangram_client::prelude::*};

/// Get a process's output.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_process_output(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let mut locations = args.locations;
		locations.set_from_reference_if_unset(&args.reference);
		let process = self
			.resolve_process_with_locations(&args.reference, &locations)
			.await?;
		let id = process.node.clone();
		let location = locations.get_for_options(&process);
		let process = tg::Process::<tg::Value>::with_referent(process);
		let options = tg::process::wait::Options { location };
		let output = process
			.output_with_handle(&client, options)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process output"))?;
		self.print_serde(output.to_data(), args.print).await?;
		Ok(())
	}
}
