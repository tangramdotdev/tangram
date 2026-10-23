use {crate::Component, tangram_cli_get::Ext as _, tangram_client::prelude::*};

/// Get a process's output.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	/// Select the source of process state.
	#[arg(long, default_value = "auto", value_parser = tangram_cli_core::source_parser())]
	pub source: tg::process::Source,
}

impl Component<'_> {
	pub async fn command_process_output(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let mut locations = args.locations;
		locations.set_from_reference_if_unset(&args.reference);
		let process = self
			.get_process_with_locations(&args.reference, &locations)
			.await?;
		let id = process.node.clone();
		let location = locations.get_for_options(&process);
		let process = tg::Process::<tg::Value>::with_referent(process);
		let options = tg::process::wait::Options {
			location,
			source: args.source,
		};
		let output = process
			.output_with_instance(&client, options)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process output"))?;
		self.print_serde(output.to_data(), args.print).await?;
		Ok(())
	}
}
