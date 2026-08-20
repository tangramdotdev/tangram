use {crate::Cli, tangram_client::prelude::*};

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
	pub locations: crate::location::Args,

	/// Get the process's metadata.
	#[arg(long)]
	pub metadata: bool,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_process_get(&mut self, args: Args) -> tg::Result<()> {
		let mut options = args.options;
		options
			.locations
			.set_from_reference_if_unset(&args.reference);
		let process = self
			.get_process_with_locations(&args.reference, &options.locations)
			.await?;
		self.command_process_get_inner(process, options).await
	}

	pub(crate) async fn command_process_get_inner(
		&mut self,
		process: tg::Referent<tg::process::Id>,
		options: Options,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let id = process.node.clone();
		let location = options.locations.get_for_options(&process);
		let process = tg::Process::<tg::Value>::with_referent(process);
		let options_ = tg::process::get::Options {
			availability: options.availability,
			location,
			metadata: options.metadata,
		};
		let output = process
			.get_with_handle(&client, options_)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
		if let Some(metadata) = output.metadata {
			let metadata = serde_json::to_string(&metadata)
				.map_err(|error| tg::error!(!error, "failed to serialize the metadata"))?;
			self.print_info_message(&metadata);
		}
		if let Some(availability) = output.availability {
			let availability = serde_json::to_string(&availability)
				.map_err(|error| tg::error!(!error, "failed to serialize the availability"))?;
			self.print_info_message(&availability);
		}
		self.print_serde(output.data, options.print).await?;
		Ok(())
	}
}
