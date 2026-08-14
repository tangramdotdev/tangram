use {crate::Cli, tangram_client::prelude::*};

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
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_process_wait(&mut self, args: Args) -> tg::Result<()> {
		let mut options = args.options;
		options
			.locations
			.set_from_reference_if_unset(&args.reference);
		let process = self
			.resolve_process_with_locations(&args.reference, &options.locations)
			.await?;
		self.command_process_wait_inner(process, options).await
	}

	pub(crate) async fn command_process_wait_inner(
		&mut self,
		process: tg::Referent<tg::process::Id>,
		options: Options,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let id = process.node.clone();
		let location = options.locations.get_for_options(&process);
		let process = tg::Process::<tg::Value>::with_referent(process);
		let options_ = tg::process::wait::Options { location };
		let output = process
			.wait_with_handle(&client, options_)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?;
		self.print_serde(output.to_data(), options.print).await?;
		Ok(())
	}
}
