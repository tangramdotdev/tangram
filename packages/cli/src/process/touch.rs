use {crate::Cli, tangram_client::prelude::*};

/// Touch a process.
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
}

impl Cli {
	pub async fn command_process_touch(&mut self, args: Args) -> tg::Result<()> {
		let mut options = args.options;
		options
			.locations
			.set_from_reference_if_unset(&args.reference);
		let process = self
			.get_process_with_locations(&args.reference, &options.locations)
			.await?;
		self.command_process_touch_inner(process, options).await
	}

	pub(crate) async fn command_process_touch_inner(
		&mut self,
		process: tg::Referent<tg::process::Id>,
		options: Options,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let id = process.node.clone();
		let location = options.locations.get_for_options(&process);
		let process = tg::Process::<tg::Value>::with_referent(process);
		let options = tg::process::touch::Options { location };
		process
			.touch_with_handle(&client, options)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to touch the process"))?;
		Ok(())
	}
}
