use {crate::Cli, tangram_client::prelude::*};

/// Get process metadata.
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
	pub async fn command_process_metadata(&mut self, args: Args) -> tg::Result<()> {
		let process = self
			.resolve_process_with_locations(&args.reference, &args.options.locations)
			.await?;
		self.command_process_metadata_inner(process, args.options)
			.await
	}

	pub(crate) async fn command_process_metadata_inner(
		&mut self,
		process: tg::Referent<tg::process::Id>,
		options: Options,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let id = process.node.clone();
		let process = tg::Process::<tg::Value>::with_referent(process);
		let output = process
			.metadata_with_handle(&client)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process metadata"))?;
		self.print_serde(output, options.print).await?;
		Ok(())
	}
}
