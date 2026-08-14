use {crate::Cli, tangram_client::prelude::*};

/// Touch a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	#[arg(index = 1)]
	pub process: tg::Reference,
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	#[command(flatten)]
	pub locations: crate::location::Args,
}

impl Cli {
	pub async fn command_process_touch(&mut self, args: Args) -> tg::Result<()> {
		let process = self
			.resolve_process_with_locations(&args.process, &args.options.locations)
			.await?;
		self.command_process_touch_inner(process, args.options)
			.await
	}

	pub(crate) async fn command_process_touch_inner(
		&mut self,
		process: tg::Referent<tg::process::Id>,
		_options: Options,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let id = process.node.clone();
		let process = tg::Process::<tg::Value>::with_referent(process);
		process
			.touch_with_handle(&client)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to touch the process"))?;
		Ok(())
	}
}
