use {crate::Cli, tangram_client::prelude::*};

/// Touch a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(index = 1)]
	pub process: tg::Reference,
}

impl Cli {
	pub async fn command_process_touch(&mut self, args: Args) -> tg::Result<()> {
		let process = self
			.resolve_process_with_location(&args.process, &args.locations)
			.await?;
		self.command_process_touch_with_referent(process).await
	}

	pub(crate) async fn command_process_touch_with_referent(
		&mut self,
		process: tg::Referent<tg::process::Id>,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let location = process.options.location.clone().map(Into::into);
		let id = process.node;
		let arg = tg::process::touch::Arg {
			location,
			tokens: process.options.tokens,
		};
		client
			.touch_process(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to touch the process"))?;
		Ok(())
	}
}
