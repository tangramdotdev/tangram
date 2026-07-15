use {crate::Cli, tangram_client::prelude::*};

/// Touch a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(index = 1)]
	pub process: tg::Referent<tg::process::Id>,
}

impl Cli {
	pub async fn command_process_touch(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let id = args.process.item;
		let arg = tg::process::touch::Arg {
			location: args.locations.get(),
			token: args.process.options.token,
		};
		client
			.touch_process(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to touch the process"))?;
		Ok(())
	}
}
