use {crate::Cli, tangram_client::prelude::*};

/// Touch a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(index = 1)]
	pub process: tg::process::Id,
}

impl Cli {
	pub async fn command_process_touch(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::process::touch::Arg {
			location: args.locations.get(),
		};
		handle.touch_process(&args.process, arg).await.map_err(
			|source| tg::error!(!source, id = %args.process, "failed to touch the process"),
		)?;
		Ok(())
	}
}
