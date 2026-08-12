use {crate::Cli, tangram_client::prelude::*};

/// Get a process's storage status.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::Reference,
}

impl Cli {
	pub async fn command_process_stored(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let process = self.resolve_process(&args.process).await?;
		let id = process.node;
		let arg = tg::process::stored::Arg {
			location: args.locations.get(),
			tokens: process.options.tokens,
		};
		let output = client
			.try_get_process_stored(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process's storage status"))?
			.ok_or_else(|| tg::error!(%id, "failed to find the process's storage status"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
