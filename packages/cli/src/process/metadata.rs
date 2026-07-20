use {crate::Cli, tangram_client::prelude::*};

/// Get process metadata.
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
	pub async fn command_process_metadata(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let process = self.get_resolved_process(&args.process).await?;
		let id = process.item;
		let arg = tg::process::metadata::Arg {
			location: args.locations.get(),
			token: process.options.token,
		};
		let output = client
			.try_get_process_metadata(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process metadata"))?
			.ok_or_else(|| tg::error!(%id, "failed to find the process metadata"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
