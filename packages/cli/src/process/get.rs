use {crate::Cli, tangram_client::prelude::*};

/// Get a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	/// Get the process's metadata.
	#[arg(long)]
	pub metadata: bool,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::Reference,

	/// Get the process's storage status.
	#[arg(long)]
	pub stored: bool,
}

impl Cli {
	pub async fn command_process_get(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let process = self.resolve_process(&args.process).await?;
		let id = process.node;
		let arg = tg::process::get::Arg {
			location: args.locations.get(),
			metadata: args.metadata,
			stored: args.stored,
			token: process.options.token,
		};
		let output = client
			.try_get_process(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
		if let Some(metadata) = output.metadata {
			let metadata = serde_json::to_string(&metadata)
				.map_err(|error| tg::error!(!error, "failed to serialize the metadata"))?;
			self.print_info_message(&metadata);
		}
		if let Some(stored) = output.stored {
			let stored = serde_json::to_string(&stored)
				.map_err(|error| tg::error!(!error, "failed to serialize the storage status"))?;
			self.print_info_message(&stored);
		}
		self.print_serde(output.data, args.print).await?;
		Ok(())
	}
}
