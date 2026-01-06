use {crate::Cli, tangram_client::prelude::*};

/// Get a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	/// Get the process's metadata.
	#[arg(long)]
	pub metadata: bool,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_process_get(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::process::get::Arg {
			local: args.local.local,
			metadata: args.metadata,
			remotes: args.remotes.remotes,
		};
		let output = handle
			.try_get_process(&args.process, arg)
			.await
			.map_err(|source| tg::error!(!source, id = %args.process, "failed to get the process"))?
			.ok_or_else(|| tg::error!(id = %args.process, "failed to find the process"))?;
		if let Some(metadata) = output.metadata {
			let metadata = serde_json::to_string(&metadata)
				.map_err(|source| tg::error!(!source, "failed to serialize the metadata"))?;
			Self::print_info_message(&metadata);
		}
		self.print_serde(output.data, args.print).await?;
		Ok(())
	}
}
