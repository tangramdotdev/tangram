use {crate::Cli, tangram_client::prelude::*};

/// Get process metadata.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_process_metadata(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::process::metadata::Arg {
			local: args.local.local,
			remotes: args.remotes.remotes,
		};
		let output = handle
			.try_get_process_metadata(&args.process, arg)
			.await
			.map_err(
				|source| tg::error!(!source, id = %args.process, "failed to get the process metadata"),
			)?
			.ok_or_else(|| tg::error!(id = %args.process, "failed to find the process metadata"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
