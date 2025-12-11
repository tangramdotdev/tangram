use {crate::Cli, tangram_client::prelude::*};

/// Cancel a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,

	#[arg(index = 2)]
	pub token: String,
}

impl Cli {
	pub async fn command_process_cancel(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Cancel the process.
		let arg = tg::process::cancel::Arg {
			local: args.local.local,
			remotes: args.remotes.remotes,
			token: args.token,
		};
		handle
			.cancel_process(&args.process, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to cancel the process"))?;

		Ok(())
	}
}
