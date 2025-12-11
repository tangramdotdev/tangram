use {crate::Cli, futures::StreamExt as _, tangram_client::prelude::*};

/// Get a process's status.
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
	pub async fn command_process_status(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::process::status::Arg {
			local: args.local.local,
			remotes: args.remotes.remotes,
		};
		let stream = handle.get_process_status(&args.process, arg).await?;
		self.print_serde_stream(stream.boxed(), args.print).await?;
		Ok(())
	}
}
