use {crate::Cli, futures::StreamExt as _, tangram_client::prelude::*};

/// Get a process's status.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::process::Id,
}

impl Cli {
	pub async fn command_process_status(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let stream = handle.get_process_status(&args.process).await?;
		self.print_serde_stream(stream.boxed(), args.print).await?;
		Ok(())
	}
}
