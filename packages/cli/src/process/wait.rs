use crate::Cli;
use std::io::IsTerminal as _;
use tangram_client as tg;

/// Wait for a process to finish.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub pretty: Option<bool>,

	#[arg(index = 1)]
	pub process: tg::process::Id,
}

impl Cli {
	pub async fn command_process_wait(&self, args: Args) -> tg::Result<()> {
		// let handle = self.handle().await?;
		// let process = tg::Process::with_id(args.process);
		// let output = process.wait(&handle).await?;
		// Self::output_json(output, args.pretty).await?;
		// Ok(())
		todo!()
	}
}
