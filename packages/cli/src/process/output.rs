use crate::Cli;
use tangram_client as tg;

/// Get a process's output.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long, default_value = "1")]
	pub depth: crate::object::get::Depth,

	#[arg(index = 1)]
	pub process: tg::process::Id,
}

impl Cli {
	pub async fn command_process_output(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the process.
		let process = tg::Process::new(args.process, None, None, None, None);

		// Get the output.
		let output = process.output(&handle).await?;

		// Print the output.
		Self::print_output(&output, args.depth);

		Ok(())
	}
}
