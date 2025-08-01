use crate::Cli;
use tangram_client as tg;

/// Get a process's output.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Whether to print blobs.
	#[arg(long)]
	pub print_blobs: bool,

	/// The depth to print.
	#[arg(short = 'd', long, default_value = "0")]
	pub print_depth: crate::object::get::Depth,

	/// Whether to print pretty.
	#[arg(long)]
	pub print_pretty: Option<bool>,

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
		Self::print_output(
			&handle,
			&output,
			args.print_depth,
			args.print_pretty,
			args.print_blobs,
		)
		.await?;

		Ok(())
	}
}
