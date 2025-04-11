use crate::Cli;
use std::io::IsTerminal as _;
use tangram_client as tg;

/// Get a process's output.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
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
		let stdout = std::io::stdout();
		let output = if stdout.is_terminal() {
			let options = tg::value::print::Options {
				recursive: false,
				style: tg::value::print::Style::Pretty { indentation: "  " },
			};
			output.print(options)
		} else {
			output.to_string()
		};
		println!("{output}");

		Ok(())
	}
}
