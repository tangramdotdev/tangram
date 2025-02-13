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
	pub async fn command_process_output(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let process = tg::Process::new(args.process, None, None, None, None);
		let output = process.wait(&handle).await?;
		let output = if output.status.is_succeeded() {
			output
				.output
				.ok_or_else(|| tg::error!("expected the output to be set"))?
		} else if let Some(error) = output.error {
			return Err(tg::error!(!error, "the process failed"));
		} else {
			return Err(tg::error!("the process failed"));
		};
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
