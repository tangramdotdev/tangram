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

		// Get the process.
		let process = tg::Process::new(args.process, None, None, None, None);

		// Await the process.
		let wait = process.wait(&handle).await?;

		// Return an error if necessary.
		if let Some(error) = wait.error {
			return Err(error);
		}
		match &wait.exit {
			Some(tg::process::Exit::Code { code }) if *code != 0 => {
				return Err(tg::error!("the process exited with code {code}"));
			},
			Some(tg::process::Exit::Signal { signal }) => {
				return Err(tg::error!("the process exited with signal {signal}"));
			},
			_ => (),
		}

		// Get the output.
		let output: tg::Value = wait
			.output
			.ok_or_else(|| tg::error!("expected the output to be set"))?;

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
