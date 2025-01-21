use crate::Cli;
use std::io::IsTerminal as _;
use tangram_client as tg;

/// Build a command.
#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub inner: crate::command::run::InnerArgs,

	/// The reference to the command to build.
	#[arg(index = 1)]
	pub reference: Option<tg::Reference>,
}

impl Cli {
	pub async fn command_command_build(&self, args: Args) -> tg::Result<()> {
		// Get the reference.
		let reference = args
			.reference
			.clone()
			.unwrap_or_else(|| ".".parse().unwrap());

		// Run the command.
		let kind = crate::command::run::InnerKind::Build;
		let output = self.command_run_inner(reference, kind, args.inner).await?;

		// Print the output.
		match output {
			crate::command::run::InnerOutput::Detached(process) => {
				println!("{process}");
			},
			crate::command::run::InnerOutput::Path(path) => {
				println!("{}", path.display());
			},
			crate::command::run::InnerOutput::Value(value) => {
				if !value.is_null() {
					let stdout = std::io::stdout();
					let value = if stdout.is_terminal() {
						let options = tg::value::print::Options {
							recursive: false,
							style: tg::value::print::Style::Pretty { indentation: "  " },
						};
						value.print(options)
					} else {
						value.to_string()
					};
					println!("{value}");
				}
			},
		}

		Ok(())
	}
}
