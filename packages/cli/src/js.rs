use crate::Cli;

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	#[must_use]
	pub fn command_js(args: Args) -> std::process::ExitCode {
		todo!()
	}
}
