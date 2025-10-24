use crate::Cli;

mod run;
mod sandbox;
mod session;

#[derive(Clone, Debug, clap::Args)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Run(self::run::Args),
	Sandbox(self::sandbox::Args),
	Session(self::session::Args),
}

impl Cli {
	#[must_use]
	pub fn command_internal(args: Args) -> std::process::ExitCode {
		match args.command {
			Command::Run(args) => Cli::command_internal_run(args),
			Command::Sandbox(args) => Cli::command_internal_sandbox(args),
			Command::Session(args) => Cli::command_internal_session(args),
		}
	}
}
