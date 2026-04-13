use crate::Cli;

pub mod init;
pub mod run;

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	#[command(hide = true)]
	Init(self::init::Args),
	#[command(hide = true)]
	Run(self::run::Args),
}

impl Cli {
	#[must_use]
	pub fn command_sandbox_vm(args: Args) -> std::process::ExitCode {
		match args.command {
			Command::Init(args) => Self::command_sandbox_vm_init(args),
			Command::Run(args) => Self::command_sandbox_vm_run(args),
		}
	}
}
