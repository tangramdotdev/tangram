use crate::Cli;

pub mod init;
pub mod run;

/// Manage Linux sandbox containers.
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
	pub fn command_sandbox_container(args: Args) -> std::process::ExitCode {
		match args.command {
			Command::Init(args) => Self::command_sandbox_container_init(args),
			Command::Run(args) => Self::command_sandbox_container_run(args),
		}
	}
}

fn chunk_pairs<T>(values: Vec<T>) -> Vec<[T; 2]> {
	let mut iter = values.into_iter();
	let mut output = Vec::new();
	while let Some(first) = iter.next() {
		let second = iter.next().unwrap();
		output.push([first, second]);
	}
	output
}

fn chunk_triples<T>(values: Vec<T>) -> Vec<[T; 3]> {
	let mut iter = values.into_iter();
	let mut output = Vec::new();
	while let Some(first) = iter.next() {
		let second = iter.next().unwrap();
		let third = iter.next().unwrap();
		output.push([first, second, third]);
	}
	output
}
