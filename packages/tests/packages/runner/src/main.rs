use {
	clap::Parser as _,
	std::{path::PathBuf, process::ExitCode},
};

mod clean;
mod database;
mod instance;
mod progress;
mod run;
mod semaphore;
mod snapshot;
mod temp;
mod test;
mod vfs;
mod wrap;

type Error = Box<dyn std::error::Error + Send + Sync>;

type Result<T, E = Error> = std::result::Result<T, E>;

/// Run the CLI tests.
#[derive(clap::Parser)]
enum Args {
	Clean(clean::Args),
	#[command(subcommand)]
	Database(database::Subcommand),
	#[command(subcommand)]
	Instance(instance::Subcommand),
	Run(run::Args),
	#[command(subcommand)]
	Semaphore(semaphore::Command),
	Wrap(wrap::Args),
}

/// Get the path to the repository.
#[must_use]
pub fn repository_path() -> PathBuf {
	PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../..")
}

fn main() -> ExitCode {
	let args = Args::parse();
	let result = match args {
		Args::Clean(args) => runtime().and_then(|runtime| runtime.block_on(clean::clean(args))),
		Args::Database(subcommand) => {
			runtime().and_then(|runtime| runtime.block_on(database::command(subcommand)))
		},
		Args::Instance(subcommand) => {
			runtime().and_then(|runtime| runtime.block_on(instance::command(subcommand)))
		},
		Args::Run(args) => runtime().and_then(|runtime| runtime.block_on(run::run(args))),
		Args::Semaphore(command) => semaphore::command(&command),
		Args::Wrap(args) => runtime().and_then(|runtime| runtime.block_on(wrap::wrap(args))),
	};
	match result {
		Ok(code) => code,
		Err(error) => {
			eprintln!("error: {error}");
			ExitCode::FAILURE
		},
	}
}

fn runtime() -> Result<tokio::runtime::Runtime> {
	let runtime = tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()?;
	Ok(runtime)
}
