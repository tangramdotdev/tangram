use {
	clap::Parser as _,
	std::{path::PathBuf, process::ExitCode},
};

mod clean;
mod database;
mod progress;
mod run;
mod semaphore;
mod snapshot;
mod temp;
mod test;
mod vfs;

type Error = Box<dyn std::error::Error + Send + Sync>;

type Result<T, E = Error> = std::result::Result<T, E>;

/// Run the CLI tests.
#[derive(clap::Parser)]
enum Args {
	Clean(clean::Args),
	Run(run::Args),
	#[command(subcommand)]
	Semaphore(semaphore::Command),
}

fn main() -> ExitCode {
	let args = Args::parse();
	let result = match args {
		Args::Clean(args) => runtime().and_then(|runtime| runtime.block_on(clean::clean(args))),
		Args::Run(args) => runtime().and_then(|runtime| runtime.block_on(run::run(args))),
		Args::Semaphore(command) => semaphore::command(&command),
	};
	match result {
		Ok(code) => code,
		Err(error) => {
			eprintln!("error: {error}");
			ExitCode::FAILURE
		},
	}
}

/// Get the path to the repository.
#[must_use]
pub fn repository_path() -> PathBuf {
	PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn runtime() -> Result<tokio::runtime::Runtime> {
	let runtime = tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()?;
	Ok(runtime)
}
