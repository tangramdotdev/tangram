use {clap::Parser as _, std::process::ExitCode};

mod process;
mod run;
mod server;
mod signal;

type Error = Box<dyn std::error::Error + Send + Sync>;

type Result<T, E = Error> = std::result::Result<T, E>;

/// Supervise the processes of a CLI test without polling.
#[derive(clap::Parser)]
enum Args {
	Run(run::Args),
	Start(server::StartArgs),
	Status(server::StatusArgs),
	Stop(server::StopArgs),
	Wait(server::WaitArgs),
}

fn main() -> ExitCode {
	let args = Args::parse();
	let result = match args {
		Args::Run(args) => run::run(&args),
		Args::Start(args) => server::start(&args),
		Args::Status(args) => server::status(&args),
		Args::Stop(args) => server::stop(&args),
		Args::Wait(args) => server::wait(&args),
	};
	match result {
		Ok(code) => ExitCode::from(code),
		Err(error) => {
			eprintln!("error: {error}");
			ExitCode::FAILURE
		},
	}
}
