use {std::path::PathBuf, tangram_client::prelude::*};

/// Perform an operation on sandbox paths in a user namespace that the parent maps.
#[derive(Clone, Debug, clap::Args)]
pub struct Args {
	pub operation: Operation,

	#[arg(num_args = 1.., required = true)]
	pub paths: Vec<PathBuf>,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum Operation {
	Own,
	Probe,
	Reclaim,
	Remove,
}

pub fn run(args: &Args) -> tg::Result<std::process::ExitCode> {
	let operation = match args.operation {
		Operation::Own => tangram_sandbox::unshare::Operation::Own,
		Operation::Probe => tangram_sandbox::unshare::Operation::Probe,
		Operation::Reclaim => tangram_sandbox::unshare::Operation::Reclaim,
		Operation::Remove => tangram_sandbox::unshare::Operation::Remove,
	};
	tangram_sandbox::unshare::main(operation, &args.paths)
}
