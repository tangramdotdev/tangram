use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

/// Initialize a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(action = clap::ArgAction::Append, long = "library-path", num_args = 1)]
	pub library_paths: Vec<PathBuf>,

	#[arg(long)]
	pub path: PathBuf,

	#[arg(long)]
	pub url: tangram_uri::Uri,

	#[arg(long)]
	pub tangram_path: PathBuf,
}

impl Cli {
	#[must_use]
	pub fn command_sandbox_init(args: Args) -> std::process::ExitCode {
		let config = tangram_sandbox::init::Arg {
			library_paths: args.library_paths,
			path: args.path,
			tangram_path: args.tangram_path,
			url: args.url,
		};
		let result = tangram_sandbox::init(&config);
		if let Err(error) = result {
			Cli::print_error_basic(tg::Referent::with_item(error));
			return std::process::ExitCode::FAILURE;
		}
		std::process::ExitCode::SUCCESS
	}
}
