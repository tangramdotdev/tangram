use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(action = clap::ArgAction::Append, long = "library-path", num_args = 1)]
	pub library_paths: Vec<PathBuf>,

	#[arg(long)]
	pub url: tangram_uri::Uri,

	#[arg(long)]
	pub tangram_path: PathBuf,
}

impl Cli {
	#[must_use]
	pub fn command_sandbox_container_init(args: Args) -> std::process::ExitCode {
		let arg = tangram_sandbox::container::init::Arg {
			serve: tangram_sandbox::serve::Arg {
				library_paths: args.library_paths,
				tangram_path: args.tangram_path,
				url: args.url,
			},
		};
		match tangram_sandbox::container::init::run(&arg) {
			Ok(code) => code,
			Err(error) => {
				Cli::print_error_basic(tg::Referent::with_item(error));
				std::process::ExitCode::FAILURE
			},
		}
	}
}
