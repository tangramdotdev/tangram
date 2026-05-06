use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(action = clap::ArgAction::Append, long = "library-path", num_args = 1)]
	pub library_paths: Vec<PathBuf>,

	#[arg(long)]
	pub output_path: PathBuf,

	#[arg(long)]
	pub url: tangram_uri::Uri,

	#[arg(long)]
	pub tangram_path: PathBuf,
}

impl Cli {
	pub fn command_sandbox_container_init(args: Args) -> tg::Result<std::process::ExitCode> {
		let arg = tangram_sandbox::container::init::Arg {
			serve: tangram_sandbox::serve::Arg {
				library_paths: args.library_paths,
				listen: false,
				output_path: args.output_path,
				tangram_path: args.tangram_path,
				url: args.url,
			},
		};
		tangram_sandbox::container::init::run(&arg)
	}
}
