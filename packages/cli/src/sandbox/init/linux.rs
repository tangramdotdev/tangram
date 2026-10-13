use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub http2: crate::sandbox::Http2,

	#[arg(action = clap::ArgAction::Append, long = "library-path", num_args = 1)]
	pub library_paths: Vec<PathBuf>,

	#[arg(long)]
	pub output_path: Option<PathBuf>,

	#[arg(long)]
	pub tangram_path: Option<PathBuf>,

	#[arg(long)]
	pub url: Option<tangram_uri::Uri>,
}

impl Cli {
	pub fn command_sandbox_linux_init(args: Args) -> tg::Result<std::process::ExitCode> {
		let arg = tangram_sandbox::init::linux::Arg {
			serve: tangram_sandbox::serve::Arg {
				http2: args.http2.config(),
				library_paths: args.library_paths,
				listen: false,
				output_path: args
					.output_path
					.ok_or_else(|| tg::error!("the sandbox output path is required"))?,
				tangram_path: args
					.tangram_path
					.ok_or_else(|| tg::error!("the tangram executable path is required"))?,
				url: args
					.url
					.ok_or_else(|| tg::error!("the sandbox URL is required"))?,
			},
		};
		tangram_sandbox::init::linux::run(&arg)
	}
}
