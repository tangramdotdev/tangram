use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

/// Serve sandbox requests.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(action = clap::ArgAction::Append, long = "library-path", num_args = 1)]
	pub library_paths: Vec<PathBuf>,

	#[command(flatten)]
	pub listen: Listen,

	#[arg(long)]
	pub output_path: PathBuf,

	#[arg(long)]
	pub url: tangram_uri::Uri,

	#[arg(long)]
	pub tangram_path: PathBuf,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Listen {
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "listen",
		require_equals = true,
	)]
	connect: Option<bool>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "connect",
		require_equals = true,
	)]
	listen: Option<bool>,
}

impl Listen {
	#[must_use]
	pub fn get(&self) -> bool {
		self.listen
			.or(self.connect.map(|value| !value))
			.unwrap_or(true)
	}
}

impl Cli {
	#[must_use]
	pub fn command_sandbox_serve(args: Args) -> std::process::ExitCode {
		let arg = tangram_sandbox::serve::Arg {
			library_paths: args.library_paths,
			listen: args.listen.get(),
			output_path: args.output_path,
			tangram_path: args.tangram_path,
			url: args.url,
		};
		let result = tangram_sandbox::serve::run(&arg);
		if let Err(error) = result {
			Cli::print_error_basic(tg::Referent::with_item(error));
			return std::process::ExitCode::FAILURE;
		}
		std::process::ExitCode::SUCCESS
	}
}
