use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

/// Run a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub artifacts_path: PathBuf,

	#[arg(long)]
	pub listen_path: PathBuf,

	#[command(flatten)]
	pub options: super::Options,

	#[arg(long)]
	pub output_path: PathBuf,

	#[arg(long)]
	pub path: PathBuf,

	#[arg(long)]
	pub ready_fd: Option<i32>,

	#[arg(long)]
	pub root_path: PathBuf,

	#[arg(long)]
	pub server_path: PathBuf,

	#[arg(long)]
	pub socket_path: PathBuf,

	#[arg(long)]
	pub tangram_path: PathBuf,

	#[arg(long)]
	pub temp_path: PathBuf,
}

impl Cli {
	#[must_use]
	pub fn command_sandbox_run(args: Args) -> std::process::ExitCode {
		let config = tangram_sandbox::Config {
			artifacts_path: args.artifacts_path,
			hostname: args.options.hostname,
			listen_path: args.listen_path,
			mounts: args.options.mounts,
			network: args.options.network.get(),
			output_path: args.output_path,
			root_path: args.root_path,
			socket_path: args.socket_path,
			tangram_path: args.tangram_path,
			scratch_path: args.temp_path,
			user: args.options.user,
		};
		let result = tangram_sandbox::run(&config, args.ready_fd);
		if let Err(error) = result {
			Cli::print_error_basic(tg::Referent::with_item(error));
			return std::process::ExitCode::FAILURE;
		}
		std::process::ExitCode::SUCCESS
	}
}
