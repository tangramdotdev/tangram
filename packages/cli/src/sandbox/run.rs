use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

/// Run a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub id: tg::sandbox::Id,

	#[command(flatten)]
	pub options: super::Options,

	#[arg(long)]
	pub path: PathBuf,

	#[arg(long)]
	pub ready_fd: Option<i32>,

	#[arg(long)]
	pub server_path: PathBuf,

	#[arg(long)]
	pub socket_path: PathBuf,
}

impl Cli {
	#[must_use]
	pub fn command_sandbox_run(args: Args) -> std::process::ExitCode {
		let options = tangram_sandbox::Config {
			hostname: args.options.hostname,
			id: args.id,
			mounts: args.options.mounts,
			network: args.options.network.get(),
			path: args.path,
			ready_fd: args.ready_fd,
			server_path: args.server_path,
			socket_path: args.socket_path,
			user: args.options.user,
		};
		let result = tangram_sandbox::run(options);
		if let Err(error) = result {
			Cli::print_error_basic(tg::Referent::with_item(error));
			return std::process::ExitCode::FAILURE;
		}
		std::process::ExitCode::SUCCESS
	}
}
