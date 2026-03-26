use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

/// Run a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub artifacts_path: PathBuf,

	#[command(flatten)]
	pub options: super::Options,

	#[arg(long)]
	pub path: PathBuf,

	#[arg(long)]
	pub ready_fd: Option<i32>,

	#[arg(long)]
	pub rootfs_path: PathBuf,

	#[arg(long)]
	pub tangram_path: PathBuf,
}

impl Cli {
	#[must_use]
	pub fn command_sandbox_run(args: Args) -> std::process::ExitCode {
		let config = tangram_sandbox::RunArg {
			artifacts_path: args.artifacts_path,
			hostname: args.options.hostname,
			mounts: args.options.mounts,
			network: args.options.network.get(),
			path: args.path,
			rootfs_path: args.rootfs_path,
			tangram_path: args.tangram_path,
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
