use {
	crate::Cli,
	std::{net::Ipv4Addr, path::PathBuf},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub artifacts_path: PathBuf,

	#[arg(long)]
	pub cpu: Option<u64>,

	#[arg(long)]
	pub host_ip: Ipv4Addr,

	#[arg(long)]
	pub hostname: Option<String>,
	
	#[arg(long)]
	pub guest_ip: Ipv4Addr,

	#[arg(long)]
	pub id: tg::sandbox::Id,

	#[arg(long)]
	pub kernel_path: PathBuf,

	#[arg(long)]
	pub memory: Option<u64>,

	#[arg(action = clap::ArgAction::Append, long = "mount", num_args = 1)]
	pub mounts: Vec<tg::sandbox::Mount>,

	#[arg(long)]
	pub network: bool,

	#[arg(long)]
	pub path: PathBuf,

	#[arg(long)]
	pub rootfs_path: PathBuf,

	#[arg(long)]
	pub tangram_path: PathBuf,

	#[arg(long)]
	pub url: tangram_uri::Uri,

	#[arg(long)]
	pub user: Option<String>,
}

impl Cli {
	#[must_use]
	pub fn command_sandbox_vm_run(args: Args) -> std::process::ExitCode {
		let arg = tangram_sandbox::vm::run::Arg {
			artifacts_path: args.artifacts_path,
			cpu: args.cpu,
			host_ip: args.host_ip,
			guest_ip: args.guest_ip,
			hostname: args.hostname,
			id: args.id,
			kernel_path: args.kernel_path,
			memory: args.memory,
			mounts: args.mounts,
			network: args.network,
			path: args.path,
			rootfs_path: args.rootfs_path,
			tangram_path: args.tangram_path,
			url: args.url,
			user: args.user,
		};
		let result = tangram_sandbox::vm::run::run(&arg);
		match result {
			Ok(code) => code,
			Err(error) => {
				Cli::print_error_basic(tg::Referent::with_item(error));
				std::process::ExitCode::FAILURE
			},
		}
	}
}
