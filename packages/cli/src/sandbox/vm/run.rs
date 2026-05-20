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

	#[arg(long, hide = true, value_name = "DIR")]
	pub create_snapshot: Option<PathBuf>,

	#[arg(long)]
	pub cpu: Option<u64>,

	/// The size of the artifacts DAX window in bytes, or `0` to disable DAX. Must be a power of
	/// two because the window is exposed as a PCI BAR.
	#[arg(long, default_value_t = 512 * 1024 * 1024, hide = true)]
	pub dax: u64,

	#[arg(action = clap::ArgAction::Append, long = "dns", num_args = 1)]
	pub dns: Vec<Ipv4Addr>,

	#[arg(long)]
	pub firewall: tangram_sandbox::Firewall,

	#[arg(long)]
	pub guest_ip: Option<Ipv4Addr>,

	#[arg(long)]
	pub host_ip: Option<Ipv4Addr>,

	#[arg(long)]
	pub hostname: Option<String>,

	#[arg(long)]
	pub id: tg::sandbox::Id,

	#[arg(long)]
	pub kernel_path: PathBuf,

	#[arg(long)]
	pub memory: Option<u64>,

	#[arg(long, default_value_t = 8, hide = true)]
	pub max_cpu: u64,

	#[arg(long, default_value_t = 8 * 1024 * 1024 * 1024, hide = true)]
	pub max_memory: u64,

	#[arg(action = clap::ArgAction::Append, long = "mount", num_args = 1)]
	pub mounts: Vec<tg::sandbox::Mount>,

	#[arg(long)]
	pub network: Option<tangram_sandbox::vm::run::NetworkKind>,

	#[arg(long)]
	pub path: PathBuf,

	#[arg(action = clap::ArgAction::Append, long = "port", num_args = 1)]
	pub ports: Vec<tg::sandbox::Port>,

	#[arg(long, hide = true, value_name = "PATH")]
	pub image_path: PathBuf,

	#[arg(long)]
	pub rootfs_path: PathBuf,

	#[arg(long)]
	pub snapshot: Option<PathBuf>,

	#[arg(long, default_value_t = 1, hide = true)]
	pub snapshot_cpu: u64,

	#[arg(long, default_value_t = 512 * 1024 * 1024, hide = true)]
	pub snapshot_memory: u64,

	#[arg(long)]
	pub tangram_path: PathBuf,

	#[arg(long)]
	pub url: tangram_uri::Uri,

	#[arg(long)]
	pub user: Option<String>,
}

impl Cli {
	pub fn command_sandbox_vm_run(args: Args) -> tg::Result<std::process::ExitCode> {
		let arg = tangram_sandbox::vm::run::Arg {
			artifacts_path: args.artifacts_path,
			create_snapshot: args.create_snapshot,
			cpu: args.cpu,
			dax: (args.dax != 0).then_some(args.dax),
			dns: args.dns,
			firewall: args.firewall,
			guest_ip: args.guest_ip,
			host_ip: args.host_ip,
			hostname: args.hostname,
			id: args.id,
			kernel_path: args.kernel_path,
			max_cpu: args.max_cpu,
			max_memory: args.max_memory,
			memory: args.memory,
			mounts: args.mounts,
			network: args.network,
			path: args.path,
			ports: args.ports,
			image_path: args.image_path,
			rootfs_path: args.rootfs_path,
			snapshot: args.snapshot,
			snapshot_cpu: args.snapshot_cpu,
			snapshot_memory: args.snapshot_memory,
			tangram_path: args.tangram_path,
			url: args.url,
			user: args.user,
		};
		tangram_sandbox::vm::run::run(&arg)
	}
}
