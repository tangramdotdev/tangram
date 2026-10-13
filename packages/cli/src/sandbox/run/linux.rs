use std::{ffi::OsString, net::Ipv4Addr, path::PathBuf};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub as_pid_1: bool,

	#[arg(action = clap::ArgAction::Append, long = "bind", num_args = 2)]
	pub binds: Vec<PathBuf>,

	#[arg(long)]
	pub cgroup: Option<String>,

	#[arg(long)]
	pub cgroup_cpu: Option<u64>,

	#[arg(long)]
	pub cgroup_entered: bool,

	#[arg(long)]
	pub cgroup_fd: Option<i32>,

	#[arg(long)]
	pub cgroup_memory: Option<u64>,

	#[arg(long)]
	pub cgroup_memory_oom_group: bool,

	#[arg(long)]
	pub cgroup_memory_swap: Option<u64>,

	#[arg(long)]
	pub cgroup_pids: Option<u64>,

	#[arg(long)]
	pub cgroup_readonly: bool,

	#[arg(default_value = "/", long)]
	pub chdir: PathBuf,

	#[arg(long)]
	pub clearenv: bool,

	#[arg(allow_hyphen_values = true, trailing_var_arg = true)]
	pub command: Vec<OsString>,

	#[arg(action = clap::ArgAction::Append, long = "dev", num_args = 1)]
	pub devs: Vec<PathBuf>,

	#[arg(long)]
	pub die_with_parent: bool,

	#[arg(long)]
	pub filesystem_fd: Option<i32>,

	#[arg(long)]
	pub filesystem_inodes: Option<u64>,

	#[arg(long)]
	pub filesystem_path: Option<PathBuf>,

	#[arg(long)]
	pub filesystem_size: Option<u64>,

	#[arg(long)]
	pub fuse_fd: Option<i32>,

	#[arg(long)]
	pub fuse_path: Option<PathBuf>,

	#[arg(long)]
	pub gateway_ip: Option<Ipv4Addr>,

	#[arg(long, default_value_t = 0)]
	pub gid: u32,

	#[arg(long)]
	pub guest_ip: Option<Ipv4Addr>,

	#[arg(long)]
	pub hostname: Option<String>,

	#[arg(long, default_value_t = 0)]
	pub index: u64,

	#[arg(long)]
	pub network: Option<tangram_sandbox::run::linux::Network>,

	#[arg(long)]
	pub network_fd: Option<i32>,

	#[arg(long)]
	pub new_session: bool,

	#[arg(default_value_t = 0, long)]
	pub nice: u8,

	#[arg(action = clap::ArgAction::Append, long = "overlay-src", num_args = 1)]
	pub overlay_sources: Vec<PathBuf>,

	#[arg(action = clap::ArgAction::Append, long = "overlay", num_args = 3)]
	pub overlays: Vec<PathBuf>,

	#[arg(long, default_value = "kvm", value_parser = parse_platform)]
	pub platform: tangram_sandbox::Platform,

	#[arg(action = clap::ArgAction::Append, long = "proc", num_args = 1)]
	pub procs: Vec<PathBuf>,

	#[arg(long)]
	pub rlimit_nofile: Option<u64>,

	#[arg(action = clap::ArgAction::Append, long = "ro-bind", num_args = 2)]
	pub ro_binds: Vec<PathBuf>,

	#[arg(long)]
	pub runtime_path: Option<PathBuf>,

	#[arg(long)]
	pub seccomp: Option<tangram_sandbox::SeccompPolicy>,

	#[arg(action = clap::ArgAction::Append, long = "setenv", num_args = 2)]
	pub setenvs: Vec<String>,

	#[arg(action = clap::ArgAction::Append, long = "tmpfs", num_args = 1)]
	pub tmpfs: Vec<PathBuf>,

	#[arg(long, default_value_t = 0)]
	pub uid: u32,

	#[arg(long)]
	pub unshare_all: bool,

	#[arg(long)]
	pub user_namespace_fd: Option<i32>,
}

impl Args {
	pub(super) fn into_arg(
		self,
		backend: tangram_sandbox::Kind,
	) -> tangram_sandbox::run::linux::Arg {
		let binds = chunk_pairs(self.binds)
			.into_iter()
			.map(|[source, target]| tangram_sandbox::run::linux::Bind { source, target })
			.collect();
		let overlays = chunk_triples(self.overlays)
			.into_iter()
			.map(
				|[upperdir, workdir, target]| tangram_sandbox::run::linux::Overlay {
					target,
					upperdir,
					workdir,
				},
			)
			.collect();
		let ro_binds = chunk_pairs(self.ro_binds)
			.into_iter()
			.map(|[source, target]| tangram_sandbox::run::linux::Bind { source, target })
			.collect();
		let setenvs = chunk_pairs(self.setenvs)
			.into_iter()
			.map(|[key, value]| tangram_sandbox::run::linux::SetEnv { key, value })
			.collect();
		tangram_sandbox::run::linux::Arg {
			as_pid_1: self.as_pid_1,
			backend,
			binds,
			cgroup: self.cgroup,
			cgroup_cpu: self.cgroup_cpu,
			cgroup_entered: self.cgroup_entered,
			cgroup_fd: self.cgroup_fd,
			cgroup_memory: self.cgroup_memory,
			cgroup_memory_oom_group: self.cgroup_memory_oom_group,
			cgroup_memory_swap: self.cgroup_memory_swap,
			cgroup_pids: self.cgroup_pids,
			cgroup_readonly: self.cgroup_readonly,
			chdir: self.chdir,
			clearenv: self.clearenv,
			command: self.command,
			devs: self.devs,
			die_with_parent: self.die_with_parent,
			filesystem_fd: self.filesystem_fd,
			filesystem_inodes: self.filesystem_inodes,
			filesystem_mount_fd: None,
			filesystem_path: self.filesystem_path,
			filesystem_size: self.filesystem_size,
			fuse_fd: self.fuse_fd,
			fuse_path: self.fuse_path,
			gateway_ip: self.gateway_ip,
			gid: self.gid,
			guest_ip: self.guest_ip,
			hostname: self.hostname,
			index: self.index,
			network: self.network,
			network_fd: self.network_fd,
			new_session: self.new_session,
			nice: self.nice,
			overlay_sources: self.overlay_sources,
			overlays,
			platform: self.platform,
			procs: self.procs,
			rlimit_nofile: self.rlimit_nofile,
			ro_binds,
			runtime_path: self.runtime_path,
			seccomp: self.seccomp,
			setenvs,
			tmpfs: self.tmpfs,
			uid: self.uid,
			unshare_all: self.unshare_all,
			user_namespace_fd: self.user_namespace_fd,
		}
	}
}

fn chunk_pairs<T>(values: Vec<T>) -> Vec<[T; 2]> {
	let mut iter = values.into_iter();
	let mut output = Vec::new();
	while let Some(first) = iter.next() {
		output.push([first, iter.next().unwrap()]);
	}
	output
}

fn chunk_triples<T>(values: Vec<T>) -> Vec<[T; 3]> {
	let mut iter = values.into_iter();
	let mut output = Vec::new();
	while let Some(first) = iter.next() {
		output.push([first, iter.next().unwrap(), iter.next().unwrap()]);
	}
	output
}

fn parse_platform(value: &str) -> tangram_client::Result<tangram_sandbox::Platform> {
	serde_json::from_value(serde_json::Value::String(value.to_owned()))
		.map_err(|error| tangram_client::error!(!error, %value, "invalid gvisor platform"))
}
