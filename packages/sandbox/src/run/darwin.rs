use {
	std::{ffi::OsString, path::PathBuf, process::ExitCode},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub struct Arg {
	pub as_pid_1: bool,
	pub binds: Vec<Bind>,
	pub cgroup: Option<String>,
	pub cgroup_memory_oom_group: bool,
	pub chdir: PathBuf,
	pub command: Vec<OsString>,
	pub devs: Vec<PathBuf>,
	pub die_with_parent: bool,
	pub gid: libc::gid_t,
	pub hostname: Option<String>,
	pub new_session: bool,
	pub overlay_sources: Vec<PathBuf>,
	pub overlays: Vec<Overlay>,
	pub procs: Vec<PathBuf>,
	pub ro_binds: Vec<Bind>,
	pub setenvs: Vec<SetEnv>,
	pub share_net: bool,
	pub tmpfs: Vec<PathBuf>,
	pub uid: libc::uid_t,
	pub unshare_all: bool,
}

#[derive(Clone, Debug)]
pub struct Bind {
	pub source: PathBuf,
	pub target: PathBuf,
}

#[derive(Clone, Debug)]
pub struct Overlay {
	pub target: PathBuf,
	pub upperdir: PathBuf,
	pub workdir: PathBuf,
}

#[derive(Clone, Debug)]
pub struct SetEnv {
	pub key: String,
	pub value: String,
}

pub fn run(_arg: &Arg) -> tg::Result<ExitCode> {
	Err(tg::error!(
		"the sandbox run command is only supported on Linux"
	))
}
