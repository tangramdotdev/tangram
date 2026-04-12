use {
	crate::Cli,
	std::{ffi::OsString, path::PathBuf},
	tangram_client::prelude::*,
};

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
	pub cgroup_memory_oom_group: bool,

	#[arg(default_value = "/", long)]
	pub chdir: PathBuf,

	#[arg(allow_hyphen_values = true, required = true, trailing_var_arg = true)]
	pub command: Vec<OsString>,

	#[arg(action = clap::ArgAction::Append, long = "dev", num_args = 1)]
	pub devs: Vec<PathBuf>,

	#[arg(long)]
	pub die_with_parent: bool,

	#[arg(long)]
	pub gid: u32,

	#[arg(long)]
	pub hostname: Option<String>,

	#[arg(long)]
	pub new_session: bool,

	#[arg(action = clap::ArgAction::Append, long = "overlay-src", num_args = 1)]
	pub overlay_sources: Vec<PathBuf>,

	#[arg(action = clap::ArgAction::Append, long = "overlay", num_args = 3)]
	pub overlays: Vec<PathBuf>,

	#[arg(action = clap::ArgAction::Append, long = "proc", num_args = 1)]
	pub procs: Vec<PathBuf>,

	#[arg(action = clap::ArgAction::Append, long = "ro-bind", num_args = 2)]
	pub ro_binds: Vec<PathBuf>,

	#[arg(action = clap::ArgAction::Append, long = "setenv", num_args = 2)]
	pub setenvs: Vec<String>,

	#[arg(long)]
	pub share_net: bool,

	#[arg(action = clap::ArgAction::Append, long = "tmpfs", num_args = 1)]
	pub tmpfs: Vec<PathBuf>,

	#[arg(long)]
	pub uid: u32,

	#[arg(long)]
	pub unshare_all: bool,
}

impl Args {
	fn into_arg(self) -> tangram_sandbox::container::run::Arg {
		let binds = super::chunk_pairs(self.binds)
			.into_iter()
			.map(|[source, target]| tangram_sandbox::container::run::Bind { source, target })
			.collect();
		let overlays = super::chunk_triples(self.overlays)
			.into_iter()
			.map(
				|[upperdir, workdir, target]| tangram_sandbox::container::run::Overlay {
					target,
					upperdir,
					workdir,
				},
			)
			.collect();
		let ro_binds = super::chunk_pairs(self.ro_binds)
			.into_iter()
			.map(|[source, target]| tangram_sandbox::container::run::Bind { source, target })
			.collect();
		let setenvs = super::chunk_pairs(self.setenvs)
			.into_iter()
			.map(|[key, value]| tangram_sandbox::container::run::SetEnv { key, value })
			.collect();
		tangram_sandbox::container::run::Arg {
			as_pid_1: self.as_pid_1,
			binds,
			cgroup: self.cgroup,
			cgroup_memory_oom_group: self.cgroup_memory_oom_group,
			chdir: self.chdir,
			command: self.command,
			devs: self.devs,
			die_with_parent: self.die_with_parent,
			gid: self.gid,
			hostname: self.hostname,
			new_session: self.new_session,
			overlay_sources: self.overlay_sources,
			overlays,
			procs: self.procs,
			ro_binds,
			setenvs,
			share_net: self.share_net,
			tmpfs: self.tmpfs,
			uid: self.uid,
			unshare_all: self.unshare_all,
		}
	}
}

impl Cli {
	#[must_use]
	pub fn command_sandbox_container_run(args: Args) -> std::process::ExitCode {
		let arg = args.into_arg();
		match tangram_sandbox::container::run::run(&arg) {
			Ok(code) => code,
			Err(error) => {
				Cli::print_error_basic(tg::Referent::with_item(error));
				std::process::ExitCode::FAILURE
			},
		}
	}
}
