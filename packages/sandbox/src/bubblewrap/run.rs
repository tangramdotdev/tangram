use {
	std::{
		os::{fd::IntoRawFd as _, unix::process::CommandExt as _},
		path::{Path, PathBuf},
		process::Command,
	},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Arg {
	pub as_pid_1: bool,
	pub gid: u32,
	pub hostname: Option<String>,
	pub path: PathBuf,
	pub root: Option<PathBuf>,
	pub seccomp: Option<crate::SeccompPolicy>,
	pub uid: u32,
}

pub fn run(command_arg: &crate::run::Command, arg: &Arg) -> tg::Result<u8> {
	let mut command = command(command_arg, arg)?;
	let error = command.exec();
	Err(tg::error!(!error, "failed to execute bubblewrap"))
}

impl Default for Arg {
	fn default() -> Self {
		Self {
			as_pid_1: false,
			gid: 0,
			hostname: None,
			path: "bwrap".into(),
			root: None,
			seccomp: None,
			uid: 0,
		}
	}
}

fn command(command_arg: &crate::run::Command, arg: &Arg) -> tg::Result<Command> {
	// Create the native command.
	let mut command = Command::new(&arg.path);

	// Bind the prepared root.
	{
		let root = arg.root.as_deref().unwrap_or(Path::new("/"));
		let root = std::fs::File::open(root)
			.map_err(|error| tg::error!(!error, "failed to open the prepared root"))?;

		// Transfer the root descriptor to the native launcher's --bind-fd argument.
		let fd = root.into_raw_fd();

		// SAFETY: The descriptor is newly opened and exclusively owned by this launch.
		if unsafe { libc::fcntl(fd, libc::F_SETFD, 0) } < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to inherit the prepared root"
			));
		}
		command
			.arg("--bind-fd")
			.arg(fd.to_string())
			.arg("/")
			.arg("--dev")
			.arg("/dev")
			.arg("--proc")
			.arg("/proc");
	}

	// Create the user namespace for the external runtime.
	command.arg("--unshare-user");

	// Set the isolation and process options.
	command
		.arg("--unshare-pid")
		.arg("--unshare-ipc")
		.arg("--die-with-parent")
		.arg("--new-session")
		.arg("--uid")
		.arg(arg.uid.to_string())
		.arg("--gid")
		.arg(arg.gid.to_string())
		.arg("--cap-drop")
		.arg("ALL")
		.arg("--chdir")
		.arg(&command_arg.cwd)
		.arg("--clearenv");
	if arg.as_pid_1 {
		command.arg("--as-pid-1");
	}
	if let Some(hostname) = &arg.hostname {
		command.arg("--unshare-uts").arg("--hostname").arg(hostname);
	}

	// Set the environment and seccomp filter.
	for (key, value) in &command_arg.env {
		command.arg("--setenv").arg(key).arg(value);
	}
	if let Some(fd) = arg.seccomp.map(crate::seccomp::descriptor).transpose()? {
		command.arg("--seccomp").arg(fd.to_string());
	}

	// Append the workload arguments.
	command.arg("--").args(&command_arg.args);

	Ok(command)
}
