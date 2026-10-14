use {
	crate::linux::run::Prepared,
	std::{os::fd::IntoRawFd as _, path::Path, process::Command},
	tangram_client::prelude::*,
};

pub(crate) fn command(prepared: &Prepared, path: &Path, builtin: bool) -> tg::Result<Command> {
	// Create the native command.
	let arg = prepared.arg;
	let mut command = Command::new(path);

	// Bind the prepared root.
	{
		let root = prepared.root.unwrap_or(Path::new("/"));
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

	// Select the user namespace.
	if builtin {
		let namespace = std::fs::File::open("/proc/self/ns/user")
			.map_err(|error| tg::error!(!error, "failed to open the prepared user namespace"))?;
		let fd = namespace.into_raw_fd();
		command.arg("--userns").arg(fd.to_string());
	} else {
		command.arg("--unshare-user");
	}

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
		.arg(&arg.process.directory)
		.arg("--clearenv");
	if arg.as_pid_1 {
		command.arg("--as-pid-1");
	}
	if let Some(hostname) = &arg.process.hostname {
		command.arg("--unshare-uts").arg("--hostname").arg(hostname);
	}

	// Set the environment and seccomp filter.
	for (key, value) in &arg.process.environment {
		command.arg("--setenv").arg(key).arg(value);
	}
	if let Some(fd) = prepared.seccomp_fd {
		command.arg("--seccomp").arg(fd.to_string());
	}

	// Append the workload arguments.
	command.arg("--").args(&arg.process.arguments);

	Ok(command)
}
