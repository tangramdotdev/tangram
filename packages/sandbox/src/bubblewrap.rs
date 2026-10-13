use {
	crate::run::linux::Arg,
	std::{
		io::Write as _,
		os::{fd::AsRawFd as _, unix::process::CommandExt as _},
		path::Path,
	},
	tangram_client::prelude::*,
};

pub(crate) fn exec(arg: &Arg, root: &Path) -> tg::Result<()> {
	// Launch the command in the prepared filesystem and network namespace.
	let path = arg.runtime_path.as_deref().unwrap_or(Path::new("bwrap"));
	let root_file = std::fs::File::open(root)
		.map_err(|error| tg::error!(!error, "failed to open the prepared bubblewrap root"))?;
	// SAFETY: The descriptor is live and must survive exec so bubblewrap can bind the prepared mount.
	if unsafe { libc::fcntl(root_file.as_raw_fd(), libc::F_SETFD, 0) } < 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to inherit the bubblewrap root"
		));
	}
	let mut command = std::process::Command::new(path);
	command
		.arg("--bind-fd")
		.arg(root_file.as_raw_fd().to_string())
		.arg("/")
		.arg("--unshare-user")
		.arg("--unshare-pid")
		.arg("--unshare-ipc")
		.arg("--uid")
		.arg(arg.uid.to_string())
		.arg("--gid")
		.arg(arg.gid.to_string())
		.arg("--cap-drop")
		.arg("ALL")
		.arg("--chdir")
		.arg(&arg.chdir);
	for target in &arg.devs {
		let source = root.join(
			target
				.strip_prefix("/")
				.map_err(|error| tg::error!(!error, "invalid bubblewrap device mount target"))?,
		);
		command.arg("--dev-bind").arg(source).arg(target);
	}
	for target in &arg.procs {
		command.arg("--proc").arg(target);
	}
	if arg.as_pid_1 {
		command.arg("--as-pid-1");
	}
	if arg.die_with_parent {
		command.arg("--die-with-parent");
	}
	if let Some(hostname) = &arg.hostname {
		command.arg("--unshare-uts").arg("--hostname").arg(hostname);
	}
	if arg.clearenv {
		command.arg("--clearenv");
	}
	for entry in &arg.setenvs {
		command.arg("--setenv").arg(&entry.key).arg(&entry.value);
	}

	// Pass the workload filter to bubblewrap before it drops setup privileges.
	let mut filter_file = None;
	if let Some(policy) = arg.seccomp {
		let filter = crate::builtin::seccomp::filter(policy)?;
		let path = root.parent().unwrap().join("seccomp.bpf");
		let mut file = std::fs::OpenOptions::new()
			.create_new(true)
			.read(true)
			.write(true)
			.open(&path)
			.map_err(|error| tg::error!(!error, "failed to create the bubblewrap seccomp file"))?;
		// SAFETY: sock_filter is the initialized, padding-free Linux cBPF instruction ABI.
		let bytes = unsafe {
			std::slice::from_raw_parts(
				filter.as_ptr().cast(),
				std::mem::size_of_val(filter.as_slice()),
			)
		};
		file.write_all(bytes)
			.map_err(|error| tg::error!(!error, "failed to write the bubblewrap seccomp filter"))?;
		use std::io::Seek as _;
		file.rewind().map_err(|error| {
			tg::error!(!error, "failed to rewind the bubblewrap seccomp filter")
		})?;
		// SAFETY: The descriptor is live and must survive exec so bubblewrap can read the filter.
		if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFD, 0) } < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to inherit the bubblewrap seccomp filter"
			));
		}
		command.arg("--seccomp").arg(file.as_raw_fd().to_string());
		filter_file = Some(file);
	}
	command.arg("--").args(&arg.command);
	let error = command.exec();
	drop(filter_file);
	Err(tg::error!(!error, "failed to execute bubblewrap"))
}
