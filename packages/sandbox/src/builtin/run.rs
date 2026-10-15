use {
	crate::{builtin::util::CStringVec, linux, mount},
	std::{
		ffi::CString,
		os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Arg {
	pub as_pid_1: bool,
	pub capabilities: Vec<crate::Capability>,
	pub gid: u32,
	pub hostname: Option<String>,
	pub root: Option<PathBuf>,
	pub seccomp: Option<crate::SeccompPolicy>,
	pub uid: u32,
}

pub fn run(command: &crate::run::Command, arg: &Arg) -> tg::Result<u8> {
	// Configure the process lifetime and isolation namespaces.
	crate::util::set_parent_death_signal(libc::SIGKILL)?;
	linux::unshare(
		namespace_flags(arg),
		"failed to unshare the builtin namespaces",
	)?;
	if let Some(hostname) = &arg.hostname {
		linux::set_hostname(hostname)?;
	}

	// Bind the prepared root through an owned descriptor.
	let root_path = arg.root.as_deref().unwrap_or(Path::new("/"));
	let root_descriptor = std::fs::File::open(root_path)
		.map_err(|error| tg::error!(!error, "failed to open the prepared root"))?;
	let mounts = mounts(&root_descriptor);

	// Create the builtin mount root.
	let root = tangram_util::fs::Temp::new()
		.map_err(|error| tg::error!(!error, "failed to create the builtin root"))?;
	let root_path = root.path().join("root");
	std::fs::create_dir_all(&root_path)
		.map_err(|error| tg::error!(!error, "failed to create the builtin root"))?;

	// Fork the workload or its PID 1 reaper.
	let parent = linux::open_pidfd()?;
	let child = linux::fork()?;
	if child == 0 {
		let result = if arg.as_pid_1 {
			child_main(command, arg, &mounts, &root_path, &parent)
		} else {
			reap(command, arg, &mounts, &root_path, &parent)
		};
		if let Err(error) = result {
			eprintln!("{}", error.trace());
			std::process::exit(105);
		}
		std::process::exit(0);
	}

	// Wait for the workload while retaining its root resources.
	let status = linux::wait_for_child(child)?;

	Ok(status)
}

pub(crate) fn mount_fuse(fd: i32, path: &Path) -> tg::Result<()> {
	if fd < 0 {
		return Err(tg::error!("the FUSE mount requires a valid descriptor"));
	}

	// SAFETY: The control descriptor is transferred by the launcher and exclusively owned here.
	let fd = unsafe { OwnedFd::from_raw_fd(fd) };
	tangram_vfs::fuse::mount_dev_fuse(&fd, path)
		.map_err(|error| tg::error!(!error, "failed to mount the FUSE filesystem"))?;

	Ok(())
}

fn namespace_flags(arg: &Arg) -> libc::c_int {
	let mut flags = libc::CLONE_NEWIPC | libc::CLONE_NEWPID;
	if arg.hostname.is_some() {
		flags |= libc::CLONE_NEWUTS;
	}
	flags
}

fn mounts(root: &std::fs::File) -> mount::Arg {
	let bind = mount::Bind {
		devices: false,
		fd: Some(root.as_raw_fd()),
		source: PathBuf::new(),
		target: "/".into(),
	};
	let mut mounts = mount::Arg {
		devs: vec!["/dev".into()],
		nodev: true,
		nosuid: true,
		procs: vec!["/proc".into()],
		..Default::default()
	};
	mounts.binds.push(bind);
	mounts
}

fn reap(
	command: &crate::run::Command,
	arg: &Arg,
	mounts: &mount::Arg,
	root: &Path,
	parent: &OwnedFd,
) -> tg::Result<()> {
	// Protect the reaper's host filesystem and inherited descriptors from the workload.
	// SAFETY: prctl takes scalar arguments and the operation does not retain pointers.
	if unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) } != 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to protect the builtin reaper"
		));
	}

	// Fork the command under the protected reaper.
	crate::util::set_parent_death_signal(libc::SIGKILL)?;
	let child = linux::fork()?;
	if child == 0 {
		child_main(command, arg, mounts, root, parent)?;
		std::process::exit(0);
	}

	// Reap the sandbox processes until the command exits.
	loop {
		let mut status = 0;

		// SAFETY: The status pointer is valid for the duration of the waitpid call.
		let reaped = unsafe { libc::waitpid(-1, &raw mut status, 0) };
		if reaped < 0 {
			let error = std::io::Error::last_os_error();
			if error.kind() == std::io::ErrorKind::Interrupted {
				continue;
			}
			return Err(tg::error!(!error, "failed to reap the builtin workload"));
		}

		if reaped == child {
			let code = if libc::WIFEXITED(status) {
				libc::WEXITSTATUS(status)
			} else {
				(128 + libc::WTERMSIG(status)).min(255)
			};
			std::process::exit(code);
		}
	}
}

fn child_main(
	command: &crate::run::Command,
	arg: &Arg,
	mounts: &mount::Arg,
	root: &Path,
	parent: &OwnedFd,
) -> tg::Result<()> {
	// Configure the namespaces and process lifetime.
	linux::unshare(
		libc::CLONE_NEWNS,
		"failed to create the builtin mount namespace",
	)?;
	crate::util::set_parent_death_signal(libc::SIGKILL)?;
	linux::start_session()?;

	// Apply the typed mounts and enter the root filesystem.
	mount::apply(mounts, Some(root))?;
	mount::pivot_root_into(root)?;
	mount::change_directory(&command.cwd)?;

	// Prepare the seccomp filter directly from its policy.
	let filter = arg.seccomp.map(crate::seccomp::filter).transpose()?;

	// Drop privileges and close the inherited descriptors.
	linux::set_capabilities(&arg.capabilities, arg.uid == 0)?;
	linux::set_securebits()?;
	linux::restrict_capabilities(&arg.capabilities)?;
	linux::setresgid(arg.gid)?;
	linux::setresuid(arg.uid)?;
	crate::util::set_parent_death_signal(libc::SIGKILL)?;
	linux::validate_parent(parent)?;
	linux::set_no_new_privs()?;
	linux::close_non_std_fds()?;

	// Install the seccomp filter.
	if let Some(filter) = filter {
		crate::seccomp::install_filter(filter)?;
	}

	// Execute the command in the sandbox.
	exec(command)?;

	Ok(())
}

fn exec(command: &crate::run::Command) -> tg::Result<()> {
	// Encode the command arguments.
	let executable = command
		.args
		.first()
		.ok_or_else(|| tg::error!("a command is required"))?;
	let executable = CString::new(executable.as_bytes())
		.map_err(|error| tg::error!(!error, "failed to encode the executable"))?;
	let argv = command
		.args
		.iter()
		.map(|value| {
			CString::new(value.as_bytes())
				.map_err(|error| tg::error!(!error, "failed to encode an argument"))
		})
		.collect::<tg::Result<CStringVec>>()?;

	// Encode the command environment.
	let envp = command
		.env
		.iter()
		.map(|(key, value)| envstring(key, value))
		.collect::<tg::Result<CStringVec>>()?;

	// Execute the command.
	// SAFETY: The terminated strings and pointer arrays remain live until exec replaces the process.
	unsafe {
		libc::execvpe(
			executable.as_ptr(),
			argv.as_ptr().cast(),
			envp.as_ptr().cast(),
		);
	}

	Err(tg::error!(
		source = std::io::Error::last_os_error(),
		"failed to execute the command"
	))
}

fn envstring(key: &str, value: &str) -> tg::Result<CString> {
	let mut bytes = key.as_bytes().to_vec();
	bytes.push(b'=');
	bytes.extend_from_slice(value.as_bytes());
	let value = CString::new(bytes)
		.map_err(|error| tg::error!(!error, "failed to encode an environment entry"))?;
	Ok(value)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn hostname_selects_uts_namespace() {
		let arg = Arg {
			hostname: Some("tangram-test".into()),
			..Default::default()
		};
		assert_ne!(namespace_flags(&arg) & libc::CLONE_NEWUTS, 0);
		assert_eq!(namespace_flags(&Arg::default()) & libc::CLONE_NEWUTS, 0);
	}

	#[test]
	fn tangram_resource_options_are_not_backend_inputs() {
		for option in [
			"cgroup",
			"cgroup_fd",
			"filesystem_fd",
			"index",
			"network_fd",
			"nice",
			"rlimit_nofile",
		] {
			let value = serde_json::json!({ (option): 0 });
			assert!(serde_json::from_value::<Arg>(value).is_err());
		}
	}
}
