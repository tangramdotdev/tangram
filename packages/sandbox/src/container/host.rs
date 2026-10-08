use {
	std::{
		ffi::CString,
		io::Read as _,
		os::fd::{AsRawFd as _, FromRawFd as _},
		path::Path,
	},
	tangram_client::prelude::*,
};

const AT_RECURSIVE: libc::c_uint = 0x8000;
const MOUNT_ATTR_NODEV: u64 = 0x0000_0004;

#[derive(Debug)]
struct NamespaceProbeError {
	error: std::io::Error,
	stage: libc::c_int,
}

pub fn validate() -> tg::Result<()> {
	validate_user_namespaces()?;
	validate_cgroup_v2(Path::new("/sys/fs/cgroup"), Path::new("/proc/self/cgroup"))?;
	validate_seccomp()?;
	validate_mount_syscalls()?;

	Ok(())
}

fn validate_user_namespaces() -> tg::Result<()> {
	let path = Path::new("/proc/sys/user/max_user_namespaces");
	let value = std::fs::read_to_string(path).map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to read the user namespace limit; enable user namespaces for hardened container isolation"
		)
	})?;
	let value = value.trim().parse::<u64>().map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to parse the user namespace limit"
		)
	})?;
	if value == 0 {
		return Err(tg::error!(
			path = %path.display(),
			"user namespaces are disabled; set user.max_user_namespaces to a positive value"
		));
	}
	// SAFETY: geteuid has no arguments or memory safety requirements.
	if unsafe { libc::geteuid() } != 0 {
		let path = Path::new("/proc/sys/kernel/unprivileged_userns_clone");
		match std::fs::read_to_string(path) {
			Ok(value) if value.trim() == "0" => {
				return Err(tg::error!(
					path = %path.display(),
					"unprivileged user namespaces are disabled; enable kernel.unprivileged_userns_clone or run the runner with the required privilege"
				));
			},
			Ok(_) => {},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
			Err(error) => {
				return Err(tg::error!(
					!error,
					path = %path.display(),
					"failed to read the unprivileged user namespace setting"
				));
			},
		}
	}
	probe_container_namespaces().map_err(|error| {
		let stage = namespace_probe_stage(error.stage);
		tg::error!(
			source = error.error,
			%stage,
			"the hardened container namespace probe failed during {stage}; ensure user and mount namespaces and modern mount syscalls are allowed by the kernel, seccomp, and the host security policy"
		)
	})?;

	Ok(())
}

fn probe_container_namespaces() -> Result<(), NamespaceProbeError> {
	// SAFETY: getuid and getgid have no arguments or memory safety requirements.
	let uid = unsafe { libc::getuid() };
	let gid = unsafe { libc::getgid() };
	let uid_map = CString::new(format!("0 {uid} 1\n")).unwrap();
	let gid_map = CString::new(format!("0 {gid} 1\n")).unwrap();
	let mut pipe = [0; 2];
	// SAFETY: The pipe array is valid for both returned descriptors.
	if unsafe { libc::pipe2(pipe.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
		return Err(NamespaceProbeError {
			error: std::io::Error::last_os_error(),
			stage: 0,
		});
	}
	// SAFETY: fork duplicates the process and returns in both branches.
	let pid = unsafe { libc::fork() };
	if pid < 0 {
		let error = std::io::Error::last_os_error();
		// SAFETY: Both descriptors were created by pipe2 and remain owned here.
		unsafe {
			libc::close(pipe[0]);
			libc::close(pipe[1]);
		}
		return Err(NamespaceProbeError { error, stage: 0 });
	}
	if pid == 0 {
		// SAFETY: The child only calls async-signal-safe functions before exiting.
		unsafe {
			libc::close(pipe[0]);
			if libc::unshare(libc::CLONE_NEWUSER) != 0 {
				probe_child_fail(pipe[1], 1);
			}
			if write_probe_file(c"/proc/self/uid_map", uid_map.as_bytes_with_nul()) != 0 {
				probe_child_fail(pipe[1], 2);
			}
			if write_probe_file(c"/proc/self/setgroups", b"deny\0") != 0 {
				probe_child_fail(pipe[1], 3);
			}
			if write_probe_file(c"/proc/self/gid_map", gid_map.as_bytes_with_nul()) != 0 {
				probe_child_fail(pipe[1], 4);
			}
			if libc::unshare(libc::CLONE_NEWNS) != 0 {
				probe_child_fail(pipe[1], 5);
			}
			if libc::mount(
				std::ptr::null(),
				c"/".as_ptr(),
				std::ptr::null(),
				libc::MS_REC | libc::MS_PRIVATE,
				std::ptr::null(),
			) != 0
			{
				probe_child_fail(pipe[1], 6);
			}
			let flags = libc::OPEN_TREE_CLONE | libc::OPEN_TREE_CLOEXEC | AT_RECURSIVE;
			let mount = libc::syscall(libc::SYS_open_tree, libc::AT_FDCWD, c"/tmp".as_ptr(), flags);
			if mount < 0 {
				probe_child_fail(pipe[1], 7);
			}
			let attributes = [MOUNT_ATTR_NODEV, 0, 0, 0];
			if libc::syscall(
				libc::SYS_mount_setattr,
				mount,
				c"".as_ptr(),
				u32::try_from(libc::AT_EMPTY_PATH).unwrap() | AT_RECURSIVE,
				attributes.as_ptr(),
				std::mem::size_of_val(&attributes),
			) != 0
			{
				probe_child_fail(pipe[1], 8);
			}
			if libc::syscall(
				libc::SYS_move_mount,
				mount,
				c"".as_ptr(),
				libc::AT_FDCWD,
				c"/tmp".as_ptr(),
				libc::MOVE_MOUNT_F_EMPTY_PATH,
			) != 0
			{
				probe_child_fail(pipe[1], 9);
			}
			libc::close(mount.try_into().unwrap());
			libc::_exit(0);
		}
	}
	// SAFETY: The parent owns the read end and no longer needs the write end.
	unsafe { libc::close(pipe[1]) };
	// SAFETY: The read descriptor is uniquely owned by the parent.
	let mut reader = unsafe { std::fs::File::from_raw_fd(pipe[0]) };
	let mut bytes = [0; std::mem::size_of::<[libc::c_int; 2]>()];
	let mut length = 0;
	while length < bytes.len() {
		match reader.read(&mut bytes[length..]) {
			Ok(0) => break,
			Ok(value) => length += value,
			Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {},
			Err(error) => return Err(NamespaceProbeError { error, stage: 0 }),
		}
	}
	let mut status = 0;
	loop {
		// SAFETY: The pid identifies the child created above and status is writable.
		let result = unsafe { libc::waitpid(pid, &raw mut status, 0) };
		if result >= 0 {
			break;
		}
		let error = std::io::Error::last_os_error();
		if error.kind() != std::io::ErrorKind::Interrupted {
			return Err(NamespaceProbeError { error, stage: 0 });
		}
	}
	if libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0 {
		return Ok(());
	}
	let (stage, error) = if length == bytes.len() {
		let stage = libc::c_int::from_ne_bytes(bytes[..4].try_into().unwrap());
		let errno = libc::c_int::from_ne_bytes(bytes[4..].try_into().unwrap());
		(stage, std::io::Error::from_raw_os_error(errno))
	} else {
		(
			0,
			std::io::Error::other("the container namespace probe child failed"),
		)
	};

	Err(NamespaceProbeError { error, stage })
}

unsafe fn probe_child_fail(pipe: libc::c_int, stage: libc::c_int) -> ! {
	let error = unsafe { *libc::__errno_location() };
	let values = [stage, error];
	unsafe {
		libc::write(pipe, values.as_ptr().cast(), std::mem::size_of_val(&values));
		libc::_exit(1);
	}
}

fn namespace_probe_stage(stage: libc::c_int) -> &'static str {
	match stage {
		1 => "creating a user namespace",
		2 => "mapping the user ID",
		3 => "disabling setgroups",
		4 => "mapping the group ID",
		5 => "creating a mount namespace",
		6 => "making mounts private",
		7 => "cloning a detached mount with open_tree",
		8 => "setting recursive mount attributes with mount_setattr",
		9 => "attaching a detached mount with move_mount",
		_ => "starting the namespace probe",
	}
}

unsafe fn write_probe_file(path: &std::ffi::CStr, bytes: &[u8]) -> libc::c_int {
	let fd = unsafe { libc::open(path.as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC) };
	if fd < 0 {
		return -1;
	}
	let length = bytes.len().saturating_sub(1);
	let result = unsafe { libc::write(fd, bytes.as_ptr().cast(), length) };
	let error = unsafe { *libc::__errno_location() };
	unsafe { libc::close(fd) };
	if result == isize::try_from(length).unwrap() {
		0
	} else {
		unsafe { *libc::__errno_location() = error };
		-1
	}
}

fn validate_cgroup_v2(root: &Path, proc_self_cgroup: &Path) -> tg::Result<()> {
	let current = std::fs::read_to_string(proc_self_cgroup).map_err(|error| {
		tg::error!(
			!error,
			path = %proc_self_cgroup.display(),
			"failed to read the current cgroup"
		)
	})?;
	let current = parse_unified_cgroup(&current)?;
	let current = root.join(current.trim_start_matches('/'));
	let controllers_path = current.join("cgroup.subtree_control");
	let controllers = std::fs::read_to_string(&controllers_path).map_err(|error| {
		tg::error!(
			!error,
			path = %controllers_path.display(),
			"cgroup v2 is unavailable; run the hardened runner in a delegated cgroup v2 hierarchy"
		)
	})?;
	validate_controllers(&controllers).map_err(|error| {
		tg::error!(
			!error,
			path = %controllers_path.display(),
			"the hardened runner requires delegated cpu, memory, and pids cgroup v2 controllers"
		)
	})?;
	for name in [
		"cgroup.events",
		"cgroup.kill",
		"cgroup.procs",
		"cpu.max",
		"memory.max",
		"memory.oom.group",
		"memory.swap.max",
		"pids.max",
	] {
		let path = current.join(name);
		if !path.exists() {
			return Err(tg::error!(
				path = %path.display(),
				"the hardened runner requires the cgroup v2 {name} feature"
			));
		}
	}
	let directory = std::fs::File::open(&current).map_err(
		|error| tg::error!(!error, path = %current.display(), "failed to open the current cgroup"),
	)?;
	rustix::fs::accessat(
		&directory,
		".",
		rustix::fs::Access::WRITE_OK | rustix::fs::Access::EXEC_OK,
		rustix::fs::AtFlags::EACCESS,
	)
	.map_err(|error| {
		tg::error!(
			!error,
			path = %current.display(),
			"the hardened runner's cgroup is not delegated for creating sandbox cgroups"
		)
	})?;

	Ok(())
}

fn parse_unified_cgroup(contents: &str) -> tg::Result<&str> {
	contents
		.lines()
		.find_map(|line| line.strip_prefix("0::"))
		.ok_or_else(|| tg::error!("the process is not running in a unified cgroup v2 hierarchy"))
}

fn validate_controllers(contents: &str) -> tg::Result<()> {
	let controllers = contents.split_ascii_whitespace().collect::<Vec<_>>();
	let missing = ["cpu", "memory", "pids"]
		.into_iter()
		.filter(|required| !controllers.contains(required))
		.collect::<Vec<_>>();
	if !missing.is_empty() {
		return Err(tg::error!(
			missing = %missing.join(", "),
			"required cgroup controllers are not enabled"
		));
	}

	Ok(())
}

fn validate_seccomp() -> tg::Result<()> {
	let mut action = libc::SECCOMP_RET_ERRNO;
	// SAFETY: The kernel only reads and may normalize the action value.
	let result = unsafe {
		libc::syscall(
			libc::SYS_seccomp,
			libc::SECCOMP_GET_ACTION_AVAIL,
			0,
			&raw mut action,
		)
	};
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(
			!error,
			"seccomp filtering is unavailable; use a kernel with seccomp filter support and allow the seccomp syscall"
		));
	}

	Ok(())
}

fn validate_mount_syscalls() -> tg::Result<()> {
	probe_openat2()?;

	Ok(())
}

fn probe_openat2() -> tg::Result<()> {
	let path = CString::new(".").unwrap();
	// SAFETY: The open_how type is valid when zero initialized.
	let mut how: libc::open_how = unsafe { std::mem::zeroed() };
	how.flags = u64::try_from(libc::O_PATH | libc::O_CLOEXEC).unwrap();
	how.resolve = 0x08 | 0x02 | 0x04;
	let root = std::fs::File::open("/")
		.map_err(|error| tg::error!(!error, "failed to open the root for the openat2 probe"))?;
	// SAFETY: The path and open_how pointers remain valid for the syscall.
	let fd = unsafe {
		libc::syscall(
			libc::SYS_openat2,
			root.as_raw_fd(),
			path.as_ptr(),
			&raw const how,
			std::mem::size_of::<libc::open_how>(),
		)
	};
	if fd < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(
			!error,
			"openat2 is unavailable or blocked; hardened container mounts require openat2"
		));
	}
	// SAFETY: The syscall returned a newly owned descriptor.
	unsafe { libc::close(fd.try_into().unwrap()) };

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_unified_cgroup_path() {
		assert_eq!(parse_unified_cgroup("0::/runner\n").unwrap(), "/runner");
		assert!(parse_unified_cgroup("2:cpu:/runner\n").is_err());
	}

	#[test]
	fn validate_required_controllers() {
		validate_controllers("memory pids io cpu").unwrap();
		let error = validate_controllers("memory cpu").unwrap_err();
		assert!(error.trace().to_string().contains("pids"));
	}

	#[test]
	fn validate_required_cgroup_features() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		let current = temp.path().join("runner");
		std::fs::create_dir_all(&current).unwrap();
		let proc_self_cgroup = temp.path().join("self.cgroup");
		std::fs::write(&proc_self_cgroup, "0::/runner\n").unwrap();
		std::fs::write(current.join("cgroup.subtree_control"), "cpu memory pids\n").unwrap();
		for name in [
			"cgroup.events",
			"cgroup.kill",
			"cgroup.procs",
			"cpu.max",
			"memory.max",
			"memory.oom.group",
			"memory.swap.max",
			"pids.max",
		] {
			std::fs::write(current.join(name), "").unwrap();
		}

		validate_cgroup_v2(temp.path(), &proc_self_cgroup).unwrap();
		std::fs::remove_file(current.join("cgroup.kill")).unwrap();
		let error = validate_cgroup_v2(temp.path(), &proc_self_cgroup).unwrap_err();
		assert!(error.trace().to_string().contains("cgroup.kill"));
	}

	#[test]
	fn mount_syscalls_are_available() {
		probe_container_namespaces().unwrap();
		validate_mount_syscalls().unwrap();
	}

	#[test]
	fn seccomp_is_available() {
		validate_seccomp().unwrap();
	}

	#[test]
	fn user_namespaces_are_usable() {
		if std::fs::read_to_string("/proc/sys/user/max_user_namespaces")
			.ok()
			.and_then(|value| value.trim().parse::<u64>().ok())
			.is_some_and(|value| value > 0)
		{
			probe_container_namespaces().unwrap();
		}
	}
}
