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

pub fn validate(filesystem_path: &Path) -> tg::Result<()> {
	validate_user_namespaces()?;
	validate_cgroup_v2()?;
	validate_seccomp()?;
	validate_mount_syscalls()?;
	validate_filesystem(filesystem_path)?;

	Ok(())
}

fn validate_filesystem(root: &Path) -> tg::Result<()> {
	const PROJECT: u32 = i32::MAX.cast_unsigned();

	let name = format!("tangram-quota-probe-{:016x}", rand::random::<u64>());
	let path = root.join(name);
	let result = super::filesystem::create(&path, PROJECT, Some(64 * 1024), Some(4));
	let enforcement_result = if result.is_ok() {
		probe_filesystem_enforcement(&path)
	} else {
		Ok(())
	};
	let mut file_remove_result = Ok(());
	for name in ["bytes", "inode-0", "inode-1", "inode-2", "inode-3"] {
		if let Err(error) = remove_probe_file(&path.join(name))
			&& file_remove_result.is_ok()
		{
			file_remove_result = Err(error);
		}
	}
	let clear_result = if result.is_ok() {
		super::filesystem::clear(&path, PROJECT)
	} else {
		Ok(())
	};
	let directory_remove_result = match std::fs::remove_dir(&path) {
		Ok(()) => Ok(()),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(error) => Err(error),
	};
	result.map_err(|error| {
		tg::error!(
			!error,
			path = %root.display(),
			"the hardened container filesystem requires a disk-backed ext4 or XFS filesystem with project quotas"
		)
	})?;
	enforcement_result.map_err(|error| {
		tg::error!(
			!error,
			path = %root.display(),
			"the hardened container filesystem project quota is not enforced"
		)
	})?;
	file_remove_result.map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to remove a hardened container filesystem prerequisite probe file"
		)
	})?;
	clear_result.map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to clear the hardened container filesystem prerequisite probe"
		)
	})?;
	directory_remove_result.map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to remove the hardened container filesystem prerequisite probe"
		)
	})?;

	Ok(())
}

fn probe_filesystem_enforcement(path: &Path) -> tg::Result<()> {
	let byte_path = path.join("bytes");
	let file = std::fs::File::create(&byte_path).map_err(|error| {
		tg::error!(!error, path = %byte_path.display(), "failed to create the project byte quota probe")
	})?;
	// SAFETY: The descriptor is valid and the offset and length are nonnegative.
	let result = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, 128 * 1024) };
	if result == 0 {
		return Err(tg::error!(
			"the project byte quota was accepted but is not enforced"
		));
	}
	let error = std::io::Error::last_os_error();
	if error.raw_os_error() != Some(libc::EDQUOT) {
		return Err(tg::error!(
			!error,
			"the project byte quota probe failed with an unexpected error"
		));
	}
	drop(file);
	remove_probe_file(&byte_path).map_err(|error| {
		tg::error!(!error, path = %byte_path.display(), "failed to remove the project byte quota probe")
	})?;

	for index in 0..4 {
		let path = path.join(format!("inode-{index}"));
		match std::fs::File::create(&path) {
			Ok(_) if index < 3 => {},
			Ok(_) => {
				return Err(tg::error!(
					"the project inode quota was accepted but is not enforced"
				));
			},
			Err(error) if index == 3 && error.raw_os_error() == Some(libc::EDQUOT) => {},
			Err(error) => {
				return Err(tg::error!(
					!error,
					"the project inode quota probe failed with an unexpected error"
				));
			},
		}
	}

	Ok(())
}

fn remove_probe_file(path: &Path) -> std::io::Result<()> {
	match std::fs::remove_file(path) {
		Ok(()) => Ok(()),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(error) => Err(error),
	}
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

fn validate_cgroup_v2() -> tg::Result<()> {
	// Probe a child so the hierarchy root need not expose per-cgroup limits itself.
	let name = format!("tangram-probe-{:016x}", rand::random::<u64>());
	let options = super::cgroup::Options {
		cpu: Some(1),
		cpu_pool: None,
		cpu_request: None,
		memory: Some(64 * 1024 * 1024),
		memory_oom_group: true,
		memory_swap: Some(0),
		pids: Some(32),
	};
	let cgroup = super::cgroup::Cgroup::new(&name, &options)?;
	let directory = cgroup.handle()?.open_fd()?;
	let path = std::path::PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
	validate_cgroup_features(&path)?;
	cgroup.cleanup()?;
	Ok(())
}

fn validate_cgroup_features(current: &Path) -> tg::Result<()> {
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
	fn validate_required_cgroup_features() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		let current = temp.path().join("runner");
		std::fs::create_dir_all(&current).unwrap();
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

		validate_cgroup_features(&current).unwrap();
		std::fs::remove_file(current.join("cgroup.kill")).unwrap();
		let error = validate_cgroup_features(&current).unwrap_err();
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
