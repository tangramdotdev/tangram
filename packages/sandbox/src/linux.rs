use {
	num::ToPrimitive as _,
	std::{
		io::{Read as _, Write as _},
		os::{
			fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd},
			unix::net::UnixStream,
		},
	},
	tangram_client::prelude::*,
};

pub(crate) fn fork() -> tg::Result<libc::pid_t> {
	let pid = unsafe { libc::fork() };
	if pid < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to fork the sandbox child"));
	}
	Ok(pid)
}

pub(crate) fn validate_descriptor(fd: RawFd) -> tg::Result<()> {
	// Check the descriptor without taking ownership of it.
	// SAFETY: fcntl accepts a numeric descriptor and does not retain any pointers.
	if unsafe { libc::fcntl(fd, libc::F_GETFD) } < 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			%fd,
			"the launch descriptor is not open"
		));
	}
	Ok(())
}

pub(crate) fn open_pidfd() -> tg::Result<OwnedFd> {
	// SAFETY: Both system calls take only scalar arguments.
	let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, libc::getpid(), 0) };
	if fd < 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to open the launcher pidfd"
		));
	}

	// SAFETY: The successful syscall returned a newly owned descriptor.
	let fd = unsafe { OwnedFd::from_raw_fd(fd.try_into().unwrap()) };

	Ok(fd)
}

pub(crate) fn validate_parent(parent: &OwnedFd) -> tg::Result<()> {
	let mut descriptor = libc::pollfd {
		events: libc::POLLIN,
		fd: parent.as_raw_fd(),
		revents: 0,
	};
	loop {
		// SAFETY: The pollfd is valid for one descriptor and poll does not retain its address.
		let result = unsafe { libc::poll(&raw mut descriptor, 1, 0) };
		if result == 0 {
			return Ok(());
		}
		if result > 0 {
			return Err(tg::error!(
				"the parent exited while changing the sandbox identity"
			));
		}
		let error = std::io::Error::last_os_error();
		if error.kind() != std::io::ErrorKind::Interrupted {
			return Err(tg::error!(!error, "failed to query the launcher pidfd"));
		}
	}
}

pub(crate) fn set_rlimit_nofile(limit: Option<u64>) -> tg::Result<()> {
	let Some(limit) = limit else {
		return Ok(());
	};
	let limit = libc::rlim_t::try_from(limit)
		.map_err(|error| tg::error!(!error, "the open file limit is too large"))?;
	let rlimit = libc::rlimit {
		rlim_cur: limit,
		rlim_max: limit,
	};
	let result = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raw const rlimit) };
	if result < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to set the open file limit"));
	}

	Ok(())
}

pub(crate) fn enter_user_namespace(uid: libc::uid_t, gid: libc::gid_t) -> tg::Result<()> {
	let host_uid = unsafe { libc::getuid() };
	let host_gid = unsafe { libc::getgid() };
	unshare(libc::CLONE_NEWUSER, "failed to unshare the user namespace")?;
	std::fs::write("/proc/self/uid_map", format!("{uid} {host_uid} 1\n"))
		.map_err(|error| tg::error!(!error, "failed to write the uid map"))?;
	std::fs::write("/proc/self/setgroups", "deny")
		.map_err(|error| tg::error!(!error, "failed to deny setgroups"))?;
	std::fs::write("/proc/self/gid_map", format!("{gid} {host_gid} 1\n"))
		.map_err(|error| tg::error!(!error, "failed to write the gid map"))?;
	Ok(())
}

pub(crate) fn enter_user_namespace_with_parent(
	fd: RawFd,
	count: usize,
) -> tg::Result<Vec<OwnedFd>> {
	// Validate the synchronization descriptor.
	if fd < 0 {
		return Err(tg::error!(fd = %fd, "the user namespace requires a valid sync fd"));
	}

	// Create the user namespace.
	unshare(libc::CLONE_NEWUSER, "failed to unshare the user namespace")?;

	// Send the process identity to the parent.
	// SAFETY: The descriptor is inherited from the parent and is exclusively owned here.
	let fd = unsafe { OwnedFd::from_raw_fd(fd) };
	let mut socket = UnixStream::from(fd);

	// SAFETY: This function has no preconditions.
	let pid = unsafe { libc::getpid() };
	socket
		.write_all(&pid.to_ne_bytes())
		.map_err(|error| tg::error!(!error, "failed to signal user namespace readiness"))?;

	// Wait for the parent to install the identity maps and send the ID-mapped mounts.
	let filesystems = if count == 0 {
		let mut buffer = [0];
		socket
			.read_exact(&mut buffer)
			.map_err(|error| tg::error!(!error, "failed to wait for the user namespace mapping"))?;
		Vec::new()
	} else {
		let socket = socket.into();
		(0..count)
			.map(|_| crate::filesystem::receive(&socket))
			.collect::<tg::Result<Vec<_>>>()?
	};

	// Clear the supplementary groups.
	let result = unsafe { libc::setgroups(0, std::ptr::null()) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to clear supplementary groups"));
	}

	Ok(filesystems)
}

pub(crate) fn unshare(flags: libc::c_int, message: &'static str) -> tg::Result<()> {
	let result = unsafe { libc::unshare(flags) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "{}", message));
	}
	Ok(())
}

pub(crate) fn set_hostname(hostname: &str) -> tg::Result<()> {
	let result = unsafe { libc::sethostname(hostname.as_ptr().cast(), hostname.len()) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to set the hostname"));
	}
	Ok(())
}

pub(crate) fn start_session() -> tg::Result<()> {
	let ret = unsafe { libc::setsid() };
	if ret < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to start a new session"));
	}
	Ok(())
}

pub(crate) fn setresuid(uid: libc::uid_t) -> tg::Result<()> {
	let ret = unsafe { libc::setresuid(uid, uid, uid) };
	if ret != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, uid = %uid, "failed to set the uid"));
	}
	Ok(())
}

pub(crate) fn setresgid(gid: libc::gid_t) -> tg::Result<()> {
	let ret = unsafe { libc::setresgid(gid, gid, gid) };
	if ret != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, gid = %gid, "failed to set the gid"));
	}
	Ok(())
}

pub(crate) fn set_filesystem_ids(uid: libc::uid_t, gid: libc::gid_t) -> tg::Result<()> {
	// SAFETY: These calls change only this process's filesystem credentials, and the all-ones ID queries them without changing them.
	unsafe {
		libc::setfsgid(gid);
		if libc::setfsgid(libc::gid_t::MAX).cast_unsigned() != gid {
			return Err(tg::error!(%gid, "failed to set the filesystem gid"));
		}
		libc::setfsuid(uid);
		if libc::setfsuid(libc::uid_t::MAX).cast_unsigned() != uid {
			return Err(tg::error!(%uid, "failed to set the filesystem uid"));
		}
	}
	Ok(())
}

pub(crate) fn set_no_new_privs() -> tg::Result<()> {
	let ret = unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) };
	if ret != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to set no_new_privs"));
	}
	Ok(())
}

pub(crate) fn set_securebits() -> tg::Result<()> {
	const SECBIT_NOROOT: libc::c_int = 1 << 0;
	const SECBIT_NOROOT_LOCKED: libc::c_int = 1 << 1;
	const SECBIT_NO_SETUID_FIXUP: libc::c_int = 1 << 2;
	const SECBIT_NO_SETUID_FIXUP_LOCKED: libc::c_int = 1 << 3;
	const SECBIT_KEEP_CAPS_LOCKED: libc::c_int = 1 << 5;
	const SECBIT_NO_CAP_AMBIENT_RAISE: libc::c_int = 1 << 6;
	const SECBIT_NO_CAP_AMBIENT_RAISE_LOCKED: libc::c_int = 1 << 7;
	const PR_SET_SECUREBITS: libc::c_int = 28;

	// Compute the securebits mask.
	let securebits = SECBIT_NOROOT
		| SECBIT_NOROOT_LOCKED
		| SECBIT_NO_SETUID_FIXUP
		| SECBIT_NO_SETUID_FIXUP_LOCKED
		| SECBIT_KEEP_CAPS_LOCKED
		| SECBIT_NO_CAP_AMBIENT_RAISE
		| SECBIT_NO_CAP_AMBIENT_RAISE_LOCKED;

	// Lock the securebits.
	let ret = unsafe { libc::prctl(PR_SET_SECUREBITS, securebits, 0, 0, 0) };
	if ret != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to set securebits"));
	}

	Ok(())
}

pub(crate) fn drop_capabilities() -> tg::Result<()> {
	#[repr(C)]
	struct CapUserHeader {
		version: u32,
		pid: i32,
	}

	#[repr(C)]
	#[derive(Clone, Copy)]
	struct CapUserData {
		effective: u32,
		permitted: u32,
		inheritable: u32,
	}

	const _: () = {
		assert!(std::mem::offset_of!(CapUserHeader, version) == 0);
		assert!(std::mem::offset_of!(CapUserHeader, pid) == 4);
		assert!(std::mem::offset_of!(CapUserData, permitted) == 4);
		assert!(std::mem::offset_of!(CapUserData, inheritable) == 8);
	};

	const CAPABILITY_VERSION_3: u32 = 0x2008_0522;
	const PR_CAP_AMBIENT: libc::c_int = 47;
	const PR_CAP_AMBIENT_CLEAR_ALL: libc::c_int = 4;
	const PR_CAPBSET_DROP: libc::c_int = 24;

	// Clear the ambient capabilities.
	let ret = unsafe { libc::prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0) };
	if ret != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to clear ambient capabilities"));
	}

	// Drop the capability bounding set.
	for capability in 0..64 {
		// SAFETY: prctl takes scalar arguments and the operation does not retain pointers.
		if unsafe { libc::prctl(libc::PR_CAPBSET_READ, capability, 0, 0, 0) } < 0 {
			let error = std::io::Error::last_os_error();
			if error.raw_os_error() == Some(libc::EINVAL) {
				break;
			}
			return Err(tg::error!(
				!error,
				"failed to read the capability bounding set"
			));
		}

		// SAFETY: prctl takes scalar arguments and the operation does not retain pointers.
		if unsafe { libc::prctl(PR_CAPBSET_DROP, capability, 0, 0, 0) } != 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				capability = %capability,
				"failed to drop a capability from the bounding set"
			));
		}
	}

	// Clear the effective, permitted, and inheritable capabilities.
	let mut header = CapUserHeader {
		pid: 0,
		version: CAPABILITY_VERSION_3,
	};
	let mut data = [CapUserData {
		effective: 0,
		inheritable: 0,
		permitted: 0,
	}; 2];
	let ret = unsafe {
		libc::syscall(
			libc::SYS_capset,
			std::ptr::addr_of_mut!(header),
			data.as_mut_ptr(),
		)
	};
	if ret != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to drop capabilities"));
	}

	Ok(())
}

pub(crate) fn close_non_std_fds() -> tg::Result<()> {
	const CLOSE_RANGE_UNSHARE: libc::c_uint = 1 << 1;
	let max = libc::c_uint::MAX;
	let result = unsafe { libc::syscall(libc::SYS_close_range, 3u32, max, CLOSE_RANGE_UNSHARE) };
	if result == 0 {
		return Ok(());
	}
	let error = std::io::Error::last_os_error();
	if !matches!(
		error.raw_os_error(),
		Some(libc::ENOSYS | libc::EINVAL | libc::EPERM)
	) {
		return Err(tg::error!(!error, "failed to close extra file descriptors"));
	}
	close_non_std_fds_fallback()
}

pub(crate) fn close_non_std_fds_fallback() -> tg::Result<()> {
	let entries = std::fs::read_dir("/proc/self/fd")
		.map_err(|error| tg::error!(!error, "failed to enumerate file descriptors"))?;
	let mut fds = Vec::new();
	for entry in entries {
		let entry =
			entry.map_err(|error| tg::error!(!error, "failed to read a directory entry"))?;
		let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
			continue;
		};
		let Ok(fd) = name.parse::<RawFd>() else {
			continue;
		};
		if fd <= libc::STDERR_FILENO {
			continue;
		}
		fds.push(fd);
	}
	for fd in fds {
		unsafe {
			libc::close(fd);
		}
	}
	Ok(())
}

pub(crate) fn wait_for_child(child: libc::pid_t) -> tg::Result<u8> {
	loop {
		let mut status = 0;
		let result = unsafe { libc::waitpid(child, std::ptr::addr_of_mut!(status), 0) };
		if result < 0 {
			let error = std::io::Error::last_os_error();
			if error.raw_os_error() == Some(libc::EINTR) {
				continue;
			}
			return Err(tg::error!(!error, "failed to wait for the sandbox child"));
		}
		if libc::WIFEXITED(status) {
			return Ok(libc::WEXITSTATUS(status).min(255).to_u8().unwrap());
		}
		if libc::WIFSIGNALED(status) {
			let signal = libc::WTERMSIG(status);
			return Ok((128 + signal).min(255).to_u8().unwrap());
		}
		return Ok(1);
	}
}
