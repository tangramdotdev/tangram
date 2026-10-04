use {
	rustix::{
		fd::{AsFd as _, OwnedFd},
		io::FdFlags,
		net::{
			RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, SendAncillaryBuffer,
			SendAncillaryMessage, SendFlags,
		},
	},
	std::{
		ffi::{CStr, CString},
		io::{IoSlice, IoSliceMut, Read as _},
		mem::MaybeUninit,
		os::{
			fd::{AsRawFd as _, FromRawFd as _},
			unix::net::UnixStream,
		},
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

const FSCONFIG_CMD_CREATE: libc::c_uint = 6;
const FSCONFIG_SET_STRING: libc::c_uint = 1;
const FSMOUNT_CLOEXEC: libc::c_uint = 1;
const FSOPEN_CLOEXEC: libc::c_uint = 1;
const MOUNT_ATTR_IDMAP: u64 = 0x0010_0000;

/// A detached tmpfs shared by the runner and its sandbox launcher.
///
/// Keeping the filesystem detached avoids modifying the host mount namespace. The mount remains
/// alive only while either process holds its descriptor, so an abrupt runner exit cannot leak it.
/// Overlay upper/work, the output directory, /dev/shm, and /tmp all reside on this filesystem and share its
/// byte and inode limits.
pub struct Filesystem {
	fd: OwnedFd,
}

impl Filesystem {
	pub fn new(fd: OwnedFd) -> Self {
		Self { fd }
	}

	#[must_use]
	pub fn path(&self) -> PathBuf {
		path(&self.fd)
	}
}

pub fn create(size: Option<u64>, inodes: Option<u64>) -> tg::Result<OwnedFd> {
	let fd = syscall_fd(
		libc::SYS_fsopen,
		&[c"tmpfs".as_ptr() as usize, FSOPEN_CLOEXEC as usize],
	)
	.map_err(|error| {
		tg::error!(
			!error,
			"failed to create a filesystem context for the sandbox; hardened filesystem limits require Linux 5.2 or newer and user namespaces"
		)
	})?;
	configure(&fd, c"mode", "0755")?;
	if let Some(size) = size {
		configure(&fd, c"size", &size.to_string())?;
	}
	if let Some(inodes) = inodes {
		configure(&fd, c"nr_inodes", &inodes.to_string())?;
	}
	syscall(
		libc::SYS_fsconfig,
		&[raw_fd_arg(&fd), FSCONFIG_CMD_CREATE as usize, 0, 0, 0],
	)
	.map_err(|error| tg::error!(!error, "failed to create the sandbox filesystem"))?;
	let mount = syscall_fd(
		libc::SYS_fsmount,
		&[raw_fd_arg(&fd), FSMOUNT_CLOEXEC as usize, 0],
	)
	.map_err(|error| tg::error!(!error, "failed to mount the sandbox filesystem"))?;

	Ok(mount)
}

/// Create a mapping from the workload IDs to the runner's host identity.
pub fn host_namespace(uid: libc::uid_t, gid: libc::gid_t) -> tg::Result<OwnedFd> {
	// The setup identity remains mapped to the runner in the parent user namespace.
	// SAFETY: These functions have no preconditions.
	let setup_uid = unsafe { libc::geteuid() };
	let setup_gid = unsafe { libc::getegid() };
	let (mut host, guest) = UnixStream::pair()
		.map_err(|error| tg::error!(!error, "failed to create the host mapping socket pair"))?;
	// SAFETY: The child only invokes async-signal-safe operations before exiting.
	let pid = unsafe { libc::fork() };
	if pid < 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to fork the host mapping process"
		));
	}
	if pid == 0 {
		// SAFETY: The sockets are live, the buffer is valid, and the child exits without running destructors.
		unsafe {
			libc::close(host.as_raw_fd());
			let mut status = libc::unshare(libc::CLONE_NEWUSER);
			if status < 0 {
				status = std::io::Error::last_os_error()
					.raw_os_error()
					.unwrap_or(libc::EIO);
			}
			if libc::write(
				guest.as_raw_fd(),
				(&raw const status).cast(),
				std::mem::size_of_val(&status),
			) != std::mem::size_of_val(&status).cast_signed()
			{
				libc::_exit(1);
			}
			let mut byte = 0u8;
			loop {
				if libc::read(guest.as_raw_fd(), (&raw mut byte).cast(), 1) >= 0 {
					break;
				}
				if std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
					break;
				}
			}
			libc::_exit(0);
		}
	}
	drop(guest);
	let result = (|| {
		let mut status = [0; std::mem::size_of::<libc::c_int>()];
		host.read_exact(&mut status)
			.map_err(|error| tg::error!(!error, "failed to wait for the host mapping namespace"))?;
		let status = libc::c_int::from_ne_bytes(status);
		if status != 0 {
			return Err(tg::error!(
				source = std::io::Error::from_raw_os_error(status),
				"failed to create the host mapping namespace"
			));
		}
		std::fs::write(
			format!("/proc/{pid}/uid_map"),
			format!("{uid} {setup_uid} 1\n"),
		)
		.map_err(|error| tg::error!(!error, "failed to write the host uid mapping"))?;
		std::fs::write(
			format!("/proc/{pid}/gid_map"),
			format!("{gid} {setup_gid} 1\n"),
		)
		.map_err(|error| tg::error!(!error, "failed to write the host gid mapping"))?;
		let namespace = std::fs::File::open(format!("/proc/{pid}/ns/user"))
			.map_err(|error| tg::error!(!error, "failed to open the host mapping namespace"))?;
		Ok(namespace.into())
	})();

	// Release and reap the helper even if configuring its mapping failed.
	drop(host);
	loop {
		// SAFETY: The PID identifies our child and no wait status is requested.
		if unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) } >= 0 {
			break;
		}
		let error = std::io::Error::last_os_error();
		if error.raw_os_error() != Some(libc::EINTR) {
			return Err(tg::error!(
				!error,
				"failed to reap the host mapping process"
			));
		}
	}

	result
}

/// Return a detached host view without changing ownership in the workload's mount.
pub fn host_mount(filesystem: &OwnedFd, namespace: &OwnedFd) -> tg::Result<OwnedFd> {
	let flags = libc::OPEN_TREE_CLONE | libc::OPEN_TREE_CLOEXEC | libc::AT_EMPTY_PATH as u32;
	let mount = syscall_fd(
		libc::SYS_open_tree,
		&[
			raw_fd_arg(filesystem),
			c"".as_ptr() as usize,
			flags as usize,
		],
	)
	.map_err(|error| tg::error!(!error, "failed to clone the sandbox filesystem mount"))?;
	let attributes = [
		MOUNT_ATTR_IDMAP,
		0,
		0,
		u64::try_from(namespace.as_raw_fd()).unwrap(),
	];
	syscall(libc::SYS_mount_setattr, &[
		raw_fd_arg(&mount), c"".as_ptr() as usize, libc::AT_EMPTY_PATH as usize,
		attributes.as_ptr() as usize, std::mem::size_of_val(&attributes),
	]).map_err(|error| tg::error!(!error, "failed to map the sandbox filesystem for the host; subordinate identities require ID-mapped tmpfs support (Linux 6.3 or newer)"))?;

	Ok(mount)
}

pub fn prepare(fd: &OwnedFd, uid: libc::uid_t, gid: libc::gid_t) -> tg::Result<()> {
	let root = path(fd);
	// Keep every parent directory owned by the workload so the host mapping can also create and remove entries.
	for (name, mode) in [
		("", 0o755),
		("output", 0o755),
		("scratch", 0o755),
		("shm", 0o1777),
		("tmp", 0o1777),
		("upper", 0o755),
		("upper/opt", 0o755),
		("upper/opt/tangram", 0o755),
		("work", 0o755),
	] {
		let path = root.join(name);
		std::fs::create_dir_all(&path).map_err(|error| {
			tg::error!(!error, path = %path.display(), "failed to create a sandbox filesystem directory")
		})?;
		std::os::unix::fs::chown(&path, Some(uid), Some(gid)).map_err(|error| {
			tg::error!(!error, path = %path.display(), "failed to set the sandbox filesystem directory owner")
		})?;
		let permissions =
			<std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(mode);
		std::fs::set_permissions(&path, permissions).map_err(|error| {
			tg::error!(!error, path = %path.display(), "failed to set the sandbox filesystem directory permissions")
		})?;
	}

	Ok(())
}

pub fn receive(fd: &OwnedFd) -> tg::Result<Filesystem> {
	let mut byte = [0u8; 1];
	let mut iovecs = [IoSliceMut::new(&mut byte)];
	let mut space = [MaybeUninit::<u8>::uninit(); rustix::cmsg_space!(ScmRights(1))];
	let mut control = RecvAncillaryBuffer::new(&mut space);
	let message = rustix::net::recvmsg(fd, &mut iovecs, &mut control, RecvFlags::empty())
		.map_err(|error| tg::error!(!error, "failed to receive the sandbox filesystem"))?;
	let mut filesystem = None;
	if message.bytes == 1 {
		for message in control.drain() {
			if let RecvAncillaryMessage::ScmRights(mut fds) = message {
				filesystem = fds.next();
				if filesystem.is_some() {
					break;
				}
			}
		}
	}
	let fd = filesystem.ok_or_else(|| tg::error!("failed to receive the sandbox filesystem fd"))?;
	rustix::io::fcntl_setfd(&fd, FdFlags::CLOEXEC)
		.map_err(|error| tg::error!(!error, "failed to configure the sandbox filesystem fd"))?;

	Ok(Filesystem::new(fd))
}

pub fn send(socket: &OwnedFd, filesystem: &OwnedFd) -> tg::Result<()> {
	let byte = [0u8; 1];
	let iovecs = [IoSlice::new(&byte)];
	let fds = [filesystem.as_fd()];
	let mut space = [MaybeUninit::<u8>::uninit(); rustix::cmsg_space!(ScmRights(1))];
	let mut control = SendAncillaryBuffer::new(&mut space);
	control.push(SendAncillaryMessage::ScmRights(&fds));
	rustix::net::sendmsg(socket, &iovecs, &mut control, SendFlags::NOSIGNAL)
		.map_err(|error| tg::error!(!error, "failed to send the sandbox filesystem"))?;

	Ok(())
}

#[must_use]
pub fn path(fd: &OwnedFd) -> PathBuf {
	PathBuf::from(format!("/proc/self/fd/{}", fd.as_raw_fd()))
}

pub fn relocate(path: &mut PathBuf, from: &Path, to: &Path) {
	let Ok(suffix) = path.strip_prefix(from) else {
		return;
	};
	*path = to.join(suffix);
}

fn configure(fd: &OwnedFd, key: &CStr, value: &str) -> tg::Result<()> {
	let value = CString::new(value).unwrap();
	syscall(
		libc::SYS_fsconfig,
		&[
			raw_fd_arg(fd),
			FSCONFIG_SET_STRING as usize,
			key.as_ptr() as usize,
			value.as_ptr() as usize,
			0,
		],
	)
	.map_err(|error| {
		tg::error!(
			!error,
			key = %key.to_string_lossy(),
			"failed to configure the sandbox filesystem"
		)
	})?;

	Ok(())
}

fn syscall(number: libc::c_long, args: &[usize]) -> std::io::Result<libc::c_long> {
	let result = unsafe {
		match args {
			[a, b] => libc::syscall(number, *a, *b),
			[a, b, c] => libc::syscall(number, *a, *b, *c),
			[a, b, c, d, e] => libc::syscall(number, *a, *b, *c, *d, *e),
			_ => unreachable!(),
		}
	};
	if result < 0 {
		return Err(std::io::Error::last_os_error());
	}

	Ok(result)
}

fn raw_fd_arg(fd: &OwnedFd) -> usize {
	usize::try_from(fd.as_raw_fd()).unwrap()
}

fn syscall_fd(number: libc::c_long, args: &[usize]) -> std::io::Result<OwnedFd> {
	let fd = syscall(number, args)?;
	let fd = i32::try_from(fd).unwrap();
	// SAFETY: A successful fsopen or fsmount syscall returns a new owned descriptor.
	let fd = unsafe { OwnedFd::from_raw_fd(fd) };

	Ok(fd)
}

#[cfg(test)]
mod tests {
	use {super::*, std::path::PathBuf};

	#[test]
	fn relocate_descendant() {
		let mut path = PathBuf::from("/sandbox/filesystem/upper/opt/tangram");
		relocate(
			&mut path,
			Path::new("/sandbox/filesystem"),
			Path::new("/proc/self/fd/42"),
		);
		assert_eq!(path, Path::new("/proc/self/fd/42/upper/opt/tangram"));
	}

	#[test]
	fn relocate_ignores_unrelated_path() {
		let mut path = PathBuf::from("/nix/store/example");
		relocate(
			&mut path,
			Path::new("/sandbox/filesystem"),
			Path::new("/proc/self/fd/42"),
		);
		assert_eq!(path, Path::new("/nix/store/example"));
	}
}
