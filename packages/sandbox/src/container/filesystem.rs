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
		io::{IoSlice, IoSliceMut},
		mem::MaybeUninit,
		os::fd::{AsRawFd as _, FromRawFd as _},
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

const FSCONFIG_CMD_CREATE: libc::c_uint = 6;
const FSCONFIG_SET_STRING: libc::c_uint = 1;
const FSMOUNT_CLOEXEC: libc::c_uint = 1;
const FSOPEN_CLOEXEC: libc::c_uint = 1;

/// A detached tmpfs shared by the runner and its sandbox launcher.
///
/// Keeping the filesystem detached avoids modifying the host mount namespace. The mount remains
/// alive only while either process holds its descriptor, so an abrupt runner exit cannot leak it.
/// Overlay upper/work, the output directory, and /tmp all reside on this filesystem and share its
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

pub fn prepare(fd: &OwnedFd) -> tg::Result<()> {
	let root = path(fd);
	for name in ["output", "scratch", "tmp", "upper", "work"] {
		let path = root.join(name);
		std::fs::create_dir(&path).map_err(|error| {
			tg::error!(
				!error,
				path = %path.display(),
				"failed to create a sandbox filesystem directory"
			)
		})?;
	}
	let tmp = root.join("tmp");
	let permissions =
		<std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o1777);
	std::fs::set_permissions(&tmp, permissions).map_err(|error| {
		tg::error!(
			!error,
			path = %tmp.display(),
			"failed to set the sandbox tmp directory permissions"
		)
	})?;
	let tangram = root.join("upper/opt/tangram");
	std::fs::create_dir_all(&tangram).map_err(|error| {
		tg::error!(
			!error,
			path = %tangram.display(),
			"failed to create the sandbox tangram directory"
		)
	})?;

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
