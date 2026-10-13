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
		ffi::CString,
		io::{IoSlice, IoSliceMut},
		mem::MaybeUninit,
		os::fd::{AsRawFd as _, FromRawFd as _},
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

mod ext4;
mod project;
mod xfs;

const EXT4_SUPER_MAGIC: libc::c_long = 0xef53;
const MOUNT_ATTR_IDMAP: u64 = 0x0010_0000;
const XFS_SUPER_MAGIC: libc::c_long = 0x5846_5342;

/// The runner's detached view of the writable sandbox filesystem.
///
/// The launcher attaches its mapped view only inside the private sandbox mount namespace.
/// The mounts disappear when their descriptors and the sandbox namespace are dropped.
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

pub fn create(path: &Path, project: u32, size: Option<u64>, inodes: Option<u64>) -> tg::Result<()> {
	let parent = path
		.parent()
		.ok_or_else(|| tg::error!("the sandbox filesystem has no parent"))?;
	let directory = std::fs::File::open(parent)
		.map_err(|error| tg::error!(!error, "failed to open the sandbox filesystem parent"))?;
	let filesystem = kind(&directory)?;
	match filesystem {
		EXT4_SUPER_MAGIC => ext4::create(path, project, size, inodes)?,
		XFS_SUPER_MAGIC => xfs::create(path, project, size, inodes)?,
		_ => {
			return Err(
				tg::error!(%filesystem, "filesystem limits require ext4 or XFS project quotas"),
			);
		},
	}

	Ok(())
}

pub fn create_directory(path: &Path) -> tg::Result<()> {
	std::fs::create_dir(path).map_err(|error| {
		tg::error!(!error, path = %path.display(), "failed to create the sandbox filesystem directory")
	})?;

	Ok(())
}

pub(crate) fn clear(path: &Path, project: u32) -> tg::Result<()> {
	let directory = std::fs::File::open(path).map_err(|error| {
		tg::error!(!error, path = %path.display(), "failed to open the sandbox filesystem directory")
	})?;
	let filesystem = kind(&directory)?;
	set_quota(&directory, filesystem, project, Some(0), Some(0))?;
	Ok(())
}

pub fn open(path: &Path) -> tg::Result<OwnedFd> {
	let path = CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
	let flags = libc::OPEN_TREE_CLONE | libc::OPEN_TREE_CLOEXEC;
	let mount = syscall_fd(
		libc::SYS_open_tree,
		&[
			usize::from_ne_bytes(isize::try_from(libc::AT_FDCWD).unwrap().to_ne_bytes()),
			path.as_ptr() as usize,
			flags as usize,
		],
	)
	.map_err(|error| tg::error!(!error, "failed to open the sandbox filesystem mount"))?;

	Ok(mount)
}

/// Open a detached view of the runner-owned filesystem mapped into the workload user namespace.
pub fn open_mapped(path: &Path, namespace: &OwnedFd) -> tg::Result<OwnedFd> {
	let mount = open(path)?;
	let attributes = [
		MOUNT_ATTR_IDMAP,
		0,
		0,
		u64::try_from(namespace.as_raw_fd()).unwrap(),
	];
	syscall(libc::SYS_mount_setattr, &[
		raw_fd_arg(&mount), c"".as_ptr() as usize, libc::AT_EMPTY_PATH as usize,
		attributes.as_ptr() as usize, std::mem::size_of_val(&attributes),
	]).map_err(|error| tg::error!(!error, "failed to map the sandbox filesystem for the workload; subordinate identities require ID-mapped mount support and CAP_SYS_ADMIN in the backing filesystem's user namespace"))?;

	Ok(mount)
}

pub fn attach(fd: &OwnedFd, path: &Path) -> tg::Result<()> {
	let path = CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
	syscall(
		libc::SYS_move_mount,
		&[
			raw_fd_arg(fd),
			c"".as_ptr() as usize,
			usize::from_ne_bytes(isize::try_from(libc::AT_FDCWD).unwrap().to_ne_bytes()),
			path.as_ptr() as usize,
			libc::MOVE_MOUNT_F_EMPTY_PATH as usize,
		],
	)
	.map_err(|error| tg::error!(!error, "failed to attach the workload filesystem mount"))?;
	Ok(())
}

pub fn prepare(fd: &OwnedFd, uid: libc::uid_t, gid: libc::gid_t) -> tg::Result<()> {
	let root = path(fd);
	// Keep every parent directory accessible to both the workload and the runner.
	for (name, mode) in [
		("", 0o755),
		("output", 0o755),
		("scratch", 0o755),
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

pub fn receive(fd: &OwnedFd) -> tg::Result<OwnedFd> {
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

	Ok(fd)
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

fn set_quota(
	directory: &std::fs::File,
	filesystem: libc::c_long,
	project: u32,
	size: Option<u64>,
	inodes: Option<u64>,
) -> tg::Result<()> {
	match filesystem {
		EXT4_SUPER_MAGIC => ext4::set_limit(directory, project, size, inodes)?,
		XFS_SUPER_MAGIC => xfs::set_limit(directory, project, size, inodes)?,
		_ => {
			return Err(
				tg::error!(%filesystem, "filesystem limits require ext4 or XFS project quotas"),
			);
		},
	}

	Ok(())
}

fn syscall(number: libc::c_long, args: &[usize]) -> std::io::Result<libc::c_long> {
	let result = unsafe {
		match args {
			[a, b] => libc::syscall(number, *a, *b),
			[a, b, c] => libc::syscall(number, *a, *b, *c),
			[a, b, c, d] => libc::syscall(number, *a, *b, *c, *d),
			[a, b, c, d, e] => libc::syscall(number, *a, *b, *c, *d, *e),
			_ => unreachable!(),
		}
	};
	if result < 0 {
		return Err(std::io::Error::last_os_error());
	}

	Ok(result)
}

fn raw_fd_arg(fd: &impl std::os::fd::AsRawFd) -> usize {
	usize::try_from(fd.as_raw_fd()).unwrap()
}

fn syscall_fd(number: libc::c_long, args: &[usize]) -> std::io::Result<OwnedFd> {
	let fd = syscall(number, args)?;
	let fd = i32::try_from(fd).unwrap();

	// SAFETY: A successful mount syscall returns a new owned descriptor.
	let fd = unsafe { OwnedFd::from_raw_fd(fd) };

	Ok(fd)
}

pub(crate) fn usage(directory: &std::fs::File, project: u32) -> tg::Result<u64> {
	let usage = project::usage(directory, project)?;

	Ok(usage)
}

pub(crate) fn control(path: &Path, project: u32) -> tg::Result<crate::budget::Control> {
	let parent = path
		.parent()
		.ok_or_else(|| tg::error!("the filesystem project has no parent"))?;
	let directory = std::fs::File::open(parent)
		.map_err(|error| tg::error!(!error, "failed to open the filesystem project"))?;
	let directory = std::sync::Arc::new(directory);
	let filesystem = kind(&directory)?;
	let usage = {
		let directory = directory.clone();
		Box::new(move || usage(&directory, project))
	};
	let limit = Box::new(move |size| set_quota(&directory, filesystem, project, Some(size), None));

	Ok(crate::budget::Control { limit, usage })
}

fn kind(directory: &std::fs::File) -> tg::Result<libc::c_long> {
	let mut statistics = MaybeUninit::<libc::statfs>::uninit();

	// SAFETY: The descriptor and output pointer are valid.
	if unsafe { libc::fstatfs(directory.as_raw_fd(), statistics.as_mut_ptr()) } != 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to inspect the sandbox filesystem"
		));
	}

	// SAFETY: fstatfs initialized the structure on success.
	let statistics = unsafe { statistics.assume_init() };

	Ok(statistics.f_type)
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
