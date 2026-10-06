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

const EXT4_SUPER_MAGIC: libc::c_long = 0xef53;
const FS_XFLAG_PROJINHERIT: u32 = 0x0000_0200;
const PRJQUOTA: libc::c_int = 2;
const MOUNT_ATTR_IDMAP: u64 = 0x0010_0000;
const XFS_SUPER_MAGIC: libc::c_long = 0x5846_5342;

#[repr(C)]
struct FsXAttr {
	xflags: u32,
	extsize: u32,
	nextents: u32,
	project: u32,
	cowextsize: u32,
	pad: [u8; 8],
}

#[repr(C)]
struct XfsDiskQuota {
	version: i8,
	flags: i8,
	fieldmask: u16,
	id: u32,
	block_hard: u64,
	block_soft: u64,
	inode_hard: u64,
	inode_soft: u64,
	block_count: u64,
	inode_count: u64,
	inode_timer: i32,
	block_timer: i32,
	inode_warnings: u16,
	block_warnings: u16,
	inode_timer_high: i8,
	block_timer_high: i8,
	realtime_block_timer_high: i8,
	padding2: i8,
	realtime_block_hard: u64,
	realtime_block_soft: u64,
	realtime_block_count: u64,
	realtime_block_timer: i32,
	realtime_block_warnings: u16,
	padding3: i16,
	padding4: [u8; 8],
}

/// A detached view of a disk-backed directory shared by the runner and its sandbox launcher.
///
/// Keeping the filesystem detached avoids modifying the host mount namespace. The mount remains
/// alive only while either process holds its descriptor, so an abrupt runner exit cannot leak a mount.
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
	create_directory(path)?;
	let directory = std::fs::File::open(path).map_err(|error| {
		tg::error!(!error, path = %path.display(), "failed to open the sandbox filesystem directory")
	})?;
	configure_quota(&directory, project, size, inodes)
}

pub fn create_directory(path: &Path) -> tg::Result<()> {
	std::fs::create_dir(path).map_err(|error| {
		tg::error!(!error, path = %path.display(), "failed to create the sandbox filesystem directory")
	})?;

	Ok(())
}

fn configure_quota(
	directory: &std::fs::File,
	project: u32,
	size: Option<u64>,
	inodes: Option<u64>,
) -> tg::Result<()> {
	let mut statistics = std::mem::MaybeUninit::<libc::statfs>::uninit();
	// SAFETY: The descriptor and output pointer are valid.
	if unsafe { libc::fstatfs(directory.as_raw_fd(), statistics.as_mut_ptr()) } != 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to inspect the sandbox filesystem"
		));
	}
	// SAFETY: fstatfs initialized the structure on success.
	let statistics = unsafe { statistics.assume_init() };
	if !matches!(statistics.f_type, EXT4_SUPER_MAGIC | XFS_SUPER_MAGIC) {
		return Err(
			tg::error!(filesystem = %statistics.f_type, "hardened filesystem limits require a disk-backed ext4 or XFS filesystem with project quotas enabled"),
		);
	}
	let attributes = FsXAttr {
		xflags: FS_XFLAG_PROJINHERIT,
		extsize: 0,
		nextents: 0,
		project,
		cowextsize: 0,
		pad: [0; 8],
	};
	let request = ioctl_write_request(b'X', 32, std::mem::size_of::<FsXAttr>());
	// SAFETY: The descriptor refers to the new directory and the attribute pointer is valid.
	if unsafe { libc::ioctl(directory.as_raw_fd(), request, &raw const attributes) } != 0 {
		return Err(
			tg::error!(source = std::io::Error::last_os_error(), %project, "failed to assign the sandbox filesystem project; enable project quotas and grant CAP_SYS_ADMIN"),
		);
	}
	set_quota(directory, statistics.f_type, project, size, inodes)?;

	Ok(())
}

pub(crate) fn clear(path: &Path, project: u32) -> tg::Result<()> {
	let directory = std::fs::File::open(path).map_err(|error| {
		tg::error!(!error, path = %path.display(), "failed to open the sandbox filesystem directory")
	})?;
	let mut statistics = std::mem::MaybeUninit::<libc::statfs>::uninit();
	// SAFETY: The descriptor and output pointer are valid.
	if unsafe { libc::fstatfs(directory.as_raw_fd(), statistics.as_mut_ptr()) } != 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to inspect the sandbox filesystem"
		));
	}
	// SAFETY: fstatfs initialized the structure on success.
	let statistics = unsafe { statistics.assume_init() };
	set_quota(&directory, statistics.f_type, project, Some(0), Some(0))
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
	]).map_err(|error| tg::error!(!error, "failed to map the sandbox filesystem for the host; subordinate identities require ID-mapped mount support from the backing filesystem"))?;

	Ok(mount)
}

pub fn prepare(fd: &OwnedFd, uid: libc::uid_t, gid: libc::gid_t) -> tg::Result<()> {
	let root = path(fd);
	// Keep every parent directory owned by the workload so the host mapping can also create and remove entries.
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

fn set_quota(
	directory: &std::fs::File,
	filesystem: libc::c_long,
	project: u32,
	size: Option<u64>,
	inodes: Option<u64>,
) -> tg::Result<()> {
	let result = if filesystem == XFS_SUPER_MAGIC {
		let block_mask = if size.is_some() {
			(1 << 2) | (1 << 3)
		} else {
			0
		};
		let inode_mask = if inodes.is_some() {
			(1 << 0) | (1 << 1)
		} else {
			0
		};
		let limits = XfsDiskQuota {
			version: 1,
			flags: 1 << 1,
			fieldmask: block_mask | inode_mask,
			id: project,
			block_hard: size.unwrap_or_default().div_ceil(512),
			block_soft: size.unwrap_or_default().div_ceil(512),
			inode_hard: inodes.unwrap_or_default(),
			inode_soft: inodes.unwrap_or_default(),
			block_count: 0,
			inode_count: 0,
			inode_timer: 0,
			block_timer: 0,
			inode_warnings: 0,
			block_warnings: 0,
			inode_timer_high: 0,
			block_timer_high: 0,
			realtime_block_timer_high: 0,
			padding2: 0,
			realtime_block_hard: 0,
			realtime_block_soft: 0,
			realtime_block_count: 0,
			realtime_block_timer: 0,
			realtime_block_warnings: 0,
			padding3: 0,
			padding4: [0; 8],
		};
		let command = libc::QCMD((u32::from(b'X') << 8).cast_signed() | 4, PRJQUOTA);
		quota_control(directory, command, project, (&raw const limits).cast())
	} else {
		let limits = libc::dqblk {
			dqb_bhardlimit: size.unwrap_or_default().div_ceil(1024),
			dqb_bsoftlimit: size.unwrap_or_default().div_ceil(1024),
			dqb_curspace: 0,
			dqb_ihardlimit: inodes.unwrap_or_default(),
			dqb_isoftlimit: inodes.unwrap_or_default(),
			dqb_curinodes: 0,
			dqb_btime: 0,
			dqb_itime: 0,
			dqb_valid: (u32::from(size.is_some()) * libc::QIF_BLIMITS)
				| (u32::from(inodes.is_some()) * libc::QIF_ILIMITS),
		};
		let command = libc::QCMD(libc::Q_SETQUOTA, PRJQUOTA);
		quota_control(directory, command, project, (&raw const limits).cast())
	};
	result.map_err(|error| {
		tg::error!(
			!error,
			%project,
			"failed to configure the sandbox filesystem project quota; enable project quota enforcement and grant CAP_SYS_ADMIN"
		)
	})?;

	Ok(())
}

fn quota_control(
	directory: &std::fs::File,
	command: libc::c_int,
	project: u32,
	limits: *const libc::c_void,
) -> std::io::Result<()> {
	let result = syscall(
		libc::SYS_quotactl_fd,
		&[
			raw_fd_arg(directory),
			usize::try_from(command).unwrap(),
			project as usize,
			limits as usize,
		],
	)?;
	debug_assert_eq!(result, 0);
	Ok(())
}

const fn ioctl_write_request(group: u8, number: u8, size: usize) -> libc::c_ulong {
	#[cfg(any(
		target_arch = "mips",
		target_arch = "mips32r6",
		target_arch = "mips64",
		target_arch = "mips64r6",
		target_arch = "powerpc",
		target_arch = "powerpc64"
	))]
	let (direction, direction_shift) = (4_u64, 29_u32);
	#[cfg(not(any(
		target_arch = "mips",
		target_arch = "mips32r6",
		target_arch = "mips64",
		target_arch = "mips64r6",
		target_arch = "powerpc",
		target_arch = "powerpc64"
	)))]
	let (direction, direction_shift) = (1_u64, 30_u32);
	let value = (direction << direction_shift)
		| ((size as u64) << 16)
		| ((group as u64) << 8)
		| number as u64;
	value as libc::c_ulong
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
