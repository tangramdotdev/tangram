use {
	std::{fs::File, os::fd::AsRawFd as _, path::Path},
	tangram_client::prelude::*,
};

#[cfg(test)]
mod tests;

const FS_XFLAG_PROJINHERIT: u32 = 0x0000_0200;
pub(super) const PRJQUOTA: libc::c_int = 2;

// Preserve the field order required by the Linux quota ABI.
#[repr(C)]
struct FsXAttr {
	xflags: u32,
	extsize: u32,
	nextents: u32,
	project: u32,
	cowextsize: u32,
	pad: [u8; 8],
}

pub(super) fn create(path: &Path, project: u32) -> tg::Result<File> {
	// Create the project directory.
	super::create_directory(path)?;
	let directory = File::open(path).map_err(|error| {
		tg::error!(!error, path = %path.display(), "failed to open the sandbox filesystem directory")
	})?;

	// Set the project ID and inheritance flag.
	let attributes = FsXAttr {
		cowextsize: 0,
		extsize: 0,
		nextents: 0,
		pad: [0; 8],
		project,
		xflags: FS_XFLAG_PROJINHERIT,
	};
	let request = ioctl_write_request(b'X', 32, std::mem::size_of::<FsXAttr>());

	// SAFETY: The descriptor refers to the new directory and the attribute pointer is valid.
	if unsafe { libc::ioctl(directory.as_raw_fd(), request, &raw const attributes) } != 0 {
		return Err(
			tg::error!(source = std::io::Error::last_os_error(), %project, "failed to assign the sandbox filesystem project; enable project quotas and grant CAP_SYS_ADMIN"),
		);
	}

	Ok(directory)
}

pub(super) fn id(path: &Path) -> tg::Result<u32> {
	let directory = File::open(path).map_err(|error| {
		tg::error!(!error, path = %path.display(), "failed to open the sandbox filesystem directory")
	})?;
	let mut attributes = std::mem::MaybeUninit::<FsXAttr>::zeroed();
	let request = ioctl_read_request(b'X', 31, std::mem::size_of::<FsXAttr>());

	// SAFETY: The descriptor refers to an open directory and the attribute pointer is valid for writes.
	if unsafe { libc::ioctl(directory.as_raw_fd(), request, attributes.as_mut_ptr()) } != 0 {
		return Err(
			tg::error!(source = std::io::Error::last_os_error(), path = %path.display(), "failed to read the sandbox filesystem project"),
		);
	}

	// SAFETY: The ioctl initialized the structure on success.
	let attributes = unsafe { attributes.assume_init() };

	Ok(attributes.project)
}

pub(super) fn control(
	directory: &std::fs::File,
	command: libc::c_int,
	project: u32,
	limits: *const libc::c_void,
) -> std::io::Result<()> {
	let result = super::syscall(
		libc::SYS_quotactl_fd,
		&[
			super::raw_fd_arg(directory),
			usize::try_from(command.cast_unsigned()).unwrap(),
			project as usize,
			limits as usize,
		],
	)?;
	debug_assert_eq!(result, 0);
	Ok(())
}

pub(super) fn usage(directory: &std::fs::File, project: u32) -> tg::Result<u64> {
	let mut quota = std::mem::MaybeUninit::<libc::dqblk>::zeroed();
	let command = libc::QCMD(libc::Q_GETQUOTA, PRJQUOTA);
	control(directory, command, project, quota.as_mut_ptr().cast())
		.map_err(|error| tg::error!(!error, "failed to read the filesystem project usage"))?;

	// SAFETY: The quota syscall initialized the structure on success.
	let quota = unsafe { quota.assume_init() };

	Ok(quota.dqb_curspace)
}

const fn ioctl_read_request(group: u8, number: u8, size: usize) -> libc::c_ulong {
	#[cfg(any(
		target_arch = "mips",
		target_arch = "mips32r6",
		target_arch = "mips64",
		target_arch = "mips64r6",
		target_arch = "powerpc",
		target_arch = "powerpc64"
	))]
	let (direction, direction_shift) = (2_u64, 29_u32);
	#[cfg(not(any(
		target_arch = "mips",
		target_arch = "mips32r6",
		target_arch = "mips64",
		target_arch = "mips64r6",
		target_arch = "powerpc",
		target_arch = "powerpc64"
	)))]
	let (direction, direction_shift) = (2_u64, 30_u32);
	let value = (direction << direction_shift)
		| ((size as u64) << 16)
		| ((group as u64) << 8)
		| number as u64;
	value as libc::c_ulong
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
