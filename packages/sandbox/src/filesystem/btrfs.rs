use {
	std::{
		fmt::Write as _,
		fs::File,
		os::fd::AsRawFd as _,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

const MAGIC: u32 = 0x94;
const FS_INFO: libc::Ioctl = libc::_IOR::<Info>(MAGIC, 31);
const GET_SUBVOL_INFO: libc::Ioctl = libc::_IOR::<SubvolumeInfo>(MAGIC, 60);
const QGROUP_ASSIGN: libc::Ioctl = libc::_IOW::<[u64; 3]>(MAGIC, 41);
const QGROUP_CREATE: libc::Ioctl = libc::_IOW::<[u64; 2]>(MAGIC, 42);
const QGROUP_LIMIT: libc::Ioctl = libc::_IOR::<[u64; 6]>(MAGIC, 43);
const QGROUP_LIMIT_MAX_RFER: u64 = 1;
const SUBVOL_CREATE: libc::Ioctl = libc::_IOW::<Subvolume>(MAGIC, 14);

// Preserve the field order required by the Linux Btrfs ioctl ABI.
#[repr(C)]
struct Subvolume {
	fd: i64,
	name: [u8; 4088],
}

#[repr(C)]
struct SubvolumeInfo {
	id: u64,
	unused: [u8; 496],
}

#[repr(C)]
struct Info {
	max_id: u64,
	num_devices: u64,
	fsid: [u8; 16],
	unused: [u8; 992],
}

pub(super) fn create(
	directory: &File,
	path: &Path,
	project: u32,
	size: Option<u64>,
	inodes: Option<u64>,
) -> tg::Result<()> {
	if inodes.is_some() {
		return Err(tg::error!(
			"the Btrfs filesystem does not support sandbox inode limits"
		));
	}
	let root = sysfs(directory)?;
	validate(&root)?;
	let name = path.file_name().unwrap().as_encoded_bytes();
	if name.len() > 255 || name.contains(&0) {
		return Err(tg::error!("invalid Btrfs sandbox subvolume name"));
	}

	// Keep a level-one qgroup for each project so accounting survives subvolume deletion.
	let mut arg = [1_u64, qgroup(project)];
	match ioctl(directory, QGROUP_CREATE, &mut arg) {
		Ok(_) => {},
		Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {
			if usage(directory, project)? != 0 {
				return Err(tg::error!(%project, "the Btrfs sandbox quota group is still in use"));
			}
		},
		Err(error) => {
			return Err(tg::error!(
				!error,
				"failed to create the Btrfs sandbox quota group; enable quotas and grant CAP_SYS_ADMIN"
			));
		},
	}
	set_limit(directory, project, size.unwrap_or(u64::MAX))?;

	// Create the subvolume with the kernel's default quota inheritance.
	let mut arg = Subvolume {
		fd: 0,
		name: [0; 4088],
	};
	arg.name[..name.len()].copy_from_slice(name);
	ioctl(directory, SUBVOL_CREATE, &mut arg)
		.map_err(|error| tg::error!(!error, "failed to create the Btrfs sandbox subvolume"))?;

	// Assign the empty subvolume before exposing it to workloads so no shared extents require a rescan.
	let subvolume = File::open(path)
		.map_err(|error| tg::error!(!error, "failed to open the Btrfs sandbox subvolume"))?;
	let mut info = SubvolumeInfo {
		id: 0,
		unused: [0; 496],
	};
	ioctl(&subvolume, GET_SUBVOL_INFO, &mut info)
		.map_err(|error| tg::error!(!error, "failed to read the Btrfs sandbox subvolume ID"))?;
	let mut arg = [1, info.id, qgroup(project)];
	let result = ioctl(directory, QGROUP_ASSIGN, &mut arg)
		.map_err(|error| tg::error!(!error, "failed to assign the Btrfs sandbox quota group"))?;
	if result != 0 {
		return Err(tg::error!("the Btrfs quota assignment requires a rescan"));
	}
	validate(&root)?;

	Ok(())
}

pub(super) fn set_limit(directory: &File, project: u32, size: u64) -> tg::Result<()> {
	let mut arg = [qgroup(project), QGROUP_LIMIT_MAX_RFER, size, 0, 0, 0];
	ioctl(directory, QGROUP_LIMIT, &mut arg).map_err(
		|error| tg::error!(!error, %project, "failed to set the Btrfs sandbox storage limit"),
	)?;
	Ok(())
}

pub(super) fn usage(directory: &File, project: u32) -> tg::Result<u64> {
	let root = sysfs(directory)?;
	validate(&root)?;
	let path = root.join("qgroups").join(format!("1_{project}"));
	if !path
		.try_exists()
		.map_err(|error| tg::error!(!error, "failed to inspect the Btrfs quota group"))?
	{
		return Ok(0);
	}

	// Include pending allocations so the pool retains backing before a transaction commits.
	let mut bytes = 0_u64;
	for name in [
		"referenced",
		"rsv_data",
		"rsv_meta_pertrans",
		"rsv_meta_prealloc",
	] {
		bytes = bytes
			.checked_add(read(&path.join(name))?)
			.ok_or_else(|| tg::error!("the Btrfs sandbox storage usage overflowed"))?;
	}
	Ok(bytes)
}

fn qgroup(project: u32) -> u64 {
	(1_u64 << 48) | u64::from(project)
}

fn sysfs(directory: &File) -> tg::Result<PathBuf> {
	let mut info = Info {
		fsid: [0; 16],
		max_id: 0,
		num_devices: 0,
		unused: [0; 992],
	};
	ioctl(directory, FS_INFO, &mut info)
		.map_err(|error| tg::error!(!error, "failed to read the Btrfs filesystem ID"))?;
	let mut id = String::new();
	for (index, byte) in info.fsid.into_iter().enumerate() {
		if matches!(index, 4 | 6 | 8 | 10) {
			id.push('-');
		}
		write!(id, "{byte:02x}").unwrap();
	}
	Ok(Path::new("/sys/fs/btrfs").join(id))
}

fn validate(root: &Path) -> tg::Result<()> {
	if read(&root.join("qgroups/enabled"))? != 1 {
		return Err(tg::error!(
			"the Btrfs sandbox storage limits require enabled quotas"
		));
	}
	if read(&root.join("qgroups/inconsistent"))? != 0 {
		return Err(tg::error!(
			"the Btrfs sandbox storage limits require consistent quotas; run btrfs quota rescan"
		));
	}
	if read(&root.join("quota_override"))? != 0 {
		return Err(tg::error!(
			"the Btrfs sandbox storage limits require quota enforcement"
		));
	}
	Ok(())
}

fn read(path: &Path) -> tg::Result<u64> {
	let value = std::fs::read_to_string(path)
		.map_err(|error| tg::error!(!error, path = %path.display(), "failed to read the Btrfs quota accounting; enable quotas and expose Btrfs sysfs counters"))?;
	let value = value.trim().parse().map_err(
		|error| tg::error!(!error, path = %path.display(), "invalid Btrfs quota counter"),
	)?;
	Ok(value)
}

fn ioctl<T>(directory: &File, request: libc::Ioctl, arg: &mut T) -> std::io::Result<libc::c_int> {
	// SAFETY: Each caller supplies the initialized ABI structure matching the request, and the descriptor is valid.
	let result = unsafe { libc::ioctl(directory.as_raw_fd(), request, arg) };
	if result < 0 {
		return Err(std::io::Error::last_os_error());
	}
	Ok(result)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn structures_preserve_the_linux_abi() {
		assert_eq!(std::mem::size_of::<Subvolume>(), 4096);
		assert_eq!(std::mem::offset_of!(Subvolume, name), 8);
		assert_eq!(std::mem::size_of::<SubvolumeInfo>(), 504);
		assert_eq!(std::mem::size_of::<Info>(), 1024);
		assert_eq!(std::mem::offset_of!(Info, fsid), 16);
	}
}
