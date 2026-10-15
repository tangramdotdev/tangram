use {
	std::{
		fmt::Write as _,
		fs::File,
		os::fd::AsRawFd as _,
		path::{Path, PathBuf},
		sync::{Arc, Mutex},
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
const QGROUP_RELATION: u32 = 246;
const QUOTA_TREE: u64 = 8;
const ROOT_ITEM: u32 = 132;
const ROOT_TREE: u64 = 1;
const SUBVOL_CREATE: libc::Ioctl = libc::_IOW::<Subvolume>(MAGIC, 14);
const SYNC: libc::Ioctl = libc::_IO(MAGIC, 8);
const TREE_SEARCH: libc::Ioctl = libc::_IOWR::<Search>(MAGIC, 17);

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

// Preserve the field order required by the Linux Btrfs tree search ABI.
#[repr(C)]
struct Search {
	key: SearchKey,
	buffer: [u8; 3992],
}

#[repr(C)]
struct SearchKey {
	tree: u64,
	min_object: u64,
	max_object: u64,
	min_offset: u64,
	max_offset: u64,
	min_transaction: u64,
	max_transaction: u64,
	min_type: u32,
	max_type: u32,
	count: u32,
	unused: [u32; 9],
}

#[derive(Default)]
struct Reservation {
	limit: u64,
	released: bool,
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
			if !ready(directory, project)? {
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

pub(super) fn control(
	directory: Arc<File>,
	path: &Path,
	project: u32,
) -> tg::Result<crate::budget::Control> {
	let subvolume = File::open(path)
		.map_err(|error| tg::error!(!error, "failed to open the Btrfs sandbox subvolume"))?;
	let mut info = SubvolumeInfo {
		id: 0,
		unused: [0; 496],
	};
	ioctl(&subvolume, GET_SUBVOL_INFO, &mut info)
		.map_err(|error| tg::error!(!error, "failed to read the Btrfs sandbox subvolume ID"))?;
	let released = {
		let directory = directory.clone();
		move || reclaimed(&directory, info.id)
	};
	let limit = move |size| set_limit(&directory, project, size);
	let control = reservation(limit, released);
	Ok(control)
}

pub(super) fn ready(directory: &File, project: u32) -> tg::Result<bool> {
	validate(&sysfs(directory)?)?;
	let mut offset = 0;
	while let Some(member) = search(
		directory,
		QUOTA_TREE,
		qgroup(project),
		QGROUP_RELATION,
		offset,
	)? {
		if member >= 1_u64 << 48 {
			return Err(tg::error!(
				"the Btrfs sandbox quota group has an unexpected parent"
			));
		}
		if !reclaimed(directory, member)? {
			return Ok(false);
		}
		offset = member + 1;
	}
	Ok(true)
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

	// Estimate usage for reporting; independent counters cannot establish safe pool backing.
	let mut bytes = 0_u64;
	for name in [
		"rsv_data",
		"rsv_meta_prealloc",
		"rsv_meta_pertrans",
		"referenced",
	] {
		bytes = bytes
			.checked_add(read(&path.join(name))?)
			.ok_or_else(|| tg::error!("the Btrfs sandbox storage usage overflowed"))?;
	}
	Ok(bytes)
}

fn reservation(
	set_limit: impl Fn(u64) -> tg::Result<()> + Send + Sync + 'static,
	released: impl Fn() -> tg::Result<bool> + Send + Sync + 'static,
) -> crate::budget::Control {
	let state = Arc::new(Mutex::new(Reservation::default()));
	let limit = {
		let state = state.clone();
		Box::new(move |size| {
			let mut state = state.lock().unwrap();
			if !state.released {
				set_limit(size)?;
				state.limit = size;
			}
			Ok(())
		})
	};
	let usage = Box::new(move || {
		let mut state = state.lock().unwrap();
		state.released = state.released || released()?;
		Ok(if state.released { 0 } else { state.limit })
	});
	crate::budget::Control { limit, usage }
}

fn reclaimed(directory: &File, subvolume: u64) -> tg::Result<bool> {
	validate(&sysfs(directory)?)?;

	// Retain backing until the subvolume root has been completely dropped.
	if search(directory, ROOT_TREE, subvolume, ROOT_ITEM, 0)?.is_some() {
		return Ok(false);
	}

	// Commit delayed extent accounting before removing the obsolete quota group.
	ioctl(directory, SYNC, &mut 0_u64)
		.map_err(|error| tg::error!(!error, "failed to sync the Btrfs sandbox deletion"))?;

	// Keep simple-quota ownership alive while another subvolume still references its extents.
	let path = sysfs(directory)?
		.join("qgroups")
		.join(format!("0_{subvolume}"));
	match std::fs::read_to_string(path.join("referenced")) {
		Ok(value) => {
			let referenced: u64 = value
				.trim()
				.parse()
				.map_err(|error| tg::error!(!error, "invalid Btrfs quota counter"))?;
			if referenced != 0 {
				return Ok(false);
			}
		},
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
		Err(error) => {
			return Err(tg::error!(
				!error,
				"failed to inspect the retired Btrfs quota group"
			));
		},
	}

	let mut arg = [0, subvolume];
	match ioctl(directory, QGROUP_CREATE, &mut arg) {
		Ok(_) => Ok(true),
		Err(error) if error.raw_os_error() == Some(libc::ENOENT) => Ok(true),
		Err(error) if error.raw_os_error() == Some(libc::EBUSY) => Ok(false),
		Err(error) => Err(tg::error!(
			!error,
			"failed to retire the Btrfs sandbox quota group"
		)),
	}
}

fn search(
	directory: &File,
	tree: u64,
	object: u64,
	kind: u32,
	offset: u64,
) -> tg::Result<Option<u64>> {
	let key = SearchKey {
		tree,
		min_object: object,
		max_object: object,
		min_offset: offset,
		max_offset: u64::MAX,
		min_transaction: 0,
		max_transaction: u64::MAX,
		min_type: kind,
		max_type: kind,
		count: 1,
		unused: [0; 9],
	};
	let mut arg = Search {
		key,
		buffer: [0; 3992],
	};
	ioctl(directory, TREE_SEARCH, &mut arg).map_err(|error| {
		tg::error!(
			!error,
			"failed to inspect the Btrfs sandbox quota lifecycle"
		)
	})?;
	let offset =
		(arg.key.count != 0).then(|| u64::from_ne_bytes(arg.buffer[16..24].try_into().unwrap()));
	Ok(offset)
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
	use {
		super::*,
		std::sync::atomic::{AtomicBool, AtomicU64, Ordering},
	};

	#[test]
	fn structures_preserve_the_linux_abi() {
		assert_eq!(std::mem::size_of::<Search>(), 4096);
		assert_eq!(std::mem::size_of::<SearchKey>(), 104);
		assert_eq!(std::mem::offset_of!(Search, buffer), 104);
		assert_eq!(std::mem::size_of::<Subvolume>(), 4096);
		assert_eq!(std::mem::offset_of!(Subvolume, name), 8);
		assert_eq!(std::mem::size_of::<SubvolumeInfo>(), 504);
		assert_eq!(std::mem::size_of::<Info>(), 1024);
		assert_eq!(std::mem::offset_of!(Info, fsid), 16);
	}

	#[test]
	fn backing_survives_empty_usage_and_pending_deletion() {
		let pool = crate::budget::Pool::new(100, 1, 1);
		let first = pool.allocate(100.into()).unwrap();
		let released = Arc::new(AtomicBool::new(false));
		let limit = Arc::new(AtomicU64::new(0));
		let control = reservation(
			{
				let limit = limit.clone();
				move |value| {
					limit.store(value, Ordering::SeqCst);
					Ok(())
				}
			},
			{
				let released = released.clone();
				move || Ok(released.load(Ordering::SeqCst))
			},
		);
		first.bind(control).unwrap();

		// Reserve the granted limit even when the subvolume is empty or counters are between updates.
		assert!(pool.allocate(tg::sandbox::Resource::new(1, 100)).is_err());
		assert_eq!(limit.load(Ordering::SeqCst), 100);
		drop(first);
		assert!(pool.allocate(100.into()).is_err());

		// Release backing only after reclamation and avoid changing the quota of a reused project ID.
		released.store(true, Ordering::SeqCst);
		let second = pool.allocate(tg::sandbox::Resource::new(100, 100)).unwrap();
		assert_eq!(second.limit(), 100);
		assert_eq!(limit.load(Ordering::SeqCst), 100);
	}
}
