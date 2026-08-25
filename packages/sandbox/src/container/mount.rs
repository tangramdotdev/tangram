use {
	super::run::{Arg, Bind, Overlay},
	bytes::Bytes,
	num::ToPrimitive,
	std::{
		cmp::Reverse,
		ffi::{CString, OsStr, OsString},
		os::{
			fd::AsRawFd as _,
			unix::{
				ffi::{OsStrExt as _, OsStringExt as _},
				fs::OpenOptionsExt as _,
			},
		},
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

const AT_RECURSIVE: libc::c_uint = 0x8000;
const MOUNT_ATTR_NODEV: u64 = 0x0000_0004;
const MOUNT_ATTR_NOSUID: u64 = 0x0000_0002;
const MOUNT_ATTR_RDONLY: u64 = 0x0000_0001;

#[derive(Clone, Copy, Debug, Default)]
struct MountAttributes {
	nodev: bool,
	nosuid: bool,
	readonly: bool,
}

pub fn apply(arg: &Arg, root: Option<&Path>) -> tg::Result<()> {
	make_mounts_private()?;
	if let Some(root) = root {
		std::fs::create_dir_all(root).map_err(|error| {
			tg::error!(
				!error,
				path = %root.display(),
				"failed to create the root mountpoint"
			)
		})?;
	}

	let mut overlays = arg.overlays.iter().collect::<Vec<_>>();
	overlays.sort_unstable_by_key(|overlay| path_depth(&overlay.target));
	if let Some(overlay) = overlays
		.iter()
		.find(|overlay| overlay.target == Path::new("/"))
	{
		let root = root.ok_or_else(|| tg::error!("an overlay to / requires a scratch path"))?;
		mount_overlay(&arg.overlay_sources, overlay, root)?;
	}

	let mut tmpfs = arg.tmpfs.iter().collect::<Vec<_>>();
	tmpfs.sort_unstable_by_key(|path| path_depth(path));
	for target in tmpfs {
		mount_tmpfs(&map_target(root, target)?)?;
	}

	let mut devs = arg.devs.iter().collect::<Vec<_>>();
	devs.sort_unstable_by_key(|path| path_depth(path));
	for target in devs {
		mount_dev(&map_target(root, target)?)?;
	}

	let mut procs = arg.procs.iter().collect::<Vec<_>>();
	procs.sort_unstable_by_key(|path| path_depth(path));
	for target in procs {
		mount_proc(&map_target(root, target)?)?;
	}

	for overlay in overlays
		.into_iter()
		.filter(|overlay| overlay.target != Path::new("/"))
	{
		let target = map_target(root, &overlay.target)?;
		mount_overlay(&arg.overlay_sources, overlay, &target)?;
	}

	let mut binds = arg
		.binds
		.iter()
		.map(|bind| (bind, false))
		.chain(arg.ro_binds.iter().map(|bind| (bind, true)))
		.collect::<Vec<_>>();
	binds.sort_unstable_by_key(|(bind, _)| path_depth(&bind.target));
	for (bind, readonly) in binds {
		let target = map_target(root, &bind.target)?;
		let attributes = MountAttributes {
			nodev: arg.unshare_all,
			nosuid: arg.unshare_all,
			readonly,
		};
		mount_bind(bind, &target, attributes)?;
	}

	Ok(())
}

pub fn pivot_root_into(root: &Path) -> tg::Result<()> {
	let put_old = root.join(".pivot_root");
	std::fs::create_dir_all(&put_old).map_err(|error| {
		tg::error!(
			!error,
			path = %put_old.display(),
			"failed to create the pivot_root staging directory"
		)
	})?;
	change_directory(root)?;
	let result =
		unsafe { libc::syscall(libc::SYS_pivot_root, c".".as_ptr(), c".pivot_root".as_ptr()) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, root = %root.display(), "pivot_root failed"));
	}
	change_directory(Path::new("/"))?;
	let result = unsafe { libc::umount2(c"/.pivot_root".as_ptr(), libc::MNT_DETACH) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to unmount the old root"));
	}
	std::fs::remove_dir("/.pivot_root")
		.map_err(|error| tg::error!(!error, "failed to remove the old root mountpoint"))?;
	Ok(())
}

pub fn change_directory(path: &Path) -> tg::Result<()> {
	let ret = unsafe { libc::chdir(cstring(path.as_os_str()).as_ptr()) };
	if ret != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(
			!error,
			path = %path.display(),
			"failed to change directories"
		));
	}
	Ok(())
}

fn map_target(root: Option<&Path>, target: &Path) -> tg::Result<PathBuf> {
	if let Some(root) = root {
		if target == Path::new("/") {
			return Ok(root.to_owned());
		}
		let suffix = target.strip_prefix("/").map_err(|error| {
			tg::error!(
				!error,
				path = %target.display(),
				"expected an absolute target path"
			)
		})?;
		let target = root.join(suffix);
		validate_target_path(root, &target)?;
		Ok(target)
	} else {
		Ok(target.to_owned())
	}
}

fn validate_target_path(root: &Path, target: &Path) -> tg::Result<()> {
	let suffix = target.strip_prefix(root).unwrap();
	let mut path = root.to_owned();
	for component in suffix.components() {
		path.push(component);
		match std::fs::symlink_metadata(&path) {
			Ok(metadata) if metadata.file_type().is_symlink() => {
				return Err(tg::error!(
					path = %path.display(),
					"mount targets may not traverse symbolic links"
				));
			},
			Ok(_) => {},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
			Err(error) => {
				return Err(tg::error!(
					!error,
					path = %path.display(),
					"failed to inspect a mount target"
				));
			},
		}
	}
	Ok(())
}

fn mount_bind(bind: &Bind, target: &Path, attributes: MountAttributes) -> tg::Result<()> {
	create_mountpoint_if_not_exists(&bind.source, target).map_err(|error| {
		tg::error!(
			!error,
			error = %bind.source.display(),
			target = %target.display(),
			"failed to create the bind mountpoint"
		)
	})?;
	let source = cstring(&bind.source);
	let target_path = target;
	let target = cstring(target_path);
	let flags = libc::MS_BIND | libc::MS_REC;
	mount_raw(Some(&source), &target, None, flags, std::ptr::null_mut()).map_err(|error| {
		tg::error!(
			!error,
			error = %bind.source.display(),
			target = %target_path.display(),
			"failed to create the bind mount"
		)
	})?;
	set_mount_attributes(target_path, attributes)?;
	Ok(())
}

fn set_mount_attributes(target: &Path, attributes: MountAttributes) -> tg::Result<()> {
	let mut attribute_set = 0;
	if attributes.nodev {
		attribute_set |= MOUNT_ATTR_NODEV;
	}
	if attributes.nosuid {
		attribute_set |= MOUNT_ATTR_NOSUID;
	}
	if attributes.readonly {
		attribute_set |= MOUNT_ATTR_RDONLY;
	}
	if attribute_set == 0 {
		return Ok(());
	}

	match set_mount_attributes_modern(target, attribute_set) {
		Ok(()) => Ok(()),
		Err(error)
			if matches!(
				error.raw_os_error(),
				Some(libc::ENOSYS | libc::EINVAL | libc::EOPNOTSUPP | libc::EPERM)
			) =>
		{
			set_mount_attributes_legacy(target, attributes)
		},
		Err(error) => Err(tg::error!(
			!error,
			target = %target.display(),
			"failed to set recursive mount attributes"
		)),
	}
}

fn set_mount_attributes_modern(target: &Path, attribute_set: u64) -> std::io::Result<()> {
	let target = cstring(target);
	// The array matches Linux's mount_attr ABI: attr_set, attr_clr, propagation, and userns_fd.
	let attributes = [attribute_set, 0, 0, 0];
	// SAFETY: The target and attribute pointers remain valid for the duration of the syscall.
	let result = unsafe {
		libc::syscall(
			libc::SYS_mount_setattr,
			libc::AT_FDCWD,
			target.as_ptr(),
			AT_RECURSIVE,
			attributes.as_ptr(),
			std::mem::size_of_val(&attributes),
		)
	};
	if result != 0 {
		return Err(std::io::Error::last_os_error());
	}
	Ok(())
}

fn set_mount_attributes_legacy(target: &Path, attributes: MountAttributes) -> tg::Result<()> {
	let mut mountpoints = mountpoints_under(target)?;
	mountpoints.sort_unstable_by_key(|path| Reverse(path_depth(path)));
	if mountpoints.is_empty() {
		return Err(tg::error!(
			target = %target.display(),
			"failed to find the bind mount in mountinfo"
		));
	}
	for mountpoint in mountpoints {
		let mountpoint_cstring = cstring(&mountpoint);
		let mut flags = get_existing_mount_flags(&mountpoint_cstring).map_err(|error| {
			tg::error!(
				!error,
				path = %mountpoint.display(),
				"failed to get the existing mount attributes"
			)
		})?;
		flags |= libc::MS_BIND | libc::MS_REMOUNT;
		if attributes.nodev {
			flags |= libc::MS_NODEV;
		}
		if attributes.nosuid {
			flags |= libc::MS_NOSUID;
		}
		if attributes.readonly {
			flags |= libc::MS_RDONLY;
		}
		mount_raw(None, &mountpoint_cstring, None, flags, std::ptr::null_mut()).map_err(
			|error| {
				tg::error!(
					!error,
					path = %mountpoint.display(),
					"failed to set the mount attributes"
				)
			},
		)?;
	}
	Ok(())
}

fn mountpoints_under(target: &Path) -> tg::Result<Vec<PathBuf>> {
	let mountinfo = std::fs::read("/proc/self/mountinfo")
		.map_err(|error| tg::error!(!error, "failed to read mountinfo"))?;
	let mut mountpoints = Vec::new();
	for line in mountinfo.split(|byte| *byte == b'\n') {
		let Some(field) = line.split(|byte| *byte == b' ').nth(4) else {
			continue;
		};
		let mountpoint = PathBuf::from(decode_mountinfo_path(field)?);
		if mountpoint == target || mountpoint.starts_with(target) {
			mountpoints.push(mountpoint);
		}
	}
	Ok(mountpoints)
}

fn decode_mountinfo_path(input: &[u8]) -> tg::Result<OsString> {
	let mut output = Vec::with_capacity(input.len());
	let mut index = 0;
	while index < input.len() {
		if input[index] != b'\\' {
			output.push(input[index]);
			index += 1;
			continue;
		}
		let digits = input
			.get(index + 1..index + 4)
			.ok_or_else(|| tg::error!("invalid escape in mountinfo"))?;
		if !digits.iter().all(|digit| (b'0'..=b'7').contains(digit)) {
			return Err(tg::error!("invalid escape in mountinfo"));
		}
		let byte = (digits[0] - b'0') * 64 + (digits[1] - b'0') * 8 + digits[2] - b'0';
		output.push(byte);
		index += 4;
	}
	Ok(OsString::from_vec(output))
}

fn mount_overlay(lowerdirs: &[PathBuf], overlay: &Overlay, target: &Path) -> tg::Result<()> {
	if lowerdirs.is_empty() {
		return Err(tg::error!(
			"an overlay requires at least one overlay source"
		));
	}
	std::fs::create_dir_all(target).map_err(|error| {
		tg::error!(
			!error,
			path = %target.display(),
			"failed to create the overlay target"
		)
	})?;
	std::fs::create_dir_all(&overlay.upperdir).map_err(|error| {
		tg::error!(
			!error,
			path = %overlay.upperdir.display(),
			"failed to create the overlay upperdir"
		)
	})?;
	std::fs::create_dir_all(&overlay.workdir).map_err(|error| {
		tg::error!(
			!error,
			path = %overlay.workdir.display(),
			"failed to create the overlay workdir"
		)
	})?;
	let source = cstring("overlay");
	let target = cstring(target);
	let fstype = cstring("overlay");
	let data = overlay_mount_data(lowerdirs, &overlay.upperdir, &overlay.workdir);
	mount_raw(
		Some(&source),
		&target,
		Some(&fstype),
		libc::MS_NODEV | libc::MS_NOSUID,
		data.as_ptr().cast::<std::ffi::c_void>().cast_mut(),
	)
	.map_err(|error| {
		tg::error!(
			!error,
			target = %overlay.target.display(),
			"failed to create the overlay mount"
		)
	})?;
	Ok(())
}

fn mount_proc(target: &Path) -> tg::Result<()> {
	std::fs::create_dir_all(target).map_err(|error| {
		tg::error!(
			!error,
			path = %target.display(),
			"failed to create the proc mountpoint"
		)
	})?;
	let source = cstring("proc");
	let target = cstring(target);
	let fstype = cstring("proc");
	mount_raw(
		Some(&source),
		&target,
		Some(&fstype),
		libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
		std::ptr::null_mut(),
	)
	.map_err(|error| tg::error!(!error, "failed to create the proc mount"))?;
	Ok(())
}

fn mount_tmpfs(target: &Path) -> tg::Result<()> {
	std::fs::create_dir_all(target).map_err(|error| {
		tg::error!(
			!error,
			path = %target.display(),
			"failed to create the tmpfs mountpoint"
		)
	})?;
	let source = cstring("tmpfs");
	let target = cstring(target);
	let fstype = cstring("tmpfs");
	let data = cstring("mode=0755");
	mount_raw(
		Some(&source),
		&target,
		Some(&fstype),
		libc::MS_NOSUID | libc::MS_NODEV,
		data.as_ptr().cast::<std::ffi::c_void>().cast_mut(),
	)
	.map_err(|error| tg::error!(!error, "failed to create the tmpfs mount"))?;
	Ok(())
}

fn mount_dev(target: &Path) -> tg::Result<()> {
	let mut devices = Vec::new();
	for path in [
		"/dev/null",
		"/dev/zero",
		"/dev/full",
		"/dev/random",
		"/dev/urandom",
		"/dev/tty",
	] {
		let file = std::fs::File::options()
			.custom_flags(libc::O_PATH)
			.read(true)
			.open(path)
			.map_err(|error| tg::error!(!error, %path, "failed to open the device"))?;
		devices.push((path, file));
	}

	std::fs::create_dir_all(target).map_err(|error| {
		tg::error!(
			!error,
			path = %target.display(),
			"failed to create the dev mountpoint"
		)
	})?;
	let source = cstring("tmpfs");
	let target_cstring = cstring(target);
	let fstype = cstring("tmpfs");
	let data = cstring("mode=0755,size=64k");
	mount_raw(
		Some(&source),
		&target_cstring,
		Some(&fstype),
		libc::MS_NOSUID | libc::MS_STRICTATIME,
		data.as_ptr().cast::<std::ffi::c_void>().cast_mut(),
	)
	.map_err(|error| tg::error!(!error, "failed to create the dev mount"))?;
	let pts = target.join("pts");
	std::fs::create_dir_all(&pts)
		.map_err(|error| tg::error!(!error, "failed to create the devpts mountpoint"))?;
	let pts_source = cstring("devpts");
	let pts_target = cstring(&pts);
	let pts_fstype = cstring("devpts");
	let pts_data = cstring("newinstance,ptmxmode=0666,mode=0620");
	mount_raw(
		Some(&pts_source),
		&pts_target,
		Some(&pts_fstype),
		libc::MS_NOSUID | libc::MS_NOEXEC,
		pts_data.as_ptr().cast::<std::ffi::c_void>().cast_mut(),
	)
	.map_err(|error| tg::error!(!error, "failed to create the devpts mount"))?;

	let shm = target.join("shm");
	std::fs::create_dir_all(&shm)
		.map_err(|error| tg::error!(!error, "failed to create the shm mountpoint"))?;
	let shm_source = cstring("tmpfs");
	let shm_target = cstring(&shm);
	let shm_fstype = cstring("tmpfs");
	let shm_data = cstring("mode=1777");
	mount_raw(
		Some(&shm_source),
		&shm_target,
		Some(&shm_fstype),
		libc::MS_NODEV | libc::MS_NOSUID,
		shm_data.as_ptr().cast::<std::ffi::c_void>().cast_mut(),
	)
	.map_err(|error| tg::error!(!error, "failed to create the shm mount"))?;

	for (path, file) in &devices {
		let source = PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()));
		let target = target.join(Path::new(path).file_name().unwrap());
		let entry = Bind {
			source,
			target: target.clone(),
		};
		mount_bind(&entry, &target, MountAttributes::default())?;
	}

	configure_dev(target)
}

fn make_mounts_private() -> tg::Result<()> {
	let result = unsafe {
		libc::mount(
			std::ptr::null(),
			c"/".as_ptr(),
			std::ptr::null(),
			libc::MS_REC | libc::MS_PRIVATE,
			std::ptr::null(),
		)
	};
	if result < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(
			!error,
			"failed to make the mount namespace private"
		));
	}
	Ok(())
}

fn configure_dev(target: &Path) -> tg::Result<()> {
	for name in ["fd", "stdin", "stdout", "stderr", "ptmx"] {
		let path = target.join(name);
		if path.exists() {
			std::fs::remove_file(&path).ok();
		}
	}
	std::os::unix::fs::symlink("../proc/self/fd", target.join("fd"))
		.map_err(|error| tg::error!(!error, "failed to create /dev/fd"))?;
	std::os::unix::fs::symlink("../proc/self/fd/0", target.join("stdin"))
		.map_err(|error| tg::error!(!error, "failed to create /dev/stdin"))?;
	std::os::unix::fs::symlink("../proc/self/fd/1", target.join("stdout"))
		.map_err(|error| tg::error!(!error, "failed to create /dev/stdout"))?;
	std::os::unix::fs::symlink("../proc/self/fd/2", target.join("stderr"))
		.map_err(|error| tg::error!(!error, "failed to create /dev/stderr"))?;
	std::os::unix::fs::symlink("pts/ptmx", target.join("ptmx"))
		.map_err(|error| tg::error!(!error, "failed to create /dev/ptmx"))?;
	Ok(())
}

fn create_mountpoint_if_not_exists(
	source: impl AsRef<Path>,
	target: impl AsRef<Path>,
) -> std::io::Result<()> {
	let source = source.as_ref();
	let is_dir = source.is_dir();
	if is_dir {
		std::fs::create_dir_all(target)?;
	} else {
		let target = target.as_ref();
		if target.exists() {
			return Ok(());
		}
		if let Some(parent) = target.parent() {
			std::fs::create_dir_all(parent)?;
		}
		std::fs::File::create(target)?;
	}
	Ok(())
}

fn overlay_mount_data(lowerdirs: &[PathBuf], upperdir: &Path, workdir: &Path) -> Bytes {
	fn escape(out: &mut Vec<u8>, path: &[u8]) {
		for byte in path.iter().copied() {
			if byte == 0 {
				break;
			}
			if byte == b':' {
				out.push(b'\\');
			}
			out.push(byte);
		}
	}

	let mut data = Vec::new();
	data.extend_from_slice(b"xino=off,userxattr,lowerdir=");
	for (index, dir) in lowerdirs.iter().enumerate() {
		escape(&mut data, dir.as_os_str().as_bytes());
		if index + 1 != lowerdirs.len() {
			data.push(b':');
		}
	}
	data.extend_from_slice(b",upperdir=");
	data.extend_from_slice(upperdir.as_os_str().as_bytes());
	data.extend_from_slice(b",workdir=");
	data.extend_from_slice(workdir.as_os_str().as_bytes());
	data.push(0);
	data.into()
}

fn mount_raw(
	source: Option<&CString>,
	target: &CString,
	fstype: Option<&CString>,
	flags: u64,
	data: *mut std::ffi::c_void,
) -> std::io::Result<()> {
	let source = source.map_or(std::ptr::null(), |value| value.as_ptr());
	let fstype = fstype.map_or(std::ptr::null(), |value| value.as_ptr());
	let result = unsafe { libc::mount(source, target.as_ptr(), fstype, flags, data) };
	if result != 0 {
		return Err(std::io::Error::last_os_error());
	}
	Ok(())
}

fn get_existing_mount_flags(path: &CString) -> std::io::Result<libc::c_ulong> {
	const ST_RELATIME: u64 = 0x400;
	const FLAGS: [(u64, u64); 7] = [
		(libc::MS_RDONLY, libc::ST_RDONLY),
		(libc::MS_NODEV, libc::ST_NODEV),
		(libc::MS_NOEXEC, libc::ST_NOEXEC),
		(libc::MS_NOSUID, libc::ST_NOSUID),
		(libc::MS_NOATIME, libc::ST_NOATIME),
		(libc::MS_RELATIME, ST_RELATIME),
		(libc::MS_NODIRATIME, libc::ST_NODIRATIME),
	];
	let statfs = unsafe {
		let mut statfs = std::mem::MaybeUninit::zeroed();
		let ret = libc::statfs64(path.as_ptr(), statfs.as_mut_ptr());
		if ret != 0 {
			return Err(std::io::Error::last_os_error());
		}
		statfs.assume_init()
	};
	let mut flags = 0;
	for (mount_flag, stat_flag) in FLAGS {
		if (statfs.f_flags.to_u64().unwrap() & stat_flag) != 0 {
			flags |= mount_flag;
		}
	}
	Ok(flags)
}

fn path_depth(path: &Path) -> usize {
	path.components().count()
}

fn cstring(value: impl AsRef<OsStr>) -> CString {
	CString::new(value.as_ref().as_bytes()).unwrap()
}
