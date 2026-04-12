use {
	super::run::{Arg, Bind, Overlay},
	bytes::Bytes,
	num::ToPrimitive,
	std::{
		ffi::{CString, OsStr},
		os::unix::ffi::OsStrExt as _,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

pub fn apply(arg: &Arg, root: Option<&Path>) -> tg::Result<()> {
	make_mounts_private()?;
	if let Some(root) = root {
		std::fs::create_dir_all(root).map_err(|source| {
			tg::error!(
				!source,
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
		mount_bind(bind, &target, readonly)?;
	}

	Ok(())
}

pub fn pivot_root_into(root: &Path) -> tg::Result<()> {
	let put_old = root.join(".pivot_root");
	std::fs::create_dir_all(&put_old).map_err(|source| {
		tg::error!(
			!source,
			path = %put_old.display(),
			"failed to create the pivot_root staging directory"
		)
	})?;
	change_directory(root)?;
	let result =
		unsafe { libc::syscall(libc::SYS_pivot_root, c".".as_ptr(), c".pivot_root".as_ptr()) };
	if result != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, root = %root.display(), "pivot_root failed"));
	}
	change_directory(Path::new("/"))?;
	let result = unsafe { libc::umount2(c"/.pivot_root".as_ptr(), libc::MNT_DETACH) };
	if result != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to unmount the old root"));
	}
	std::fs::remove_dir("/.pivot_root")
		.map_err(|source| tg::error!(!source, "failed to remove the old root mountpoint"))?;
	Ok(())
}

pub fn change_directory(path: &Path) -> tg::Result<()> {
	let ret = unsafe { libc::chdir(cstring(path.as_os_str()).as_ptr()) };
	if ret != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(
			!source,
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
		let suffix = target.strip_prefix("/").map_err(|source| {
			tg::error!(
				!source,
				path = %target.display(),
				"expected an absolute target path"
			)
		})?;
		Ok(root.join(suffix))
	} else {
		Ok(target.to_owned())
	}
}

fn mount_bind(bind: &Bind, target: &Path, readonly: bool) -> tg::Result<()> {
	create_mountpoint_if_not_exists(&bind.source, target).map_err(|source| {
		tg::error!(
			!source,
			source = %bind.source.display(),
			target = %target.display(),
			"failed to create the bind mountpoint"
		)
	})?;
	let source = cstring(&bind.source);
	let target_path = target;
	let target = cstring(target_path);
	let mut flags = libc::MS_BIND | libc::MS_REC;
	flags |= get_existing_mount_flags(&source).unwrap_or(0);
	mount_raw(Some(&source), &target, None, flags, std::ptr::null_mut()).map_err(|source| {
		tg::error!(
			!source,
			source = %bind.source.display(),
			target = %target_path.display(),
			"failed to create the bind mount"
		)
	})?;
	if readonly {
		let flags = flags | libc::MS_REMOUNT | libc::MS_RDONLY;
		mount_raw(Some(&source), &target, None, flags, std::ptr::null_mut()).map_err(|source| {
			tg::error!(
				!source,
				source = %bind.source.display(),
				target = %target_path.display(),
				"failed to remount the bind mount as read only"
			)
		})?;
	}
	Ok(())
}

fn mount_overlay(lowerdirs: &[PathBuf], overlay: &Overlay, target: &Path) -> tg::Result<()> {
	if lowerdirs.is_empty() {
		return Err(tg::error!(
			"an overlay requires at least one overlay source"
		));
	}
	std::fs::create_dir_all(target).map_err(|source| {
		tg::error!(
			!source,
			path = %target.display(),
			"failed to create the overlay target"
		)
	})?;
	std::fs::create_dir_all(&overlay.upperdir).map_err(|source| {
		tg::error!(
			!source,
			path = %overlay.upperdir.display(),
			"failed to create the overlay upperdir"
		)
	})?;
	std::fs::create_dir_all(&overlay.workdir).map_err(|source| {
		tg::error!(
			!source,
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
		0,
		data.as_ptr().cast::<std::ffi::c_void>().cast_mut(),
	)
	.map_err(|source| {
		tg::error!(
			!source,
			target = %overlay.target.display(),
			"failed to create the overlay mount"
		)
	})?;
	Ok(())
}

fn mount_proc(target: &Path) -> tg::Result<()> {
	std::fs::create_dir_all(target).map_err(|source| {
		tg::error!(
			!source,
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
	.map_err(|source| tg::error!(!source, "failed to create the proc mount"))?;
	Ok(())
}

fn mount_tmpfs(target: &Path) -> tg::Result<()> {
	std::fs::create_dir_all(target).map_err(|source| {
		tg::error!(
			!source,
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
	.map_err(|source| tg::error!(!source, "failed to create the tmpfs mount"))?;
	Ok(())
}

fn mount_dev(target: &Path) -> tg::Result<()> {
	std::fs::create_dir_all(target).map_err(|source| {
		tg::error!(
			!source,
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
	.map_err(|source| tg::error!(!source, "failed to create the dev mount"))?;
	let pts = target.join("pts");
	std::fs::create_dir_all(&pts)
		.map_err(|source| tg::error!(!source, "failed to create the devpts mountpoint"))?;
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
	.map_err(|source| tg::error!(!source, "failed to create the devpts mount"))?;
	for source in [
		"/dev/null",
		"/dev/zero",
		"/dev/full",
		"/dev/random",
		"/dev/urandom",
		"/dev/tty",
	] {
		let source = Path::new(source);
		let target = target.join(source.file_name().unwrap());
		mount_bind(
			&Bind {
				source: source.to_owned(),
				target: target.clone(),
			},
			&target,
			false,
		)?;
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
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(
			!source,
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
		.map_err(|source| tg::error!(!source, "failed to create /dev/fd"))?;
	std::os::unix::fs::symlink("../proc/self/fd/0", target.join("stdin"))
		.map_err(|source| tg::error!(!source, "failed to create /dev/stdin"))?;
	std::os::unix::fs::symlink("../proc/self/fd/1", target.join("stdout"))
		.map_err(|source| tg::error!(!source, "failed to create /dev/stdout"))?;
	std::os::unix::fs::symlink("../proc/self/fd/2", target.join("stderr"))
		.map_err(|source| tg::error!(!source, "failed to create /dev/stderr"))?;
	std::os::unix::fs::symlink("pts/ptmx", target.join("ptmx"))
		.map_err(|source| tg::error!(!source, "failed to create /dev/ptmx"))?;
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
