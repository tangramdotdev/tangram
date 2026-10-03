use {
	super::run::{Arg, Bind, Overlay},
	bytes::Bytes,
	std::{
		ffi::{CString, OsStr},
		os::{
			fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd},
			unix::{ffi::OsStrExt as _, fs::OpenOptionsExt as _},
		},
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

const AT_RECURSIVE: libc::c_uint = 0x8000;
const RESOLVE_BENEATH: u64 = 0x08;
const RESOLVE_NO_MAGICLINKS: u64 = 0x02;
const RESOLVE_NO_SYMLINKS: u64 = 0x04;
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
	let mount_root = open_mount_root(root)?;

	let mut tmpfs = arg.tmpfs.iter().collect::<Vec<_>>();
	tmpfs.sort_unstable_by_key(|path| path_depth(path));
	for target in tmpfs {
		mount_tmpfs(&map_path_target(root, target)?)?;
	}

	let mut devs = arg.devs.iter().collect::<Vec<_>>();
	devs.sort_unstable_by_key(|path| path_depth(path));
	for target in devs {
		mount_dev(&map_path_target(root, target)?)?;
	}

	let mut procs = arg.procs.iter().collect::<Vec<_>>();
	procs.sort_unstable_by_key(|path| path_depth(path));
	for target in procs {
		mount_proc(&map_path_target(root, target)?)?;
	}

	if arg.cgroup.is_some() {
		mount_cgroup(
			&map_path_target(root, Path::new("/sys/fs/cgroup"))?,
			arg.cgroup_readonly,
		)?;
	}

	for overlay in overlays
		.into_iter()
		.filter(|overlay| overlay.target != Path::new("/"))
	{
		let target = map_path_target(root, &overlay.target)?;
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
		mount_bind(bind, mount_root.as_raw_fd(), &target, attributes)?;
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
		Ok(root.join(suffix))
	} else {
		Ok(target.to_owned())
	}
}

fn map_path_target(root: Option<&Path>, target: &Path) -> tg::Result<PathBuf> {
	let target = map_target(root, target)?;
	if let Some(root) = root {
		validate_target_path(root, &target)?;
	}
	Ok(target)
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

fn mount_bind(
	bind: &Bind,
	root: RawFd,
	target_path: &Path,
	attributes: MountAttributes,
) -> tg::Result<()> {
	let source = open_absolute_path(&bind.source).map_err(|error| {
		tg::error!(
			!error,
			path = %bind.source.display(),
			"failed to securely open the bind source"
		)
	})?;
	let directory = fd_is_directory(source.as_raw_fd()).map_err(|error| {
		tg::error!(
			!error,
			path = %bind.source.display(),
			"failed to inspect the bind source"
		)
	})?;
	let target = bind.target.strip_prefix("/").unwrap();
	let target_fd = create_mount_target(root, target, directory).map_err(|error| {
		tg::error!(
			!error,
			path = %bind.target.display(),
			"failed to securely create the bind target"
		)
	})?;
	mount_bind_modern(
		source.as_raw_fd(),
		target_fd.as_raw_fd(),
		directory,
		attributes,
	)
	.map_err(|error| {
		tg::error!(
			!error,
			source = %bind.source.display(),
			target = %target_path.display(),
			"failed to create the bind mount"
		)
	})?;
	Ok(())
}

fn mount_bind_path(bind: &Bind, target: &Path) -> tg::Result<()> {
	create_mountpoint_if_not_exists(&bind.source, target).map_err(|error| {
		tg::error!(!error, source = %bind.source.display(), target = %target.display(), "failed to create the bind mountpoint")
	})?;
	let source = cstring(&bind.source);
	let target_cstring = cstring(target);
	let flags = libc::MS_BIND | libc::MS_REC;
	mount_raw(
		Some(&source),
		&target_cstring,
		None,
		flags,
		std::ptr::null_mut(),
	)
	.map_err(|error| {
		tg::error!(!error, source = %bind.source.display(), target = %target.display(), "failed to create the bind mount")
	})?;
	Ok(())
}

fn open_mount_root(root: Option<&Path>) -> tg::Result<OwnedFd> {
	let path = root.unwrap_or_else(|| Path::new("/"));
	open_absolute_path(path).map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to securely open the mount root"
		)
	})
}

fn open_absolute_path(path: &Path) -> std::io::Result<OwnedFd> {
	let suffix = path
		.strip_prefix("/")
		.map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
	let root = std::fs::OpenOptions::new()
		.custom_flags(libc::O_PATH | libc::O_CLOEXEC)
		.read(true)
		.open("/")?;
	let suffix = if suffix.as_os_str().is_empty() {
		Path::new(".")
	} else {
		suffix
	};
	openat2(
		root.as_raw_fd(),
		suffix,
		libc::O_PATH | libc::O_CLOEXEC,
		RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS | RESOLVE_NO_SYMLINKS,
	)
}

fn create_mount_target(root: RawFd, target: &Path, directory: bool) -> std::io::Result<OwnedFd> {
	let components = target
		.components()
		.map(|component| match component {
			std::path::Component::Normal(component) => Ok(component),
			_ => Err(std::io::Error::from_raw_os_error(libc::EINVAL)),
		})
		.collect::<std::io::Result<Vec<_>>>()?;
	let (name, parents) = components
		.split_last()
		.ok_or_else(|| std::io::Error::from_raw_os_error(libc::EINVAL))?;
	let mut parent = duplicate_fd(root)?;
	for component in parents {
		parent = open_or_create_directory(parent.as_raw_fd(), component)?;
	}
	if directory {
		open_or_create_directory(parent.as_raw_fd(), name)
	} else {
		open_or_create_file(parent.as_raw_fd(), name)
	}
}

fn open_or_create_directory(parent: RawFd, name: &OsStr) -> std::io::Result<OwnedFd> {
	match openat2(
		parent,
		Path::new(name),
		libc::O_DIRECTORY | libc::O_PATH | libc::O_CLOEXEC,
		RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS | RESOLVE_NO_SYMLINKS,
	) {
		Ok(fd) => Ok(fd),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
			let name_cstring = cstring(name);
			// SAFETY: The parent descriptor and component string are valid for the syscall.
			let result = unsafe { libc::mkdirat(parent, name_cstring.as_ptr(), 0o755) };
			if result != 0 {
				let error = std::io::Error::last_os_error();
				if error.kind() != std::io::ErrorKind::AlreadyExists {
					return Err(error);
				}
			}
			openat2(
				parent,
				Path::new(name),
				libc::O_DIRECTORY | libc::O_PATH | libc::O_CLOEXEC,
				RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS | RESOLVE_NO_SYMLINKS,
			)
		},
		Err(error) => Err(error),
	}
}

fn open_or_create_file(parent: RawFd, name: &OsStr) -> std::io::Result<OwnedFd> {
	match openat2(
		parent,
		Path::new(name),
		libc::O_PATH | libc::O_CLOEXEC,
		RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS | RESOLVE_NO_SYMLINKS,
	) {
		Ok(fd) if !fd_is_directory(fd.as_raw_fd())? => Ok(fd),
		Ok(_) => Err(std::io::Error::from_raw_os_error(libc::EISDIR)),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
			let name_cstring = cstring(name);
			// SAFETY: The parent descriptor and component string are valid for the syscall.
			let fd = unsafe {
				libc::openat(
					parent,
					name_cstring.as_ptr(),
					libc::O_WRONLY
						| libc::O_CREAT | libc::O_EXCL
						| libc::O_CLOEXEC | libc::O_NOFOLLOW,
					0o644,
				)
			};
			if fd >= 0 {
				// SAFETY: A nonnegative result from openat is a newly owned descriptor.
				return Ok(unsafe { OwnedFd::from_raw_fd(fd) });
			}
			let error = std::io::Error::last_os_error();
			if error.kind() != std::io::ErrorKind::AlreadyExists {
				return Err(error);
			}
			let fd = openat2(
				parent,
				Path::new(name),
				libc::O_PATH | libc::O_CLOEXEC,
				RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS | RESOLVE_NO_SYMLINKS,
			)?;
			if fd_is_directory(fd.as_raw_fd())? {
				return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
			}
			Ok(fd)
		},
		Err(error) => Err(error),
	}
}

fn openat2(
	parent: RawFd,
	path: &Path,
	flags: libc::c_int,
	resolve: u64,
) -> std::io::Result<OwnedFd> {
	let path = cstring(path);
	// SAFETY: The open_how type is valid when zero initialized.
	let mut how: libc::open_how = unsafe { std::mem::zeroed() };
	how.flags = flags.try_into().unwrap();
	how.resolve = resolve;
	// SAFETY: The path and open_how pointers remain valid for the syscall.
	let fd = unsafe {
		libc::syscall(
			libc::SYS_openat2,
			parent,
			path.as_ptr(),
			&raw const how,
			std::mem::size_of::<libc::open_how>(),
		)
	};
	if fd < 0 {
		return Err(std::io::Error::last_os_error());
	}
	// SAFETY: A nonnegative result from openat2 is a newly owned descriptor.
	Ok(unsafe { OwnedFd::from_raw_fd(fd.try_into().unwrap()) })
}

fn duplicate_fd(fd: RawFd) -> std::io::Result<OwnedFd> {
	// SAFETY: fcntl does not retain the descriptor argument.
	let fd = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
	if fd < 0 {
		return Err(std::io::Error::last_os_error());
	}
	// SAFETY: F_DUPFD_CLOEXEC returned a newly owned descriptor.
	Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn fd_is_directory(fd: RawFd) -> std::io::Result<bool> {
	let mut stat = std::mem::MaybeUninit::zeroed();
	// SAFETY: The stat buffer is writable and valid for the syscall.
	let result = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
	if result != 0 {
		return Err(std::io::Error::last_os_error());
	}
	// SAFETY: A successful fstat initialized the complete stat buffer.
	let stat = unsafe { stat.assume_init() };
	Ok(stat.st_mode & libc::S_IFMT == libc::S_IFDIR)
}

fn mount_bind_modern(
	source: RawFd,
	target: RawFd,
	recursive: bool,
	attributes: MountAttributes,
) -> std::io::Result<()> {
	let recursive_flag = if recursive { AT_RECURSIVE } else { 0 };
	let flags = libc::OPEN_TREE_CLONE | libc::OPEN_TREE_CLOEXEC | libc::AT_EMPTY_PATH as u32;
	let flags = flags | recursive_flag;
	// SAFETY: The empty path selects the valid source descriptor for the syscall.
	let mount = unsafe { libc::syscall(libc::SYS_open_tree, source, c"".as_ptr(), flags) };
	if mount < 0 {
		return Err(std::io::Error::last_os_error());
	}
	// SAFETY: A nonnegative result from open_tree is a newly owned descriptor.
	let mount = unsafe { OwnedFd::from_raw_fd(mount.try_into().unwrap()) };
	let attributes = mount_attributes(attributes);
	if attributes != 0 {
		let attributes = [attributes, 0, 0, 0];
		let flags = libc::AT_EMPTY_PATH as u32 | recursive_flag;
		// SAFETY: The attribute array remains valid for the syscall.
		let result = unsafe {
			libc::syscall(
				libc::SYS_mount_setattr,
				mount.as_raw_fd(),
				c"".as_ptr(),
				flags,
				attributes.as_ptr(),
				std::mem::size_of_val(&attributes),
			)
		};
		if result != 0 {
			return Err(std::io::Error::last_os_error());
		}
	}
	let flags = libc::MOVE_MOUNT_F_EMPTY_PATH | libc::MOVE_MOUNT_T_EMPTY_PATH;
	// SAFETY: The empty paths select the valid detached mount and target descriptors.
	let result = unsafe {
		libc::syscall(
			libc::SYS_move_mount,
			mount.as_raw_fd(),
			c"".as_ptr(),
			target,
			c"".as_ptr(),
			flags,
		)
	};
	if result != 0 {
		return Err(std::io::Error::last_os_error());
	}
	Ok(())
}

fn mount_attributes(attributes: MountAttributes) -> u64 {
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
	attribute_set
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

fn mount_cgroup(target: &Path, readonly: bool) -> tg::Result<()> {
	std::fs::create_dir_all(target).map_err(|error| {
		tg::error!(
			!error,
			path = %target.display(),
			"failed to create the cgroup mountpoint"
		)
	})?;
	let target = cstring(target);
	// Mount a tmpfs first. An enclosing sandbox mounts cgroup2 at this path too, and the kernel refuses to mount a filesystem on its own mountpoint. That mount cannot be detached instead, because a mount inherited into an unprivileged mount namespace is locked.
	let underlay_source = cstring("tmpfs");
	let underlay_fstype = cstring("tmpfs");
	mount_raw(
		Some(&underlay_source),
		&target,
		Some(&underlay_fstype),
		libc::MS_NODEV | libc::MS_NOEXEC | libc::MS_NOSUID,
		std::ptr::null_mut(),
	)
	.map_err(|error| tg::error!(!error, "failed to create the cgroup underlay mount"))?;
	let source = cstring("cgroup2");
	let fstype = cstring("cgroup2");
	let mut flags = libc::MS_NODEV | libc::MS_NOEXEC | libc::MS_NOSUID;
	if readonly {
		flags |= libc::MS_RDONLY;
	}
	mount_raw(
		Some(&source),
		&target,
		Some(&fstype),
		flags,
		std::ptr::null_mut(),
	)
	.map_err(|error| tg::error!(!error, "failed to create the cgroup mount"))?;
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
		mount_bind_path(&entry, &target)?;
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
	let metadata = std::fs::metadata(source)?;
	if metadata.is_dir() {
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

fn path_depth(path: &Path) -> usize {
	path.components().count()
}

fn cstring(value: impl AsRef<OsStr>) -> CString {
	CString::new(value.as_ref().as_bytes()).unwrap()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn open_absolute_path_rejects_symbolic_links() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let directory = temp.path().join("directory");
		std::fs::create_dir(&directory).unwrap();
		let link = temp.path().join("link");
		std::os::unix::fs::symlink(&directory, &link).unwrap();

		let error = open_absolute_path(&link).unwrap_err();

		assert_eq!(error.raw_os_error(), Some(libc::ELOOP));
	}

	#[test]
	fn create_mount_target_rejects_symbolic_link_escape() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let root = temp.path().join("root");
		let outside = temp.path().join("outside");
		std::fs::create_dir(&root).unwrap();
		std::fs::create_dir(&outside).unwrap();
		std::os::unix::fs::symlink(&outside, root.join("escape")).unwrap();
		let root = open_absolute_path(&root).unwrap();

		let error =
			create_mount_target(root.as_raw_fd(), Path::new("escape/file"), false).unwrap_err();

		assert_eq!(error.raw_os_error(), Some(libc::ELOOP));
		assert!(!outside.join("file").exists());
	}

	#[test]
	fn create_mount_target_creates_expected_types() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let root = open_absolute_path(temp.path()).unwrap();

		let directory =
			create_mount_target(root.as_raw_fd(), Path::new("a/directory"), true).unwrap();
		let file = create_mount_target(root.as_raw_fd(), Path::new("a/file"), false).unwrap();

		assert!(fd_is_directory(directory.as_raw_fd()).unwrap());
		assert!(!fd_is_directory(file.as_raw_fd()).unwrap());
	}
}
