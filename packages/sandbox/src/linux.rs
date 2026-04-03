use {
	crate::{
		Command, Config, EnterOutput, abort_errno,
		common::{CStringVec, cstring, envstring, which},
	},
	bytes::Bytes,
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::{
		collections::BTreeMap,
		ffi::{CString, OsStr, OsString},
		mem::MaybeUninit,
		os::unix::ffi::OsStrExt as _,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
struct Mount {
	source: Option<PathBuf>,
	target: Option<PathBuf>,
	fstype: Option<OsString>,
	flags: libc::c_ulong,
	data: Option<Bytes>,
}

#[derive(Debug)]
struct Overlay {
	lowerdirs: Vec<PathBuf>,
	upperdir: PathBuf,
	workdir: PathBuf,
}

pub fn enter(config: &Config) -> tg::Result<EnterOutput> {
	let root_mounted = config.mounts.iter().any(|mount| {
		mount
			.source
			.as_ref()
			.left()
			.is_some_and(|source| source == Path::new("/") && mount.target == Path::new("/"))
	});
	let user = Some(config.user.clone().unwrap_or_else(|| "root".to_owned()));
	let network = config.network;
	let hostname = Some(
		config
			.hostname
			.clone()
			.unwrap_or_else(|| config.id.to_string()),
	);
	let root_host = config.path.join("root");
	std::fs::create_dir_all(&root_host)
		.map_err(|source| tg::error!(!source, "failed to create the root directory"))?;
	let chroot = Some(root_host.clone());

	let artifacts = config.server_path.join("artifacts");
	let mut overlays = BTreeMap::new();
	let mut mounts = Vec::new();
	for mount in &config.mounts {
		match &mount.source {
			tg::Either::Left(source) => {
				mounts.push(create_bind_mount(source, &mount.target, mount.readonly));
			},
			tg::Either::Right(id) => {
				create_overlay(&mut overlays, &config.path, &mount.target).map_err(|source| {
					tg::error!(!source, "failed to create overlay directories")
				})?;
				let overlay = overlays.get_mut(&mount.target).unwrap();
				overlay.lowerdirs.push(artifacts.join(id.to_string()));
			},
		}
	}

	if !root_mounted {
		let data = config.path.join(".tangram");
		std::fs::create_dir_all(&data).map_err(
			|source| tg::error!(!source, path = %data.display(), "failed to create the data directory"),
		)?;
		std::fs::create_dir_all(config.path.join("lower/etc")).ok();
		std::fs::create_dir_all(config.path.join("lower/tmp")).ok();
		std::fs::create_dir_all(config.path.join("output")).ok();

		std::fs::write(
			config.path.join("lower/etc/nsswitch.conf"),
			formatdoc!(
				"
					passwd: files compat
					shadow: files compat
					hosts: files dns compat
				"
			),
		)
		.map_err(|source| tg::error!(!source, "failed to create /etc/nsswitch.conf"))?;
		std::fs::write(
			config.path.join("lower/etc/passwd"),
			formatdoc!(
				"
					root:!:0:0:root:/nonexistent:/bin/false
					nobody:!:65534:65534:nobody:/nonexistent:/bin/false
				"
			),
		)
		.map_err(|source| tg::error!(!source, "failed to create /etc/passwd"))?;

		if network {
			std::fs::copy(
				"/etc/resolv.conf",
				config.path.join("lower/etc/resolv.conf"),
			)
			.map_err(|source| {
				tg::error!(!source, "failed to copy /etc/resolv.conf to the sandbox")
			})?;
		}

		create_overlay(&mut overlays, &config.path, Path::new("/"))
			.map_err(|source| tg::error!(!source, "failed to create overlay directories"))?;
		let overlay = overlays.get_mut(Path::new("/")).unwrap();
		overlay.lowerdirs.push(config.path.join("lower"));

		mounts.push(create_bind_mount("/dev", "/dev", false));
		mounts.push(create_bind_mount("/proc", "/proc", false));
		mounts.push(create_bind_mount(
			config.path.join(".tangram"),
			"/.tangram",
			false,
		));
		mounts.push(create_bind_mount(artifacts, "/.tangram/artifacts", true));
		mounts.push(create_bind_mount(
			config.path.join("output"),
			"/output",
			false,
		));
	}

	for (target, overlay) in overlays {
		mounts.push(create_overlay_mount(
			&overlay.lowerdirs,
			&overlay.upperdir,
			&overlay.workdir,
			&target,
		));
	}

	let (uid, gid) = get_user(user.as_ref())
		.map_err(|source| tg::error!(!source, "failed to get the uid/gid"))?;
	unsafe {
		// Update the uid map.
		let proc_uid = libc::getuid();
		let proc_gid = libc::getgid();

		let result = libc::unshare(libc::CLONE_NEWUSER);
		if result < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to unshare user namespace"
			));
		}
		std::fs::write("/proc/self/uid_map", format!("{uid} {proc_uid} 1\n"))
			.expect("failed to write the uid map");

		// Deny setgroups.
		std::fs::write("/proc/self/setgroups", "deny").expect("failed to deny setgroups");

		// Update the gid map.
		std::fs::write("/proc/self/gid_map", format!("{gid} {proc_gid} 1\n"))
			.expect("failed to write the gid map");
		// Enter a new PID and mount namespace. The first child process will have pid 1. Mounts performed here will not be visible outside the sandbox.
		let result = libc::unshare(libc::CLONE_NEWPID | libc::CLONE_NEWNS);
		if result < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to unshare pid and mount namespaces"
			));
		}

		// If sandboxing in a network, enter a new network namespace.
		if !network {
			let result = libc::unshare(libc::CLONE_NEWNET);
			if result < 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to unshare network namespace"
				));
			}
		}

		// If a new hostname is requested, enter a new UTS namespace.
		if let Some(hostname) = &hostname {
			let result = libc::unshare(libc::CLONE_NEWUTS);
			if result < 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to unshare uts namespace"
				));
			}
			let result = libc::sethostname(hostname.as_ptr().cast(), hostname.len());
			if result < 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to set the hostname"
				));
			}
		}
	}

	// Sort the mounts by target path component count so that parent mounts (like overlays at /) are mounted before child mounts (like bind mounts at /dev).
	let mut mounts = mounts;
	mounts.sort_unstable_by_key(|mount| {
		mount
			.target
			.as_ref()
			.map_or(0, |path| path.components().count())
	});
	for mount_ in &mounts {
		mount(mount_, chroot.as_ref()).map_err(|source| tg::error!(!source, "failed to mount"))?;
	}

	// chroot.
	if let Some(chroot) = &chroot {
		unsafe {
			let name = cstring(chroot);
			if libc::chroot(name.as_ptr()) != 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to chroot"
				));
			}
		}
	}

	let output = EnterOutput {
		root_path: Some(root_host),
	};

	Ok(output)
}

fn create_overlay(
	overlays: &mut BTreeMap<PathBuf, Overlay>,
	path: &Path,
	target: &Path,
) -> std::io::Result<()> {
	if overlays.contains_key(target) {
		return Ok(());
	}
	let n = overlays.len();
	let upperdir = path.join("upper").join(n.to_string());
	let workdir = path.join("work").join(n.to_string());
	std::fs::create_dir_all(&upperdir)?;
	std::fs::create_dir_all(&workdir)?;
	overlays.insert(
		target.to_owned(),
		Overlay {
			lowerdirs: Vec::new(),
			upperdir,
			workdir,
		},
	);
	Ok(())
}

fn create_bind_mount(source: impl AsRef<Path>, target: impl AsRef<Path>, readonly: bool) -> Mount {
	let mut flags = libc::MS_BIND | libc::MS_REC;
	if readonly {
		flags |= libc::MS_RDONLY;
	}
	Mount {
		source: Some(source.as_ref().to_owned()),
		target: Some(target.as_ref().to_owned()),
		fstype: None,
		flags,
		data: None,
	}
}

fn create_overlay_mount(
	lowerdirs: &[PathBuf],
	upperdir: &Path,
	workdir: &Path,
	target: &Path,
) -> Mount {
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

	let mut data = b"userxattr,lowerdir=".to_vec();
	for (n, dir) in lowerdirs.iter().enumerate() {
		escape(&mut data, dir.as_os_str().as_bytes());
		if n != lowerdirs.len() - 1 {
			data.push(b':');
		}
	}
	data.extend_from_slice(b",upperdir=");
	data.extend_from_slice(upperdir.as_os_str().as_bytes());
	data.extend_from_slice(b",workdir=");
	data.extend_from_slice(workdir.as_os_str().as_bytes());

	Mount {
		source: Some("overlay".into()),
		target: Some(target.to_owned()),
		fstype: Some("overlay".into()),
		flags: 0,
		data: Some(Bytes::from(data)),
	}
}

pub fn spawn(command: &Command) -> std::io::Result<i32> {
	// Create argv, cwd, and envp strings.
	let argv = std::iter::once(cstring(&command.executable))
		.chain(command.args.iter().map(cstring))
		.collect::<CStringVec>();
	let cwd = command.cwd.clone().map(cstring);
	let envp = command
		.env
		.iter()
		.map(|(key, value)| envstring(key, value))
		.collect::<CStringVec>();

	let executable = command
		.env
		.iter()
		.find_map(|(key, value)| {
			(key == "PATH")
				.then_some(value)
				.and_then(|path| which(path.as_ref(), &command.executable).map(cstring))
		})
		.unwrap_or_else(|| cstring(&command.executable));

	let mut clone_args: libc::clone_args = libc::clone_args {
		flags: 0,
		stack: 0,
		stack_size: 0,
		pidfd: 0,
		child_tid: 0,
		parent_tid: 0,
		exit_signal: libc::SIGCHLD as u64,
		tls: 0,
		set_tid: 0,
		set_tid_size: 0,
		cgroup: 0,
	};

	let pid = unsafe {
		libc::syscall(
			libc::SYS_clone3,
			std::ptr::addr_of_mut!(clone_args),
			std::mem::size_of::<libc::clone_args>(),
		)
	};

	// Check if clone3 failed.
	if pid < 0 {
		eprintln!("clone3 failed");
		return Err(std::io::Error::last_os_error());
	}

	// Run the process.
	if pid == 0 {
		unsafe {
			if let Some(fd) = command.stdin {
				libc::dup2(fd, libc::STDIN_FILENO);
			}
			if let Some(fd) = command.stdout {
				libc::dup2(fd, libc::STDOUT_FILENO);
			}
			if let Some(fd) = command.stderr {
				libc::dup2(fd, libc::STDERR_FILENO);
			}
			if let Some(cwd) = &cwd {
				let ret = libc::chdir(cwd.as_ptr());
				if ret == -1 {
					abort_errno!("failed to set the working directory {:?}", command.cwd);
				}
			}
			libc::execvpe(
				executable.as_ptr(),
				argv.as_ptr().cast(),
				envp.as_ptr().cast(),
			);
			abort_errno!("execvpe failed {}", command.executable.display());
		}
	}

	Ok(pid.to_i32().unwrap())
}

fn mount(mount: &Mount, chroot: Option<&PathBuf>) -> std::io::Result<()> {
	// Remap the target path.
	let target = mount.target.as_ref().map(|target| {
		if let Some(chroot) = &chroot {
			chroot.join(target.strip_prefix("/").unwrap())
		} else {
			target.clone()
		}
	});
	let source = mount.source.as_ref().map(cstring);
	let flags = if let Some(source) = &source
		&& mount.fstype.is_none()
	{
		let existing = get_existing_mount_flags(source)?;
		existing | mount.flags
	} else {
		mount.flags
	};
	let mut target = target.map(cstring);
	let fstype = mount.fstype.as_ref().map(cstring);
	let data = mount.data.as_ref().map_or(std::ptr::null_mut(), |bytes| {
		bytes.as_ptr().cast::<std::ffi::c_void>().cast_mut()
	});
	unsafe {
		if let (Some(source), Some(target)) = (&source, &mut target) {
			create_mountpoint_if_not_exists(source, target);
		}
		let source = source.as_ref().map_or(std::ptr::null(), |c| c.as_ptr());
		let target = target.as_ref().map_or(std::ptr::null(), |c| c.as_ptr());
		let fstype = fstype.as_ref().map_or(std::ptr::null(), |c| c.as_ptr());
		let result = libc::mount(source, target, fstype, flags, data);
		if result < 0 {
			eprintln!("failed to mount {source:?}:{target:?}");
			return Err(std::io::Error::last_os_error());
		}
		if (flags & libc::MS_BIND != 0) && (flags & libc::MS_RDONLY != 0) {
			let flags = flags | libc::MS_REMOUNT;
			let result = libc::mount(source, target, fstype, flags, data);
			if result < 0 {
				eprintln!("failed to remount {target:?} as read-only");
				return Err(std::io::Error::last_os_error());
			}
		}
	}
	Ok(())
}

fn create_mountpoint_if_not_exists(source: &CString, target: &mut CString) {
	unsafe {
		#[cfg_attr(all(target_arch = "x86_64"), expect(clippy::cast_possible_wrap))]
		const BACKSLASH: libc::c_char = b'\\' as _;
		#[cfg_attr(all(target_arch = "x86_64"), expect(clippy::cast_possible_wrap))]
		const SLASH: libc::c_char = b'/' as _;
		const NULL: libc::c_char = 0;

		// Determine if the target is a directory or not.
		let is_dir = 'a: {
			if source.as_bytes() == b"overlay" {
				break 'a true;
			}
			let mut stat = MaybeUninit::<libc::stat>::zeroed();
			if libc::stat(source.as_ptr(), stat.as_mut_ptr().cast()) < 0 {
				abort_errno!("failed to stat source");
			}
			let stat = stat.assume_init();
			if !(stat.st_mode & libc::S_IFDIR != 0 || stat.st_mode & libc::S_IFREG != 0) {
				abort_errno!("mount source is not a directory or regular file");
			}
			stat.st_mode & libc::S_IFDIR != 0
		};

		let ptr = target.as_ptr().cast_mut();
		let len = target.as_bytes_with_nul().len();
		let mut esc = false;
		for n in 1..len {
			match (*ptr.add(n), esc) {
				(SLASH, false) => {
					*ptr.add(n) = 0;
					libc::mkdir(target.as_ptr(), 0o755);
					*ptr.add(n) = SLASH;
				},
				(BACKSLASH, false) => {
					esc = true;
				},
				(NULL, _) => {
					break;
				},
				_ => {
					esc = false;
				},
			}
		}
		if is_dir {
			libc::mkdir(target.as_ptr(), 0o755);
		} else {
			libc::creat(target.as_ptr(), 0o777);
		}
	}
}

fn get_user(name: Option<impl AsRef<OsStr>>) -> std::io::Result<(libc::uid_t, libc::gid_t)> {
	let Some(name) = name else {
		unsafe {
			let uid = libc::getuid();
			let gid = libc::getgid();
			return Ok((uid, gid));
		}
	};
	unsafe {
		let passwd = libc::getpwnam(cstring(name.as_ref()).as_ptr());
		if passwd.is_null() {
			return Err(std::io::Error::other("getpwname failed"));
		}
		let uid = (*passwd).pw_uid;
		let gid = (*passwd).pw_gid;
		Ok((uid, gid))
	}
}

fn get_existing_mount_flags(path: &CString) -> std::io::Result<libc::c_ulong> {
	const FLAGS: [(u64, u64); 7] = [
		(libc::MS_RDONLY, libc::ST_RDONLY),
		(libc::MS_NODEV, libc::ST_NODEV),
		(libc::MS_NOEXEC, libc::ST_NOEXEC),
		(libc::MS_NOSUID, libc::ST_NOSUID),
		(libc::MS_NOATIME, libc::ST_NOATIME),
		(libc::MS_RELATIME, libc::ST_RELATIME),
		(libc::MS_NODIRATIME, libc::ST_NODIRATIME),
	];
	let statfs = unsafe {
		let mut statfs = std::mem::MaybeUninit::zeroed();
		let ret = libc::statfs64(path.as_ptr(), statfs.as_mut_ptr());
		if ret != 0 {
			eprintln!("failed to statfs {}", path.to_string_lossy());
			return Err(std::io::Error::last_os_error());
		}
		statfs.assume_init()
	};
	let mut flags = 0;
	for (mount_flag, stat_flag) in FLAGS {
		if (statfs.f_flags.abs().to_u64().unwrap() & stat_flag) != 0 {
			flags |= mount_flag;
		}
	}
	Ok(flags)
}
