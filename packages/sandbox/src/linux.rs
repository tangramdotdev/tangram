use {
	crate::{
		Config, abort_errno,
		common::{SpawnContext, start_session, which},
		server::Server,
	},
	indoc::indoc,
	num::ToPrimitive,
	std::{
		collections::HashMap,
		ffi::{CString, OsStr},
		iter,
		os::{fd::AsRawFd, unix::ffi::OsStrExt},
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

struct CStringVec {
	_strings: Vec<CString>,
	pointers: Vec<*const libc::c_char>,
}

#[derive(Debug)]
struct Mount {
	source: Option<PathBuf>,
	target: Option<PathBuf>,
	fstype: Option<CString>,
	flags: u64,
	data: Option<Vec<u8>>,
}

pub fn enter(config: &Config) -> tg::Result<()> {
	std::fs::create_dir_all(&config.root_path)
		.map_err(|source| tg::error!(!source, "failed to create the root path"))?;
	let mut overlays = HashMap::new();
	let mut mounts = Vec::new();
	for mount in &config.mounts {
		match mount {
			tg::Either::Left(mount) => {
				if !overlays.contains_key(&mount.target) {
					let lowerdirs = Vec::new();
					let upperdir = config
						.scratch_path
						.join("upper")
						.join(overlays.len().to_string());
					let workdir = config
						.scratch_path
						.join("work")
						.join(overlays.len().to_string());
					std::fs::create_dir_all(&upperdir).ok();
					std::fs::create_dir_all(&workdir).ok();
					overlays.insert(mount.target.clone(), (lowerdirs, upperdir, workdir));
				}
				let path = config.artifacts_path.join(mount.source.to_string());
				let (lower_dirs, _, _) = overlays.get_mut(&mount.target).unwrap();
				lower_dirs.push(path);
			},
			tg::Either::Right(mount) => {
				let mount = bind(&mount.source, &mount.target, mount.readonly);
				mounts.push(mount);
			},
		}
	}
	// Create the .tangram directory.
	let path = config.scratch_path.join(".tangram");
	std::fs::create_dir_all(&path).map_err(
		|source| tg::error!(!source, path = %path.display(), "failed to create the data directory"),
	)?;

	// Create /etc.
	std::fs::create_dir_all(config.scratch_path.join("lower/etc")).ok();

	// Create /tmp.
	std::fs::create_dir_all(config.scratch_path.join("lower/tmp")).ok();

	// Create nsswitch.conf.
	std::fs::write(
		config.scratch_path.join("lower/etc/nsswitch.conf"),
		indoc!(
			"
				passwd: files compat
				shadow: files compat
				hosts: files dns compat
			"
		),
	)
	.map_err(|source| tg::error!(!source, "failed to create /etc/nsswitch.conf"))?;

	// Create /etc/passwd.
	std::fs::write(
		config.scratch_path.join("lower/etc/passwd"),
		indoc!(
			"
				root:!:0:0:root:/nonexistent:/bin/false
				nobody:!:65534:65534:nobody:/nonexistent:/bin/false
			"
		),
	)
	.map_err(|source| tg::error!(!source, "failed to create /etc/passwd"))?;

	// Copy resolv.conf.
	if config.network {
		std::fs::copy(
			"/etc/resolv.conf",
			config.scratch_path.join("lower/etc/resolv.conf"),
		)
		.map_err(|source| tg::error!(!source, "failed to copy /etc/resolv.conf to the sandbox"))?;
	}

	// Get or create the root overlay.
	if !overlays.contains_key(Path::new("/")) {
		let lowerdirs = Vec::new();
		let upperdir = config
			.scratch_path
			.join("upper")
			.join(overlays.len().to_string());
		let workdir = config
			.scratch_path
			.join("work")
			.join(overlays.len().to_string());
		std::fs::create_dir_all(&upperdir).ok();
		std::fs::create_dir_all(&workdir).ok();
		overlays.insert("/".into(), (lowerdirs, upperdir, workdir));
	}
	let (lowerdirs, _, _) = overlays.get_mut(Path::new("/")).unwrap();
	lowerdirs.push(config.scratch_path.join("lower"));

	// Mount /dev, /proc, /tmp, /.tangram/artifacts, /output
	mounts.push(bind("/dev", "/dev", false));
	mounts.push(bind("/proc", "/proc", false));
	mounts.push(bind(
		config.scratch_path.join(".tangram"),
		"/.tangram",
		false,
	));
	mounts.push(bind(&config.artifacts_path, "/.tangram/artifacts", true));
	mounts.push(bind(&config.output_path, "/output", false));

	// Add the overlay mounts.
	for (merged, (lowerdirs, upperdir, workdir)) in &overlays {
		mounts.push(overlay(lowerdirs, upperdir, workdir, merged));
	}

	// Sort mounts.
	mounts.sort_unstable_by_key(|mount| mount.target.clone());

	// Get uid/gid
	let (uid, gid) = get_user(config.user.as_ref()).expect("failed to get the uid/gid");

	unsafe {
		// Update the uid map.
		let proc_uid = libc::getuid();
		let proc_gid = libc::getgid();
		let result = libc::unshare(libc::CLONE_NEWUSER);
		if result < 0 {
			let source = std::io::Error::last_os_error();
			return Err(tg::error!(
				!source,
				"failed to unshare into new user namespace"
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
			let source = std::io::Error::last_os_error();
			return Err(tg::error!(
				!source,
				"failed to unshare into new PID namespace"
			));
		}

		// If sandboxing in a network, enter a new network namespace.
		if !config.network {
			let result = libc::unshare(libc::CLONE_NEWNET);
			if result < 0 {
				let source = std::io::Error::last_os_error();
				return Err(tg::error!(
					!source,
					"failed to unshare into new network namespace"
				));
			}
		}

		// If a new hostname is requested, enter a new UTS namespace.
		if let Some(hostname) = &config.hostname {
			let result = libc::unshare(libc::CLONE_NEWUTS);
			if result < 0 {
				let source = std::io::Error::last_os_error();
				return Err(tg::error!(
					!source,
					"failed to unshare into new UTS namespace"
				));
			}
			let result = libc::sethostname(hostname.as_ptr().cast(), hostname.len());
			if result < 0 {
				let source = std::io::Error::last_os_error();
				return Err(tg::error!(!source, "failed to set hostname"));
			}
		}
	}

	// Perform the mounts.
	for m in &mounts {
		mount(m, &config.root_path)?;
	}

	// chroot
	unsafe {
		let name = cstring(config.root_path.as_os_str());
		if libc::chroot(name.as_ptr()) != 0 {
			let error = std::io::Error::last_os_error();
			return Err(tg::error!(!error, root = %config.root_path.display(), "chroot failed"));
		}
	}
	Ok(())
}

pub fn spawn(context: SpawnContext) -> tg::Result<libc::pid_t> {
	let SpawnContext {
		id,
		command,
		stdin,
		stdout,
		stderr,
		pty,
	} = context;

	// Create argv, cwd, and envp strings.
	let argv = std::iter::once(cstring(&command.executable))
		.chain(command.args.iter().map(cstring))
		.collect::<CStringVec>();
	let cwd = cstring(&command.cwd);
	let envp = command
		.env
		.iter()
		.map(|(key, value)| envstring(key, value))
		.chain(iter::once(envstring("TANGRAM_PROCESS", id.to_string())))
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
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "clone3 failed"));
	}

	// Run the process.
	if pid == 0 {
		if let Some(pty) = pty {
			start_session(&pty);
		}
		unsafe {
			libc::dup2(stdin.as_raw_fd(), libc::STDIN_FILENO);
			libc::dup2(stdout.as_raw_fd(), libc::STDOUT_FILENO);
			libc::dup2(stderr.as_raw_fd(), libc::STDERR_FILENO);
			let ret = libc::chdir(cwd.as_ptr());
			if ret == -1 {
				abort_errno!("failed to set the working directory {:?}", command.cwd);
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

fn bind(source: impl AsRef<Path>, target: impl AsRef<Path>, readonly: bool) -> Mount {
	let ro = if readonly { libc::MS_RDONLY } else { 0 };
	Mount {
		source: Some(source.as_ref().to_owned()),
		target: Some(target.as_ref().to_owned()),
		flags: libc::MS_BIND | libc::MS_REC | ro,
		fstype: None,
		data: None,
	}
}

fn overlay(lowerdirs: &[PathBuf], upperdir: &Path, workdir: &Path, merged: &Path) -> Mount {
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
	let source = Some("overlay".into());
	let target = Some(merged.to_owned());
	let fstype = Some(c"overlay".to_owned());

	let mut data = Vec::new();
	data.extend_from_slice(b"userxattr,lowerdir=");
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
		source,
		target,
		fstype,
		flags: 0,
		data: Some(data),
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

fn mount(mount: &Mount, root: &Path) -> tg::Result<()> {
	// Remap the target path.
	let target = mount
		.target
		.as_ref()
		.map(|target| root.join(target.strip_prefix("/").unwrap()));

	// Create the mountpoint if it does not exist.
	if let (Some(source), Some(target)) = (&mount.source, &target) {
		create_mountpoint_if_not_exists(source, target).map_err(|source| {
			tg::error!(!source, target = %target.display(),
				"failed to create mountpoint",
			)
		})?;
	}

	// Convert
	let source = mount.source.as_ref().map(cstring);
	let target = target.as_ref().map(cstring);
	let fstype = mount.fstype.as_ref();
	let flags = if let Some(source) = &source
		&& mount.fstype.is_none()
	{
		let existing = get_existing_mount_flags(source)
			.map_err(|source| tg::error!(!source, "failed to get existing mount flags"))?;
		existing | mount.flags
	} else {
		mount.flags
	};
	let data = mount.data.as_ref().map_or(std::ptr::null_mut(), |bytes| {
		bytes.as_ptr().cast::<std::ffi::c_void>().cast_mut()
	});
	unsafe {
		let source = source.as_ref().map_or(std::ptr::null(), |c| c.as_ptr());
		let target = target.as_ref().map_or(std::ptr::null(), |c| c.as_ptr());
		let fstype = fstype.as_ref().map_or(std::ptr::null(), |c| c.as_ptr());
		let result = libc::mount(source, target, fstype, flags, data);
		if result < 0 {
			let error = std::io::Error::last_os_error();
			return Err(tg::error!(!error, ?mount, "failed to mount"));
		}
		if (flags & libc::MS_BIND != 0) && (flags & libc::MS_RDONLY != 0) {
			let flags = flags | libc::MS_REMOUNT;
			let result = libc::mount(source, target, fstype, flags, data);
			if result < 0 {
				let error = std::io::Error::last_os_error();
				return Err(tg::error!(!error, ?mount, "failed to remount as read only"));
			}
		}
	}
	Ok(())
}

fn create_mountpoint_if_not_exists(
	source: impl AsRef<Path>,
	target: impl AsRef<Path>,
) -> std::io::Result<()> {
	let source = source.as_ref();
	let is_dir = if source.as_os_str().as_bytes() == b"overlay" {
		true
	} else {
		source.is_dir()
	};
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
		std::fs::File::create_new(target)?;
	}
	Ok(())
}

impl Server {
	pub(crate) async fn reaper_task(&self) -> tg::Result<()> {
		let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::child())
			.map_err(|source| tg::error!(!source, "failed to listen for SIGCHLD"))?;
		loop {
			signal.recv().await;
			self.reap_children();
		}
	}

	fn reap_children(&self) {
		unsafe {
			loop {
				// Wait for any child processes without blocking.
				let mut status = 0;
				let pid = libc::waitpid(-1, std::ptr::addr_of_mut!(status), libc::WNOHANG);
				let status = if libc::WIFEXITED(status) {
					libc::WEXITSTATUS(status)
				} else if libc::WIFSIGNALED(status) {
					128 + libc::WTERMSIG(status)
				} else {
					1
				};
				if pid == 0 {
					break;
				}
				if pid < 0 {
					let error = std::io::Error::last_os_error();
					tracing::error!(?error, "error waiting for children");
					break;
				}
				let status = status.max(255).to_u8().unwrap();
				if let Some(mut process) = self
					.pids
					.get(&pid)
					.and_then(|id| self.processes.get_mut(&*id))
				{
					process.status.replace(status);
					process.notify.notify_waiters();
				}
			}
		}
	}
}

impl CStringVec {
	fn as_ptr(&self) -> *const *const libc::c_char {
		self.pointers.as_ptr()
	}
}

fn cstring(s: impl AsRef<OsStr>) -> CString {
	CString::new(s.as_ref().as_bytes()).unwrap()
}

fn envstring(k: impl AsRef<OsStr>, v: impl AsRef<OsStr>) -> CString {
	let string = format!(
		"{}={}",
		k.as_ref().to_string_lossy(),
		v.as_ref().to_string_lossy()
	);
	CString::new(string).unwrap()
}

impl FromIterator<CString> for CStringVec {
	fn from_iter<T: IntoIterator<Item = CString>>(iter: T) -> Self {
		let mut strings = Vec::new();
		let mut pointers = Vec::new();
		for cstr in iter {
			pointers.push(cstr.as_ptr());
			strings.push(cstr);
		}
		pointers.push(std::ptr::null());
		Self {
			_strings: strings,
			pointers,
		}
	}
}

unsafe impl Send for CStringVec {}
