use {
	crate::{
		ManagerArg, RunArg, abort_errno,
		common::{SpawnContext, start_session, which},
		server::Server,
	},
	bytes::Bytes,
	indoc::indoc,
	num::ToPrimitive,
	std::{
		collections::HashMap,
		ffi::{CString, OsStr},
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
	data: Option<Bytes>,
}

pub(crate) fn prepare_runtime_libraries(arg: &ManagerArg) -> tg::Result<Vec<PathBuf>> {
	std::fs::remove_dir_all(&arg.rootfs_path).ok();
	std::fs::create_dir_all(&arg.rootfs_path)
		.map_err(|source| tg::error!(!source, "failed to create the sandbox directory"))?;
	let permissions = <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o755);
	crate::ROOTFS
		.extract(&arg.rootfs_path)
		.map_err(|source| tg::error!(!source, "failed to extract the sandbox rootfs"))?;
	crate::set_rootfs_permissions(&arg.rootfs_path, &crate::ROOTFS, &permissions)?;

	let lib_path = arg.rootfs_path.join("opt/tangram/lib");
	let output = std::process::Command::new("ldd")
		.arg(&arg.tangram_path)
		.output()
		.map_err(|source| {
			if source.kind() == std::io::ErrorKind::NotFound {
				tg::error!(
					"failed to prepare the sandbox rootfs: could not execute `ldd`; install `ldd` on this Linux host"
				)
			} else {
				tg::error!(
					!source,
					path = %arg.tangram_path.display(),
					"failed to execute `ldd`"
				)
			}
		})?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		let stdout = String::from_utf8_lossy(&output.stdout);
		return Err(tg::error!(
			status = %output.status,
			path = %arg.tangram_path.display(),
			stderr = %stderr.trim(),
			stdout = %stdout.trim(),
			"`ldd` failed"
		));
	}
	let stdout = String::from_utf8(output.stdout)
		.map_err(|source| tg::error!(!source, "failed to parse the `ldd` output"))?;
	for line in stdout.lines() {
		let line = line.trim();
		if line.is_empty() || line.starts_with("linux-vdso") {
			continue;
		}
		let parsed = if let Some((name, path)) = line.split_once("=>") {
			let name = name.trim();
			let path = path.trim();
			if path == "not found" {
				return Err(tg::error!(
					dependency = %name,
					executable = %arg.tangram_path.display(),
					"`ldd` reported a missing dependency"
				));
			}
			let path = path
				.split_whitespace()
				.next()
				.ok_or_else(|| tg::error!("failed to parse a path from the `ldd` output"))?;
			path.starts_with('/').then(|| PathBuf::from(path))
		} else if line.starts_with('/') {
			let path = line
				.split_whitespace()
				.next()
				.ok_or_else(|| tg::error!("failed to parse a path from the `ldd` output"))?;
			Some(PathBuf::from(path))
		} else {
			None
		};
		let Some(dependency_path) = parsed else {
			continue;
		};
		let source = std::fs::canonicalize(&dependency_path).map_err(|source| {
			tg::error!(
				!source,
				path = %dependency_path.display(),
				"failed to canonicalize the library path"
			)
		})?;
		let name = dependency_path
			.file_name()
			.and_then(|name| name.to_str())
			.ok_or_else(|| {
				tg::error!(
					path = %dependency_path.display(),
					"failed to get the library file name"
				)
			})?;
		let target = lib_path.join(name);
		if target.exists() {
			continue;
		}
		if std::fs::hard_link(&source, &target).is_err() {
			std::fs::copy(&source, &target).map_err(|error| {
				tg::error!(
					!error,
					source = %source.display(),
					target = %target.display(),
					"failed to stage the shared library"
				)
			})?;
		}
		std::fs::set_permissions(&target, permissions.clone()).map_err(|source| {
			tg::error!(
				!source,
				path = %target.display(),
				"failed to set sandbox file permissions"
			)
		})?;
	}

	Ok(Vec::new())
}

pub(crate) fn prepare_command_for_spawn(
	command: &mut crate::Command,
	_tangram_path: &Path,
	_library_paths: &[PathBuf],
) -> tg::Result<()> {
	crate::append_directories_to_path(
		command,
		&[
			Path::new("/opt/tangram/bin"),
			Path::new("/usr/bin"),
			Path::new("/bin"),
		],
	)
}

pub fn enter(arg: &RunArg) -> tg::Result<()> {
	let directory = crate::Directory::new(arg.path.clone());

	// Scan the mounts. We need to treat mounts to the same target as overlays, otherwise they are considered bind mounts.
	let iter = arg
		.mounts
		.iter()
		.map(|mount| (mount.source.clone(), mount.target.clone(), mount.readonly));
	let mut mounts = HashMap::new();
	for (source, target, readonly) in iter {
		if target == Path::new("/") && source == Path::new("/") {
			return Err(tg::error!(
				"mounting the host root directory to the guest root is not supported"
			));
		}
		mounts
			.entry(target)
			.or_insert(Vec::new())
			.push((source, readonly));
	}
	mounts
		.entry(PathBuf::from("/"))
		.or_default()
		.push((arg.rootfs_path.clone(), true));

	// Set up /opt/tangram, /etc, /tmp, /dev, and /proc.
	let root = directory.host_scratch_path().join("guest_root");
	std::fs::create_dir_all(root.join("opt/tangram")).ok();
	std::fs::create_dir_all(root.join("etc")).ok();
	std::fs::create_dir_all(root.join("tmp")).ok();

	// Setup /etc.
	std::fs::write(
		root.join("etc/nsswitch.conf"),
		indoc!(
			"
			passwd: files compat
			shadow: files compat
			hosts: files dns compat
		"
		),
	)
	.map_err(|source| tg::error!(!source, "failed to create /etc/nsswitch.conf"))?;
	std::fs::write(
		root.join("etc/passwd"),
		indoc!(
			"
			root:!:0:0:root:/nonexistent:/bin/false
			nobody:!:65534:65534:nobody:/nonexistent:/bin/false
		"
		),
	)
	.map_err(|source| tg::error!(!source, "failed to create /etc/passwd"))?;
	if arg.network {
		std::fs::copy("/etc/resolv.conf", root.join("etc/resolv.conf")).map_err(|source| {
			tg::error!(!source, "failed to copy /etc/resolv.conf to the sandbox")
		})?;
	}

	// Add the root overlay, /dev, and /proc.
	mounts.entry("/".into()).or_default().push((root, false));
	mounts
		.entry("/dev".into())
		.or_default()
		.push(("/dev".into(), false));
	mounts
		.entry("/proc".into())
		.or_default()
		.push(("/proc".into(), false));

	// Add /opt/tangram/artifacts, /opt/tangram/libexec/tangram, /opt/tangram/socket, and /opt/tangram/output.
	std::fs::create_dir_all(directory.host_output_path()).ok();
	std::fs::create_dir_all(directory.host_socket_path()).ok();
	mounts
		.entry(directory.guest_artifacts_path())
		.or_default()
		.push((arg.artifacts_path.clone(), true));
	mounts
		.entry(directory.guest_libexec_tangram_path())
		.or_default()
		.push((arg.tangram_path.clone(), true));
	mounts
		.entry(directory.guest_socket_path())
		.or_default()
		.push((directory.host_socket_path(), false));
	mounts
		.entry(directory.guest_output_path())
		.or_default()
		.push((directory.host_output_path(), false));

	// Convert the mounts.
	let mut num_overlays = 0;
	let mut mounts = mounts
		.into_iter()
		.map(|(target, sources)| {
			if sources.len() == 1 && target != Path::new("/") {
				let (source, readonly) = &sources[0];
				bind(source, target, *readonly)
			} else {
				let lowerdirs = sources
					.iter()
					.map(|(source, _)| source.clone())
					.collect::<Vec<_>>();
				let (upperdir, workdir) = if target == Path::new("/") {
					(
						directory.host_root_path(),
						directory.host_scratch_path().join("root_work"),
					)
				} else {
					let upperdir = directory
						.host_scratch_path()
						.join(format!("upper/{num_overlays}"));
					let workdir = directory
						.host_scratch_path()
						.join(format!("work/{num_overlays}"));
					num_overlays += 1;
					(upperdir, workdir)
				};
				std::fs::create_dir_all(&upperdir).ok();
				std::fs::create_dir_all(&workdir).ok();
				overlay(&lowerdirs, &upperdir, &workdir, &target)
			}
		})
		.collect::<Vec<_>>();

	// Sort mounts.
	mounts.sort_unstable_by_key(|mount| mount.target.clone());

	// Get uid/gid
	let (uid, gid) = get_user(arg.user.as_ref()).expect("failed to get the uid/gid");

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
		if !arg.network {
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
		if let Some(hostname) = &arg.hostname {
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
		mount(m, &directory.host_root_path())?;
	}

	// chroot
	unsafe {
		let root_path = directory.host_root_path();
		let name = cstring(root_path.as_os_str());
		if libc::chroot(name.as_ptr()) != 0 {
			let error = std::io::Error::last_os_error();
			return Err(tg::error!(!error, root = %root_path.display(), "chroot failed"));
		}
	}
	Ok(())
}

pub fn spawn(context: SpawnContext) -> tg::Result<libc::pid_t> {
	let SpawnContext {
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
		.collect::<CStringVec>();
	let executable = command
		.env
		.get("PATH")
		.and_then(|path| which(Path::new(path), &command.executable))
		.map_or_else(|| cstring(&command.executable), cstring);
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
			start_session(&pty, stdin.is_none(), stdout.is_none(), stderr.is_none());
		}
		unsafe {
			if let Some(fd) = &stdin {
				libc::dup2(fd.as_raw_fd(), libc::STDIN_FILENO);
			}
			if let Some(fd) = &stdout {
				libc::dup2(fd.as_raw_fd(), libc::STDOUT_FILENO);
			}
			if let Some(fd) = &stderr {
				libc::dup2(fd.as_raw_fd(), libc::STDERR_FILENO);
			}
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
	data.extend_from_slice(b"xino=off,userxattr,lowerdir=");
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
	data.push(0);

	Mount {
		source,
		target,
		fstype,
		flags: 0,
		data: Some(data.into()),
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
	const ST_RELATIME: u64 = 0x400; // This flag is missing on musl.
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
		let existing = get_existing_mount_flags(source).unwrap_or(0);
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
				let status = status.min(255).to_u8().unwrap();
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
