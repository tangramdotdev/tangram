use {
	crate::{
		ManagerArg, RunArg, abort, abort_errno,
		common::{SpawnContext, start_session, which},
		server::Server,
	},
	bytes::Bytes,
	indoc::indoc,
	num::ToPrimitive,
	std::{
		collections::HashMap,
		ffi::{CString, OsStr},
		io::Write as _,
		os::{
			fd::{AsRawFd, FromRawFd as _, OwnedFd, RawFd},
			unix::{ffi::OsStrExt, net::UnixListener as StdUnixListener},
		},
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

struct Cgroup {
	path: PathBuf,
}

#[repr(C)]
struct CapUserHeader {
	version: u32,
	pid: i32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct CapUserData {
	effective: u32,
	permitted: u32,
	inheritable: u32,
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

pub fn run(arg: &RunArg, ready_fd: Option<RawFd>) -> tg::Result<()> {
	let directory = crate::Directory::new(arg.path.clone());
	let listen_path = directory.host_listen_path();
	let listener = StdUnixListener::bind(&listen_path)
		.map_err(|source| tg::error!(!source, path = %listen_path.display(), "failed to bind"))?;
	let cgroup = create_cgroup(&directory)?;
	let child = enter(arg, cgroup.as_ref())?;
	if let Some(child) = child {
		drop(listener);
		if let Some(fd) = ready_fd {
			unsafe {
				libc::close(fd);
			}
		}
		let status = wait_for_child(child);
		if let Some(cgroup) = cgroup {
			remove_cgroup(&cgroup).ok();
		}
		return status;
	}

	listener
		.set_nonblocking(true)
		.map_err(|source| tg::error!(!source, "failed to set nonblocking mode"))?;
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|source| tg::error!(!source, "failed to create the runtime"))?;

	runtime.block_on(async move {
		let listener = tokio::net::UnixListener::from_std(listener)
			.map_err(|source| tg::error!(!source, "failed to create the unix listener"))?;
		if let Some(fd) = ready_fd {
			let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
			file.write_all(&[0x00, 0x00, 0x00])
				.map_err(|source| tg::error!(!source, "failed to write the ready signal"))?;
		}
		let server = Server::new(crate::server::ServerArg {
			library_paths: arg.library_paths.clone(),
			tangram_path: arg.tangram_path.clone(),
		});
		server
			.serve(tokio_util::either::Either::Left(listener))
			.await;
		Ok::<_, tg::Error>(())
	})?;

	Ok(())
}

fn enter(arg: &RunArg, cgroup: Option<&Cgroup>) -> tg::Result<Option<libc::pid_t>> {
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
	std::fs::create_dir_all(root.join("dev")).ok();
	std::fs::create_dir_all(root.join("proc")).ok();

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

	// Add the root overlay.
	mounts.entry("/".into()).or_default().push((root, false));

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
	mounts.push(filesystem(
		"tmpfs",
		"/dev",
		"tmpfs",
		libc::MS_NOSUID | libc::MS_STRICTATIME,
		Some("mode=0755,size=64k"),
	));
	mounts.push(filesystem(
		"proc",
		"/proc",
		"proc",
		libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
		None,
	));
	mounts.push(filesystem(
		"devpts",
		"/dev/pts",
		"devpts",
		libc::MS_NOSUID | libc::MS_NOEXEC,
		Some("newinstance,ptmxmode=0666,mode=0620"),
	));
	for source in [
		"/dev/null",
		"/dev/zero",
		"/dev/full",
		"/dev/random",
		"/dev/urandom",
		"/dev/tty",
	] {
		mounts.push(bind(source, source, false));
	}

	// Sort mounts.
	mounts.sort_unstable_by_key(|mount| mount.target.clone());

	// Get uid/gid.
	let (uid, gid) = get_user(arg.user.as_ref())
		.map_err(|source| tg::error!(!source, "failed to get the uid/gid"))?;

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
			.map_err(|source| tg::error!(!source, "failed to write the uid map"))?;

		// Deny setgroups.
		std::fs::write("/proc/self/setgroups", "deny")
			.map_err(|source| tg::error!(!source, "failed to deny setgroups"))?;

		// Update the gid map.
		std::fs::write("/proc/self/gid_map", format!("{gid} {proc_gid} 1\n"))
			.map_err(|source| tg::error!(!source, "failed to write the gid map"))?;

		// Enter a new mount and PID namespace.
		let result = libc::unshare(libc::CLONE_NEWPID | libc::CLONE_NEWNS | libc::CLONE_NEWIPC);
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

	let child = unsafe { libc::fork() };
	if child < 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to fork into the PID namespace"));
	}
	if child > 0 {
		return Ok(Some(child));
	}

	if let Some(cgroup) = cgroup {
		move_self_to_cgroup(cgroup)?;
	}
	set_parent_death_signal(libc::SIGKILL)?;
	make_mounts_private()?;

	for spec in &mounts {
		mount(spec, &directory.host_root_path())?;
	}
	configure_dev(&directory.host_root_path())?;
	configure_guest_root(&directory.host_root_path())?;
	pivot_root_into(&directory.host_root_path())?;
	set_securebits()?;
	setresgid(gid)?;
	setresuid(uid)?;
	set_no_new_privs()?;
	drop_capabilities()?;
	Ok(None)
}

pub fn spawn(context: SpawnContext) -> tg::Result<OwnedFd> {
	let SpawnContext {
		command,
		stdin,
		stdout,
		stderr,
		pty,
	} = context;
	if !command.cwd.is_absolute() {
		return Err(tg::error!(
			cwd = %command.cwd.display(),
			"the working directory must be an absolute path"
		));
	}

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
	let mut pidfd = -1;
	let mut clone_args: libc::clone_args = libc::clone_args {
		flags: libc::CLONE_PIDFD as u64,
		stack: 0,
		stack_size: 0,
		pidfd: std::ptr::addr_of_mut!(pidfd).cast::<libc::c_int>() as u64,
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
			if let Err(error) = close_non_std_fds() {
				abort!("failed to close extra file descriptors: {}", error.trace());
			}
			if let Err(error) = apply_process_seccomp() {
				abort!("failed to apply seccomp: {}", error.trace());
			}
			libc::execvpe(
				executable.as_ptr(),
				argv.as_ptr().cast(),
				envp.as_ptr().cast(),
			);
			abort_errno!("execvpe failed {}", command.executable.display());
		}
	}

	if pidfd < 0 {
		return Err(tg::error!("clone3 did not return a pidfd"));
	}
	let pidfd = unsafe { OwnedFd::from_raw_fd(pidfd) };

	Ok(pidfd)
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

fn filesystem(
	source: impl AsRef<Path>,
	target: impl AsRef<Path>,
	fstype: &str,
	flags: u64,
	data: Option<&str>,
) -> Mount {
	Mount {
		source: Some(source.as_ref().to_owned()),
		target: Some(target.as_ref().to_owned()),
		fstype: Some(cstring(fstype)),
		flags,
		data: data.map(|data| {
			let mut bytes = data.as_bytes().to_vec();
			bytes.push(0);
			bytes.into()
		}),
	}
}

fn wait_for_child(child: libc::pid_t) -> tg::Result<()> {
	unsafe {
		let mut status = 0;
		let result = libc::waitpid(child, std::ptr::addr_of_mut!(status), 0);
		if result < 0 {
			let source = std::io::Error::last_os_error();
			return Err(tg::error!(
				!source,
				"failed to wait for the sandbox process"
			));
		}
		if libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0 {
			return Ok(());
		}
		if libc::WIFSIGNALED(status) {
			let signal = libc::WTERMSIG(status);
			return Err(tg::error!(
				signal = %signal,
				"the sandbox process exited with a signal"
			));
		}
		let status = libc::WEXITSTATUS(status);
		Err(tg::error!(
			status = %status,
			"the sandbox process exited with a non-zero status"
		))
	}
}

pub(crate) fn send_signal(pidfd: &OwnedFd, signal: libc::c_int) -> tg::Result<()> {
	let result = unsafe {
		libc::syscall(
			libc::SYS_pidfd_send_signal,
			pidfd.as_raw_fd(),
			signal,
			std::ptr::null::<libc::siginfo_t>(),
			0,
		)
	};
	if result < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "pidfd_send_signal failed"));
	}
	Ok(())
}

pub(crate) fn wait_for_process(pidfd: &OwnedFd) -> tg::Result<u8> {
	unsafe {
		let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
		loop {
			let result = libc::waitid(
				libc::P_PIDFD,
				pidfd.as_raw_fd().try_into().unwrap(),
				info.as_mut_ptr(),
				libc::WEXITED,
			);
			if result == 0 {
				break;
			}
			let error = std::io::Error::last_os_error();
			if error.raw_os_error() == Some(libc::EINTR) {
				continue;
			}
			return Err(tg::error!(!error, "failed to wait for the process"));
		}
		let info = info.assume_init();
		let status = match info.si_code {
			libc::CLD_EXITED => info.si_status(),
			libc::CLD_KILLED | libc::CLD_DUMPED => 128 + info.si_status(),
			_ => 1,
		};
		Ok(status.min(255).to_u8().unwrap())
	}
}

fn create_cgroup(directory: &crate::Directory) -> tg::Result<Option<Cgroup>> {
	let root = Path::new("/sys/fs/cgroup");
	if !root.join("cgroup.controllers").exists() {
		return Ok(None);
	}
	let current = std::fs::read_to_string("/proc/self/cgroup")
		.ok()
		.and_then(|contents| {
			contents.lines().find_map(|line| {
				let (_, path) = line.split_once("::")?;
				Some(path.trim().to_owned())
			})
		})
		.unwrap_or_else(|| "/".to_owned());
	let current = root.join(current.trim_start_matches('/'));
	let name = directory
		.host_path()
		.file_name()
		.and_then(|name| name.to_str())
		.unwrap_or("sandbox");
	let name = sanitize_cgroup_name(name);
	let path = current.join(format!("tg-sandbox-{name}"));
	match std::fs::create_dir(&path) {
		Ok(()) => (),
		Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => (),
		Err(source) if matches!(source.kind(), std::io::ErrorKind::PermissionDenied) => {
			tracing::debug!(path = %path.display(), ?source, "skipping cgroup setup");
			return Ok(None);
		},
		Err(source) if matches!(source.raw_os_error(), Some(libc::EROFS)) => {
			tracing::debug!(path = %path.display(), ?source, "skipping cgroup setup");
			return Ok(None);
		},
		Err(source) => {
			return Err(tg::error!(
				!source,
				path = %path.display(),
				"failed to create the sandbox cgroup"
			));
		},
	}
	let oom_group = path.join("memory.oom.group");
	if oom_group.exists() {
		write_cgroup_file(&oom_group, b"1\n").ok();
	}
	Ok(Some(Cgroup { path }))
}

fn remove_cgroup(cgroup: &Cgroup) -> std::io::Result<()> {
	std::fs::remove_dir(&cgroup.path)
}

fn move_self_to_cgroup(cgroup: &Cgroup) -> tg::Result<()> {
	let path = cgroup.path.join("cgroup.procs");
	write_cgroup_file(&path, b"0\n").map_err(|source| {
		tg::error!(
			!source,
			path = %path.display(),
			"failed to move the sandbox process into the cgroup"
		)
	})
}

fn write_cgroup_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
	let mut file = std::fs::OpenOptions::new().write(true).open(path)?;
	file.write_all(bytes)
}

fn sanitize_cgroup_name(name: &str) -> String {
	let mut output = String::new();
	for char in name.chars() {
		if char.is_ascii_alphanumeric() || matches!(char, '-' | '_') {
			output.push(char);
		} else {
			output.push('-');
		}
	}
	if output.is_empty() {
		output.push_str("sandbox");
	}
	output
}

fn set_parent_death_signal(signal: libc::c_int) -> tg::Result<()> {
	let result = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, signal, 0, 0, 0) };
	if result < 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to set the parent death signal"));
	}
	Ok(())
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

fn configure_dev(root: &Path) -> tg::Result<()> {
	let dev = root.join("dev");
	for name in ["fd", "stdin", "stdout", "stderr", "ptmx"] {
		let path = dev.join(name);
		if path.exists() {
			std::fs::remove_file(&path).ok();
		}
	}
	std::os::unix::fs::symlink("../proc/self/fd", dev.join("fd"))
		.map_err(|source| tg::error!(!source, "failed to create /dev/fd"))?;
	std::os::unix::fs::symlink("../proc/self/fd/0", dev.join("stdin"))
		.map_err(|source| tg::error!(!source, "failed to create /dev/stdin"))?;
	std::os::unix::fs::symlink("../proc/self/fd/1", dev.join("stdout"))
		.map_err(|source| tg::error!(!source, "failed to create /dev/stdout"))?;
	std::os::unix::fs::symlink("../proc/self/fd/2", dev.join("stderr"))
		.map_err(|source| tg::error!(!source, "failed to create /dev/stderr"))?;
	std::os::unix::fs::symlink("pts/ptmx", dev.join("ptmx"))
		.map_err(|source| tg::error!(!source, "failed to create /dev/ptmx"))?;
	Ok(())
}

fn configure_guest_root(root: &Path) -> tg::Result<()> {
	let tmp = root.join("tmp");
	let permissions =
		<std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o1777);
	std::fs::set_permissions(&tmp, permissions).map_err(
		|source| tg::error!(!source, path = %tmp.display(), "failed to set /tmp permissions"),
	)
}

fn pivot_root_into(root: &Path) -> tg::Result<()> {
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
		return Err(tg::error!(
			!source,
			root = %root.display(),
			"pivot_root failed"
		));
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

fn change_directory(path: &Path) -> tg::Result<()> {
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

fn setresuid(uid: libc::uid_t) -> tg::Result<()> {
	let ret = unsafe { libc::setresuid(uid, uid, uid) };
	if ret != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, uid = %uid, "failed to set the uid"));
	}
	Ok(())
}

fn setresgid(gid: libc::gid_t) -> tg::Result<()> {
	let ret = unsafe { libc::setresgid(gid, gid, gid) };
	if ret != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, gid = %gid, "failed to set the gid"));
	}
	Ok(())
}

fn set_no_new_privs() -> tg::Result<()> {
	let ret = unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) };
	if ret != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to set no_new_privs"));
	}
	Ok(())
}

fn set_securebits() -> tg::Result<()> {
	const SECBIT_NOROOT: libc::c_int = 1 << 0;
	const SECBIT_NOROOT_LOCKED: libc::c_int = 1 << 1;
	const SECBIT_NO_SETUID_FIXUP: libc::c_int = 1 << 2;
	const SECBIT_NO_SETUID_FIXUP_LOCKED: libc::c_int = 1 << 3;
	const SECBIT_KEEP_CAPS_LOCKED: libc::c_int = 1 << 5;
	const SECBIT_NO_CAP_AMBIENT_RAISE: libc::c_int = 1 << 6;
	const SECBIT_NO_CAP_AMBIENT_RAISE_LOCKED: libc::c_int = 1 << 7;
	const PR_SET_SECUREBITS: libc::c_int = 28;

	let securebits = SECBIT_NOROOT
		| SECBIT_NOROOT_LOCKED
		| SECBIT_NO_SETUID_FIXUP
		| SECBIT_NO_SETUID_FIXUP_LOCKED
		| SECBIT_KEEP_CAPS_LOCKED
		| SECBIT_NO_CAP_AMBIENT_RAISE
		| SECBIT_NO_CAP_AMBIENT_RAISE_LOCKED;
	let ret = unsafe { libc::prctl(PR_SET_SECUREBITS, securebits, 0, 0, 0) };
	if ret != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to set securebits"));
	}
	Ok(())
}

fn drop_capabilities() -> tg::Result<()> {
	const CAPABILITY_VERSION_3: u32 = 0x2008_0522;
	const PR_CAP_AMBIENT: libc::c_int = 47;
	const PR_CAP_AMBIENT_CLEAR_ALL: libc::c_int = 4;
	const PR_CAPBSET_DROP: libc::c_int = 24;
	let ret = unsafe { libc::prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0) };
	if ret != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to clear ambient capabilities"));
	}
	let max = std::fs::read_to_string("/proc/sys/kernel/cap_last_cap")
		.ok()
		.and_then(|contents| contents.trim().parse::<u32>().ok())
		.unwrap_or(63);
	for capability in 0..=max {
		let ret = unsafe { libc::prctl(PR_CAPBSET_DROP, capability, 0, 0, 0) };
		if ret != 0 {
			let source = std::io::Error::last_os_error();
			return Err(tg::error!(
				!source,
				capability = %capability,
				"failed to drop a capability from the bounding set"
			));
		}
	}
	let mut header = CapUserHeader {
		version: CAPABILITY_VERSION_3,
		pid: 0,
	};
	let mut data = [CapUserData {
		effective: 0,
		permitted: 0,
		inheritable: 0,
	}; 2];
	let ret = unsafe {
		libc::syscall(
			libc::SYS_capset,
			std::ptr::addr_of_mut!(header),
			data.as_mut_ptr(),
		)
	};
	if ret != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to drop capabilities"));
	}
	Ok(())
}

fn apply_process_seccomp() -> tg::Result<()> {
	const AUDIT_ARCH_AARCH64: u32 = 0xc000_00b7;
	const AUDIT_ARCH_X86_64: u32 = 0xc000_003e;
	const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;
	const SECCOMP_RET_ERRNO: u32 = 0x0005_0000;
	const SECCOMP_RET_KILL_PROCESS: u32 = 0x8000_0000;
	let audit_arch = if cfg!(target_arch = "aarch64") {
		AUDIT_ARCH_AARCH64
	} else {
		AUDIT_ARCH_X86_64
	};
	let mut filter = vec![
		sock_stmt(bpf_code(libc::BPF_LD | libc::BPF_W | libc::BPF_ABS), 4),
		sock_jump(
			bpf_code(libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K),
			audit_arch,
			1,
			0,
		),
		sock_stmt(
			bpf_code(libc::BPF_RET | libc::BPF_K),
			SECCOMP_RET_KILL_PROCESS,
		),
		sock_stmt(bpf_code(libc::BPF_LD | libc::BPF_W | libc::BPF_ABS), 0),
	];
	for syscall in blocked_syscalls() {
		filter.push(sock_jump(
			bpf_code(libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K),
			syscall,
			0,
			1,
		));
		filter.push(sock_stmt(
			bpf_code(libc::BPF_RET | libc::BPF_K),
			SECCOMP_RET_ERRNO | libc::EPERM as u32,
		));
	}
	filter.push(sock_stmt(
		bpf_code(libc::BPF_RET | libc::BPF_K),
		SECCOMP_RET_ALLOW,
	));

	let mut program = libc::sock_fprog {
		len: filter.len().to_u16().unwrap(),
		filter: filter.as_mut_ptr(),
	};
	let ret = unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) };
	if ret != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to set no_new_privs"));
	}
	let ret = unsafe {
		libc::prctl(
			libc::PR_SET_SECCOMP,
			libc::SECCOMP_MODE_FILTER,
			&mut program,
		)
	};
	if ret != 0 {
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to install the seccomp filter"));
	}
	Ok(())
}

fn close_non_std_fds() -> tg::Result<()> {
	const CLOSE_RANGE_UNSHARE: libc::c_uint = 1 << 1;
	let max = libc::c_uint::MAX;
	let result = unsafe { libc::syscall(libc::SYS_close_range, 3u32, max, CLOSE_RANGE_UNSHARE) };
	if result == 0 {
		return Ok(());
	}
	let error = std::io::Error::last_os_error();
	if !matches!(
		error.raw_os_error(),
		Some(libc::ENOSYS | libc::EINVAL | libc::EPERM)
	) {
		return Err(tg::error!(!error, "failed to close extra file descriptors"));
	}
	close_non_std_fds_fallback()
}

fn close_non_std_fds_fallback() -> tg::Result<()> {
	let entries = std::fs::read_dir("/proc/self/fd")
		.map_err(|source| tg::error!(!source, "failed to enumerate file descriptors"))?;
	let mut fds = Vec::new();
	for entry in entries {
		let entry =
			entry.map_err(|source| tg::error!(!source, "failed to read a directory entry"))?;
		let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
			continue;
		};
		let Ok(fd) = name.parse::<RawFd>() else {
			continue;
		};
		if fd <= libc::STDERR_FILENO {
			continue;
		}
		fds.push(fd);
	}
	for fd in fds {
		unsafe {
			libc::close(fd);
		}
	}
	Ok(())
}

fn blocked_syscalls() -> Vec<u32> {
	#[cfg(target_arch = "x86_64")]
	let mut syscalls = vec![
		libc::SYS_add_key.to_u32().unwrap(),
		libc::SYS_bpf.to_u32().unwrap(),
		libc::SYS_delete_module.to_u32().unwrap(),
		libc::SYS_finit_module.to_u32().unwrap(),
		libc::SYS_fsconfig.to_u32().unwrap(),
		libc::SYS_fsmount.to_u32().unwrap(),
		libc::SYS_fsopen.to_u32().unwrap(),
		libc::SYS_init_module.to_u32().unwrap(),
		libc::SYS_kexec_load.to_u32().unwrap(),
		libc::SYS_mount.to_u32().unwrap(),
		libc::SYS_mount_setattr.to_u32().unwrap(),
		libc::SYS_move_mount.to_u32().unwrap(),
		libc::SYS_name_to_handle_at.to_u32().unwrap(),
		libc::SYS_open_by_handle_at.to_u32().unwrap(),
		libc::SYS_open_tree.to_u32().unwrap(),
		libc::SYS_perf_event_open.to_u32().unwrap(),
		libc::SYS_ptrace.to_u32().unwrap(),
		libc::SYS_request_key.to_u32().unwrap(),
		libc::SYS_setns.to_u32().unwrap(),
		libc::SYS_swapoff.to_u32().unwrap(),
		libc::SYS_swapon.to_u32().unwrap(),
		libc::SYS_umount2.to_u32().unwrap(),
		libc::SYS_unshare.to_u32().unwrap(),
		libc::SYS_userfaultfd.to_u32().unwrap(),
	];
	#[cfg(not(target_arch = "x86_64"))]
	let syscalls = vec![
		libc::SYS_add_key.to_u32().unwrap(),
		libc::SYS_bpf.to_u32().unwrap(),
		libc::SYS_delete_module.to_u32().unwrap(),
		libc::SYS_finit_module.to_u32().unwrap(),
		libc::SYS_fsconfig.to_u32().unwrap(),
		libc::SYS_fsmount.to_u32().unwrap(),
		libc::SYS_fsopen.to_u32().unwrap(),
		libc::SYS_init_module.to_u32().unwrap(),
		libc::SYS_kexec_load.to_u32().unwrap(),
		libc::SYS_mount.to_u32().unwrap(),
		libc::SYS_mount_setattr.to_u32().unwrap(),
		libc::SYS_move_mount.to_u32().unwrap(),
		libc::SYS_name_to_handle_at.to_u32().unwrap(),
		libc::SYS_open_by_handle_at.to_u32().unwrap(),
		libc::SYS_open_tree.to_u32().unwrap(),
		libc::SYS_perf_event_open.to_u32().unwrap(),
		libc::SYS_ptrace.to_u32().unwrap(),
		libc::SYS_request_key.to_u32().unwrap(),
		libc::SYS_setns.to_u32().unwrap(),
		libc::SYS_swapoff.to_u32().unwrap(),
		libc::SYS_swapon.to_u32().unwrap(),
		libc::SYS_umount2.to_u32().unwrap(),
		libc::SYS_unshare.to_u32().unwrap(),
		libc::SYS_userfaultfd.to_u32().unwrap(),
	];
	#[cfg(target_arch = "x86_64")]
	{
		syscalls.push(libc::SYS_ioperm.to_u32().unwrap());
		syscalls.push(libc::SYS_iopl.to_u32().unwrap());
	}
	syscalls
}

fn sock_stmt(code: u16, k: u32) -> libc::sock_filter {
	libc::sock_filter {
		code,
		jt: 0,
		jf: 0,
		k,
	}
}

fn sock_jump(code: u16, k: u32, jt: u8, jf: u8) -> libc::sock_filter {
	libc::sock_filter { code, jt, jf, k }
}

fn bpf_code(code: u32) -> u16 {
	code.try_into().unwrap()
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
	if let Some(target) = &target {
		let result = if mount.fstype.is_some() {
			std::fs::create_dir_all(target)
		} else if let Some(source) = &mount.source {
			create_mountpoint_if_not_exists(source, target)
		} else {
			Ok(())
		};
		result.map_err(|source| {
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
