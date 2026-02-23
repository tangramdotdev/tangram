use {
	crate::{
		Command, Options, abort_errno,
		common::{CStringVec, cstring, envstring},
		linux::{get_existing_mount_flags, get_user, guest::mount_and_chroot},
	},
	std::path::PathBuf,
};

pub fn enter(options: &Options) -> std::io::Result<()> {
	let (uid, gid) = get_user(options.user.as_ref()).expect("failed to get the uid/gid");
	unsafe {
		// Update the uid map.
		let proc_uid = libc::getuid();
		let proc_gid = libc::getgid();

		let result = libc::unshare(libc::CLONE_NEWUSER);
		if result < 0 {
			return Err(std::io::Error::last_os_error());
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
			return Err(std::io::Error::last_os_error());
		}

		// If sandboxing in a network, enter a new network namespace.
		if !options.network {
			let result = libc::unshare(libc::CLONE_NEWNET);
			if result < 0 {
				return Err(std::io::Error::last_os_error());
			}
		}

		// If a new hostname is requested, enter a new UTS namespace.
		if let Some(hostname) = &options.hostname {
			let result = libc::unshare(libc::CLONE_NEWUTS);
			if result < 0 {
				return Err(std::io::Error::last_os_error());
			}
			let result = libc::sethostname(hostname.as_ptr().cast(), hostname.len());
			if result < 0 {
				return Err(std::io::Error::last_os_error());
			}
		}
	}
	// Sort the mounts by target path component count so that parent mounts (like overlays at /) are mounted before child mounts (like bind mounts at /dev).
	let mut mounts = options.mounts.clone();
	mounts.sort_by_key(|mount| {
		mount
			.target
			.as_ref()
			.map_or(0, |path| path.components().count())
	});
	for m in &mounts {
		eprintln!("mount: {m:?}");
		mount(m, options.chroot.as_ref())?;
	}
	if let Some(root) = &options.chroot {
		eprintln!("--- mount tree for {:?} ---", root);
		print_tree(root.parent().unwrap(), 0, 2);
		eprintln!("--- end mount tree ---");
	}
	Ok(())
}

pub fn spawn(mut command: Command) -> std::io::Result<i32> {
	if !command.mounts.is_empty() && command.chroot.is_none() {
		return Err(std::io::Error::other(
			"cannot create mounts without a chroot directory",
		));
	}

	// Sort the mounts.
	command.mounts.sort_unstable_by_key(|mount| {
		mount
			.target
			.as_ref()
			.map_or(0, |path| path.components().count())
	});

	// Create argv, cwd, and envp strings.
	let argv = std::iter::once(cstring(&command.executable))
		.chain(command.trailing.iter().map(cstring))
		.collect::<CStringVec>();
	let cwd = command
		.cwd
		.clone()
		.map(cstring);
	let envp = command
		.env
		.iter()
		.map(|(key, value)| envstring(key, value))
		.collect::<CStringVec>();
	let executable = cstring(&command.executable);

	// Create the mounts.
	let mut mounts = Vec::with_capacity(command.mounts.len());
	for mount in &command.mounts {
		// Remap the target path.
		let target = mount.target.as_ref().map(|target| {
			if let Some(chroot) = &command.chroot {
				chroot.join(target.strip_prefix("/").unwrap())
			} else {
				target.clone()
			}
		});
		let source = mount.source.as_ref().map(cstring);
		let flags = if let Some(source) = &source
			&& mount.fstype.is_none()
		{
			let existing = get_existing_mount_flags(source)
				.inspect_err(|error| eprintln!("failed to get mount flags: {error}"))?;
			existing | mount.flags
		} else {
			mount.flags
		};
		// Create the mount.
		let mount = crate::linux::Mount {
			source,
			target: target.map(cstring),
			fstype: mount.fstype.as_ref().map(cstring),
			flags,
			data: mount.data.clone(),
		};
		mounts.push(mount);
	}

	// Get the chroot path.
	let root = command.chroot.as_ref().map(cstring);

	let mut flags = 0u64;
	if !mounts.is_empty() {
		flags |= libc::CLONE_NEWNS as u64;
	};
	let mut clone_args: libc::clone_args = libc::clone_args {
		flags,
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
			if let Some(root) = &root {
				mount_and_chroot(&mut mounts, &root);
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
			abort_errno!("execvpe failed");
		}
	}

	Ok(pid as _)
}

fn print_tree(path: &std::path::Path, depth: usize, max_depth: usize) {
	if depth >= max_depth {
		return;
	}
	let indent = "  ".repeat(depth);
	let name = path.file_name().map_or_else(
		|| path.display().to_string(),
		|n| n.to_string_lossy().to_string(),
	);
	let meta = std::fs::symlink_metadata(path);
	let suffix = match &meta {
		Ok(m) if m.is_symlink() => {
			let target = std::fs::read_link(path).unwrap_or_default();
			format!(" -> {}", target.display())
		},
		Ok(m) if m.is_dir() => "/".to_string(),
		_ => String::new(),
	};
	eprintln!("{indent}{name}{suffix}");
	if let Ok(m) = &meta {
		if m.is_dir() && !m.is_symlink() {
			if let Ok(entries) = std::fs::read_dir(path) {
				let mut entries: Vec<_> = entries.filter_map(Result::ok).collect();
				entries.sort_by_key(|e| e.file_name());
				for entry in entries {
					print_tree(&entry.path(), depth + 1, max_depth);
				}
			}
		}
	}
}

fn mount(mount: &crate::Mount, chroot: Option<&PathBuf>) -> std::io::Result<()> {
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
	let data = mount
		.data
		.as_ref()
		.map(|bytes| bytes.as_ptr().cast())
		.unwrap_or(std::ptr::null_mut());
	unsafe {
		// Create the mount point.
		if let (Some(source), Some(target)) = (&source, &mut target) {
			crate::linux::guest::create_mountpoint_if_not_exists(source, target);
		}

		let result = libc::mount(
			source
				.as_ref()
				.map(|c| c.as_ptr())
				.unwrap_or(std::ptr::null()),
			target
				.as_ref()
				.map(|c| c.as_ptr())
				.unwrap_or(std::ptr::null()),
			fstype
				.as_ref()
				.map(|c| c.as_ptr())
				.unwrap_or(std::ptr::null()),
			flags,
			data,
		);
		if result < 0 {
			eprintln!("failed to mount {source:?}:{target:?}");
			return Err(std::io::Error::last_os_error());
		}
	}
	Ok(())
}
