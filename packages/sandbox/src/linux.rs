use {
	crate::{
		Command,
		common::{CStringVec, cstring, envstring},
	},
	bytes::Bytes,
	num::ToPrimitive as _,
	std::{
		ffi::{CString, OsStr},
		io::Write,
	},
};

mod guest;
mod root;

#[derive(Debug)]
struct Mount {
	source: Option<CString>,
	target: Option<CString>,
	fstype: Option<CString>,
	flags: libc::c_ulong,
	data: Option<Bytes>,
}

struct Context {
	argv: CStringVec,
	cwd: CString,
	envp: CStringVec,
	executable: CString,
	hostname: Option<CString>,
	root: Option<CString>,
	mounts: Vec<Mount>,
	network: bool,
	socket: std::os::unix::net::UnixStream,
}

pub fn spawn(mut command: Command) -> std::io::Result<std::process::ExitCode> {
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
		.map_or_else(std::env::current_dir, Ok::<_, std::io::Error>)
		.inspect_err(|_| eprintln!("failed to get cwd"))
		.map(cstring)?;
	let envp = command
		.env
		.iter()
		.map(|(k, v)| envstring(k, v))
		.collect::<CStringVec>();
	let executable = cstring(&command.executable);
	let hostname = command.hostname.as_ref().map(cstring);

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

		// Create the mount.
		let mount = Mount {
			source: mount.source.as_ref().map(cstring),
			target: target.map(cstring),
			fstype: mount.fstype.as_ref().map(cstring),
			flags: mount.flags,
			data: mount.data.clone(),
		};
		mounts.push(mount);
	}

	// Get the chroot path.
	let root = command.chroot.as_ref().map(cstring);

	// Create the socket for guest control. This will be used to send the guest process its PID w.r.t the parent's PID namespace and to indicate to the child when it may exec.
	let (mut parent_socket, child_socket) = std::os::unix::net::UnixStream::pair()
		.inspect_err(|_| eprintln!("failed to create socket"))?;

	// Create the context.
	let context = Context {
		argv,
		cwd,
		envp,
		executable,
		hostname,
		root,
		mounts,
		network: command.network,
		socket: child_socket,
	};

	// Fork.
	let mut clone_args: libc::clone_args = libc::clone_args {
		flags: (libc::CLONE_NEWUSER | libc::CLONE_NEWPID)
			.try_into()
			.unwrap(),
		stack: 0,
		stack_size: 0,
		pidfd: 0,
		child_tid: 0,
		parent_tid: 0,
		exit_signal: 0,
		tls: 0,
		set_tid: 0,
		set_tid_size: 0,
		cgroup: 0,
	};
	let root_pid = unsafe {
		libc::syscall(
			libc::SYS_clone3,
			std::ptr::addr_of_mut!(clone_args),
			std::mem::size_of::<libc::clone_args>(),
		)
	};
	let pid = root_pid.to_i32().unwrap();

	// Check if clone3 failed.
	if pid < 0 {
		eprintln!("clone3 failed");
		return Err(std::io::Error::last_os_error());
	}

	// Run the root process.
	if pid == 0 {
		root::main(context);
	}

	// Signal the root/guest process to start and get the guest PID.
	let (uid, gid) = get_user(command.user.as_ref())?;

	// Start the process and get the pid of the guest process.
	match try_start(command.chroot.is_some(), pid, gid, uid, &mut parent_socket) {
		Ok(pid) => pid,
		Err(error) => unsafe {
			libc::kill(pid, libc::SIGKILL);
			return Err(error);
		},
	}

	// Wait for the root process to exit.
	let mut status: libc::c_int = 0;
	let ret = unsafe { libc::waitpid(pid, std::ptr::addr_of_mut!(status), libc::__WALL) };
	if ret == -1 {
		eprintln!("wait failed");
		return Err(std::io::Error::last_os_error());
	}

	let status = if libc::WIFEXITED(status) {
		libc::WEXITSTATUS(status).to_u8().unwrap().into()
	} else if libc::WIFSIGNALED(status) {
		(128 + libc::WTERMSIG(status).to_u8().unwrap()).into()
	} else {
		return Err(std::io::Error::other("unknown process termination"));
	};

	Ok(status)
}

fn try_start(
	chroot: bool,
	pid: libc::pid_t,
	child_gid: libc::gid_t,
	child_uid: libc::gid_t,
	socket: &mut std::os::unix::net::UnixStream,
) -> std::io::Result<()> {
	// If the guest process is running in a chroot jail, it's current state is blocked waiting for the host process (the caller) to update its uid and gid maps. We need to wait for the root process to notify the host of the guest's PID after it is cloned.
	if chroot {
		// Write the guest process's UID map.
		let uid = unsafe { libc::getuid() };
		std::fs::write(
			format!("/proc/{pid}/uid_map"),
			format!("{child_uid} {uid} 1\n"),
		)
		.inspect_err(|_| eprintln!("failed to write uid map"))?;

		// Deny setgroups to the process.
		std::fs::write(format!("/proc/{pid}/setgroups"), "deny")
			.inspect_err(|_| eprintln!("failed to deny setgroups"))?;

		// Write the guest process's GID map.
		let gid = unsafe { libc::getgid() };
		std::fs::write(
			format!("/proc/{pid}/gid_map"),
			format!("{child_gid} {gid} 1\n"),
		)
		.inspect_err(|_| eprintln!("failed to write gid map"))?;
	}

	// Notify the guest that it may continue.
	socket
		.write_all(&[1u8])
		.inspect_err(|_| eprintln!("failed to signal process"))?;

	// Return the child pid.
	Ok(())
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
