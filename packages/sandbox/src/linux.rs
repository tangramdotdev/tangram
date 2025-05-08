use crate::{
	Child, Command, ExitStatus,
	common::{CStringVec, cstring, envstring, socket_pair},
};
use num::ToPrimitive;
use std::{ffi::CString, os::fd::RawFd};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

mod guest;
mod init;
mod root;

pub(crate) struct Mount {
	pub source: CString,
	pub target: CString,
	pub fstype: Option<CString>,
	pub flags: libc::c_ulong,
	pub data: Option<Vec<u8>>,
	pub readonly: bool,
}

pub(crate) struct Context {
	pub argv: CStringVec,
	pub cwd: CString,
	pub envp: CStringVec,
	pub executable: CString,
	pub hostname: Option<CString>,
	pub root: Option<CString>,
	pub mounts: Vec<Mount>,
	pub network: bool,
	pub socket: std::os::unix::net::UnixStream,
	pub stdin: RawFd,
	pub stdout: RawFd,
	pub stderr: RawFd,
}

pub async fn spawn(command: &mut Command) -> std::io::Result<Child> {
	if !command.mounts.is_empty() && command.chroot.is_none() {
		return Err(std::io::Error::other(
			"cannot create mounts without a chroot directory",
		));
	}

	// Create argv, cwd, and envp strings.
	let argv = std::iter::once(cstring(&command.executable))
		.chain(command.args.iter().map(cstring))
		.collect::<CStringVec>();
	let cwd = cstring(&command.cwd);
	let envp = command
		.envs
		.iter()
		.map(|(k, v)| envstring(k, v))
		.collect::<CStringVec>();
	let executable = cstring(&command.executable);
	let hostname = command.hostname.as_ref().map(cstring);

	// Create the mounts.
	let mut mounts = Vec::with_capacity(command.mounts.len());
	for mount in &command.mounts {
		// Remap the target path.
		let target = if let Some(chroot) = &command.chroot {
			chroot.join(mount.target.strip_prefix("/").unwrap())
		} else {
			mount.target.clone()
		};

		// Create the mount.
		let mount = Mount {
			source: cstring(&mount.source),
			target: cstring(&target),
			fstype: mount.fstype.as_ref().map(cstring),
			flags: mount.flags,
			data: mount.data.clone(),
			readonly: mount.readonly,
		};
		mounts.push(mount);
	}

	let root = command.chroot.as_ref().map(cstring);

	// Create the socket for guest control. This will be used to send the guest process its PID w.r.t the parent's PID namespace and to indicate to the child when it may exec.
	let (mut parent_socket, child_socket) = socket_pair()?;

	// Create stdio.
	let (parent_stdin, child_stdin) = command.stdin.take().unwrap().split_stdin()?;
	let (parent_stdout, child_stdout) = command.stdout.take().unwrap().split_stdout()?;
	let (parent_stderr, child_stderr) = command.stderr.take().unwrap().split_stderr()?;

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
		stdin: child_stdin,
		stdout: child_stdout,
		stderr: child_stderr,
	};

	// Fork.
	let mut clone_args: libc::clone_args = libc::clone_args {
		flags: libc::CLONE_NEWUSER.try_into().unwrap(),
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
	let root_pid = root_pid.to_i32().unwrap();

	// Check if clone3 failed.
	if root_pid < 0 {
		return Err(std::io::Error::last_os_error());
	}

	// Run the root process.
	if root_pid == 0 {
		root::main(context);
	}

	// Signal the root/guest process to start and get the guest PID.
	let uid = command.uid.unwrap_or_else(|| unsafe { libc::getuid() });
	let gid = command.gid.unwrap_or_else(|| unsafe { libc::getgid() });

	let guest_pid = match try_start(command.chroot.is_some(), gid, uid, &mut parent_socket).await {
		Ok(pid) => pid,
		Err(error) => unsafe {
			libc::kill(root_pid, libc::SIGKILL);
			return Err(error);
		},
	};

	// Close unused fds.
	for fd in [child_stdin, child_stdout, child_stderr] {
		unsafe {
			libc::close(fd);
		}
	}

	// Create the child.
	let child = Child {
		guest_pid,
		root_pid,
		socket: parent_socket,
		stdin: parent_stdin,
		stdout: parent_stdout,
		stderr: parent_stderr,
	};

	Ok(child)
}

async fn try_start(
	chroot: bool,
	child_gid: libc::gid_t,
	child_uid: libc::gid_t,
	socket: &mut tokio::net::UnixStream,
) -> std::io::Result<libc::pid_t> {
	// Read the pid of the guest process.
	let pid = socket.read_i32_le().await?;

	// If the guest process is running in a chroot jail, it's current state is blocked waiting for the host process (the caller) to update its uid and gid maps. We need to wait for the root process to notify the host of the guest's PID after it is cloned.
	if chroot {
		// Write the guest process's UID map.
		let uid = unsafe { libc::getuid() };
		tokio::fs::write(
			format!("/proc/{pid}/uid_map"),
			format!("{child_uid} {uid} 1\n"),
		)
		.await?;

		// Deny setgroups to the process.
		tokio::fs::write(format!("/proc/{pid}/setgroups"), "deny").await?;

		// Write the guest process's GID map.
		let gid = unsafe { libc::getgid() };
		tokio::fs::write(
			format!("/proc/{pid}/gid_map"),
			format!("{child_gid} {gid} 1\n"),
		)
		.await?;
	}

	// Notify the guest that it may continue.
	socket.write_u8(1).await?;

	// Return the child pid.
	Ok(pid)
}

pub(crate) async fn wait(child: &mut Child) -> std::io::Result<ExitStatus> {
	// If this future is dropped, then kill the root process.
	let root_process = child.root_pid;
	let guest_process = child.guest_pid;
	scopeguard::defer! {
		unsafe {
			libc::kill(root_process, libc::SIGKILL);
			libc::kill(guest_process, libc::SIGKILL);
		}
	}

	// Wait for the root process to exit.
	tokio::task::spawn_blocking(move || {
		let mut status: libc::c_int = 0;
		let ret =
			unsafe { libc::waitpid(root_process, std::ptr::addr_of_mut!(status), libc::__WALL) };
		if ret == -1 {
			return Err(std::io::Error::last_os_error());
		}
		if libc::WIFEXITED(status) {
			let code = libc::WEXITSTATUS(status);
			if code != 0 {
				return Err(std::io::Error::other(
					"the root process exited with a nonzero exit code",
				));
			}
		}

		if libc::WIFSIGNALED(status) {
			return Err(std::io::Error::other(
				"the root process exited with a signal",
			));
		}

		Ok(())
	})
	.await
	.unwrap()?;

	// Get the status.
	let status = child.socket.read_i32_le().await?;
	if libc::WIFEXITED(status) {
		let code = libc::WEXITSTATUS(status);
		let code = code.to_u8().unwrap();
		Ok(ExitStatus::Code(code))
	} else if libc::WIFSIGNALED(status) {
		let signal = libc::WTERMSIG(status);
		let signal = signal.to_u8().unwrap();
		Ok(ExitStatus::Signal(signal))
	} else {
		Err(std::io::Error::other(
			"process exited with unknown code or signal",
		))
	}
}
