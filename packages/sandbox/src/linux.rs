use crate::{
	Child, Command, ExitStatus, Stderr, Stdin, Stdout,
	common::{
		CStringVec, GuestIo, abort, abort_errno, cstring, envstring, redirect_stdio, socket_pair,
		stdio_pair,
	},
	pty::Pty,
};
use num::ToPrimitive;
use std::{ffi::CString, os::fd::AsRawFd};
use tangram_either::Either;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Mount {
	pub source: CString,
	pub target: CString,
	pub fstype: Option<CString>,
	pub flags: libc::c_ulong,
	pub data: Option<Vec<u8>>,
	pub readonly: bool,
}

struct Context {
	argv: CStringVec,
	cwd: CString,
	envp: CStringVec,
	executable: CString,
	root: Option<CString>,
	mounts: Vec<Mount>,
	network: bool,
	socket: std::os::unix::net::UnixStream,
	stdin: GuestIo,
	stdout: GuestIo,
	stderr: GuestIo,
}

pub async fn spawn(command: &Command) -> std::io::Result<Child> {
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
	let mounts = command
		.mounts
		.iter()
		.map(|mount| Mount {
			source: cstring(&mount.source),
			target: cstring(&mount.target),
			fstype: mount.fstype.as_ref().map(cstring),
			flags: mount.flags,
			data: mount.data.clone(),
			readonly: mount.readonly,
		})
		.collect();
	let root = command.chroot.as_ref().map(cstring);

	// Create the socket for guest control. This will be used to send the guest process its PID w.r.t the parent's PID namespace and to indicate to the child when it may exec.
	let (parent_socket, child_socket) = socket_pair()?;

	// Create stdio.
	let mut pty = None;
	let (parent_stdin, child_stdin) = stdio_pair(command.stdin, &mut pty).await?;
	let (parent_stdout, child_stdout) = stdio_pair(command.stdout, &mut pty).await?;
	let (parent_stderr, child_stderr) = stdio_pair(command.stderr, &mut pty).await?;

	// Create the context.
	let context = Context {
		argv,
		cwd,
		envp,
		executable,
		root,
		mounts,
		network: command.network,
		socket: child_socket,
		stdin: child_stdin,
		stdout: child_stdout,
		stderr: child_stderr,
	};

	// Fork.
	let flags = libc::CLONE_NEWUSER.try_into().unwrap();
	let mut clone_args = libc::clone_args {
		flags,
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
	let pid = unsafe {
		libc::syscall(
			libc::SYS_clone3,
			std::ptr::addr_of_mut!(clone_args),
			std::mem::size_of::<libc::clone_args>(),
		)
	};
	let pid = pid.to_i32().unwrap();

	// Check if clone3 failed.
	if pid < 0 {
		return Err(std::io::Error::last_os_error());
	}

	// Run the root process.
	if pid == 0 {
		self::root_process(context);
	}

	// Close unused fds.
	for io in [context.stdin, context.stdout, context.stderr] {
		match io {
			Either::Left(mut pty) => {
				pty.close_tty();
			},
			Either::Right(Some(raw)) => {
				unsafe { libc::close(raw) };
			},
			Either::Right(None) => (),
		}
	}

	// Split stdio.
	let pty = pty.map(Pty::into_writer);
	let stdout = match parent_stdout {
		Either::Left(_) => Some(Either::Left(pty.as_ref().unwrap().get_reader()?)),
		Either::Right(Some(io)) => Some(Either::Right(io)),
		Either::Right(None) => None,
	};
	let stderr = match parent_stderr {
		Either::Left(_) => Some(Either::Left(pty.as_ref().unwrap().get_reader()?)),
		Either::Right(Some(io)) => Some(Either::Right(io)),
		Either::Right(None) => None,
	};
	let stdin = match parent_stdin {
		Either::Left(_) => Some(Either::Left(pty.unwrap())),
		Either::Right(Some(io)) => Some(Either::Right(io)),
		Either::Right(None) => None,
	};

	// Create the child.
	let child = Child {
		chroot: command.chroot.is_some(),
		gid: command.gid,
		pid,
		socket: parent_socket,
		uid: command.uid,
		stdin: stdin.map(|inner| Stdin { inner }),
		stdout: stdout.map(|inner| Stdout { inner }),
		stderr: stderr.map(|inner| Stderr { inner }),
	};

	Ok(child)
}

pub(crate) async fn wait(child: &mut Child) -> std::io::Result<ExitStatus> {
	// If this future is dropped, then kill the root process.
	let root_process = child.pid;
	let child_gid = child.gid;
	let child_uid = child.uid;

	// Defer closing the process.
	scopeguard::defer! {
		// Kill the root process.
		let ret = unsafe { libc::kill(root_process, libc::SIGKILL) };
		if ret != 0 {
			return;
		}

		// Wait for the root process to exit.
		tokio::task::spawn_blocking(move || {
			let mut status = 0;
			unsafe {
				libc::waitpid(
					root_process,
					std::ptr::addr_of_mut!(status),
					libc::__WALL,
				)
			}
		});
	};

	// If the guest process is running in a chroot jail, it's current state is blocked waiting for the host process (the caller) to update its uid and gid maps. We need to wait for the root process to notify the host of the guest's PID after it is cloned.
	if child.chroot {
		let pid = child.socket.read_i32_le().await?;

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
	child.socket.write_u8(1).await?;

	// Read the exit status from the host socket.
	let kind = child.socket.read_u8().await?;
	let value = child.socket.read_i32_le().await?;
	let exit = match kind {
		0 => ExitStatus::Code(value),
		1 => ExitStatus::Signal(value),
		_ => unreachable!(),
	};

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
					"root process exited with nonzero exit code",
				));
			}
		} else if libc::WIFSIGNALED(status) {
			let signal = libc::WTERMSIG(status);
			if signal != 0 {
				return Err(std::io::Error::other("root process exited with signal"));
			}
		} else {
			unreachable!();
		};
		Ok(())
	})
	.await
	.unwrap()?;

	// Return the exit code.
	Ok(exit)
}

fn root_process(context: Context) -> ! {
	unsafe {
		// Ask to receive a SIGKILL signal if the host process exits.
		let ret = libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0);
		if ret == -1 {
			abort_errno!("failed to set PDEATHSIG");
		}

		// Set the clone flags.
		let mut flags = 0;
		if context.root.is_some() {
			flags |= libc::CLONE_NEWNS;
			flags |= libc::CLONE_NEWPID;
		}
		if !context.network {
			flags |= libc::CLONE_NEWNET;
		}

		// Fork into the guest process.
		let mut clone_args = libc::clone_args {
			flags: flags.try_into().unwrap(),
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
		let pid = libc::syscall(
			libc::SYS_clone3,
			std::ptr::addr_of_mut!(clone_args),
			std::mem::size_of::<libc::clone_args>(),
		);
		let pid = pid.to_i32().unwrap();
		if pid == -1 {
			libc::close(context.socket.as_raw_fd());
			abort_errno!("failed to spawn the guest process");
		} else if pid == 0 {
			guest_process(context);
		} else {
			// Send the guest process's PID to the host process, so the host process can write the UID and GID maps.
			if context.root.is_some() {
				let ret = libc::send(
					context.socket.as_raw_fd(),
					std::ptr::addr_of!(pid).cast(),
					std::mem::size_of_val(&pid),
					0,
				);
				if ret == -1 {
					abort_errno!("failed to send the PID of guest process");
				}
			}

			// Run the guest process.
			let mut status: libc::c_int = 0;
			let ret = libc::waitpid(pid, &mut status, libc::__WALL);
			if ret == -1 {
				abort_errno!("failed to wait for the guest process");
			}

			// Get the exit code or signal.
			let (kind, value) = if libc::WIFEXITED(status) {
				let code = libc::WEXITSTATUS(status);
				(0u8, code)
			} else if libc::WIFSIGNALED(status) {
				let signal = libc::WTERMSIG(status);
				(1u8, signal)
			} else {
				abort!("the guest process exited with neither a code nor a signal");
			};
			let ret = libc::send(
				context.socket.as_raw_fd(),
				std::ptr::addr_of!(kind).cast(),
				std::mem::size_of_val(&kind),
				0,
			);
			if ret == -1 {
				abort_errno!("failed to send the guest process's exit status's kind to the host");
			}
			let ret = libc::send(
				context.socket.as_raw_fd(),
				std::ptr::addr_of!(value).cast(),
				std::mem::size_of_val(&value),
				0,
			);
			if ret == -1 {
				abort_errno!("failed to send the guest process's exit status's value to the host");
			}
		}
	}

	// Exit.
	std::process::exit(0)
}

fn guest_process(mut context: Context) -> ! {
	unsafe {
		// Ask to receive a SIGKILL signal if the root process exits.
		let ret = libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0);
		if ret == -1 {
			abort_errno!("failed to set PDEATHSIG");
		}

		// Redirect stdio.
		redirect_stdio(&mut context.stdin, &mut context.stdout, &mut context.stderr);

		// Wait for the notification from the host process to continue.
		let mut notification = 0u8;
		let ret = libc::recv(
			context.socket.as_raw_fd(),
			std::ptr::addr_of_mut!(notification).cast(),
			std::mem::size_of_val(&notification),
			0,
		);
		if ret == -1 {
			abort_errno!(
				"the guest process failed to receive the notification from the host process to continue"
			);
		}
		assert_eq!(notification, 1);

		// If requested to spawn in a chroot jail, perform the mounts and chroot.
		if context.root.is_some() {
			mount_and_chroot(&mut context);
		}

		// Set the working directory.
		let ret = libc::chdir(context.cwd.as_ptr());
		if ret == -1 {
			abort_errno!("failed to set the working directory");
		}

		// Finally, exec the process.
		libc::execve(
			context.executable.as_ptr(),
			context.argv.as_ptr().cast(),
			context.envp.as_ptr().cast(),
		);
	}

	// If execve returns then abort.
	abort_errno!("failed to call execve");
}

fn mount_and_chroot(context: &mut Context) {
	let root = context.root.as_ref().unwrap();
	unsafe {
		for mount in &context.mounts {
			let source = mount.source.as_ptr();
			let target = mount.target.as_ptr();
			let fstype = mount
				.fstype
				.as_ref()
				.map_or_else(std::ptr::null, |value| value.as_ptr());
			let flags = mount.flags;
			let data = mount
				.data
				.as_ref()
				.map_or_else(std::ptr::null, Vec::as_ptr)
				.cast();
			let ret = libc::mount(source, target, fstype, flags, data);
			if ret == -1 {
				abort_errno!(
					r#"failed to mount "{}" to "{}""#,
					mount.source.to_str().unwrap(),
					mount.target.to_str().unwrap(),
				);
			}
			if mount.readonly {
				let ret = libc::mount(
					source,
					target,
					fstype,
					flags | libc::MS_RDONLY | libc::MS_REMOUNT,
					data,
				);
				if ret == -1 {
					abort_errno!(
						r#"failed to mount "{}" to "{}""#,
						mount.source.to_str().unwrap(),
						mount.target.to_str().unwrap(),
					);
				}
			}
		}

		// Mount the root.
		let ret = libc::mount(
			root.as_ptr(),
			root.as_ptr(),
			std::ptr::null(),
			libc::MS_BIND | libc::MS_PRIVATE | libc::MS_REC,
			std::ptr::null(),
		);
		if ret == -1 {
			abort_errno!("failed to mount the root");
		}

		// Change the working directory to the pivoted root.
		if let Some(root) = &context.root {
			let ret = libc::chdir(root.as_ptr());
			if ret == -1 {
				abort_errno!("failed to change directory to the root");
			}
		}

		// Pivot the root.
		let ret = libc::syscall(libc::SYS_pivot_root, c".".as_ptr(), c".".as_ptr());
		if ret == -1 {
			abort_errno!("failed to pivot the root");
		}

		// Unmount the root.
		let ret = libc::umount2(c".".as_ptr().cast(), libc::MNT_DETACH);
		if ret == -1 {
			abort_errno!("failed to unmount the root");
		}

		// Remount the root as read-only.
		let ret = libc::mount(
			std::ptr::null(),
			c"/".as_ptr().cast(),
			std::ptr::null(),
			libc::MS_BIND | libc::MS_PRIVATE | libc::MS_RDONLY | libc::MS_REC | libc::MS_REMOUNT,
			std::ptr::null(),
		);
		if ret == -1 {
			abort_errno!("failed to remount the root as read-only");
		}
	}
}
