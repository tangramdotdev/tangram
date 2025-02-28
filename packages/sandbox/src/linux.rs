//! # Linux Sandboxing: the 3 process dance
//!
//! Sandboxing is accomplished using a combination of user, PID, mount, and network namespaces.
//!
//! There are three processes involved:
//!
//! - The parent or "host" process
//! - The "root" process
//! - The "guest" process
//!
//! In `spawn()`, the parent process converts the [Command] into a `Context` object that will be
//! used by the root/guest processes to exec the program, like converting the args/envs/executable
//! to C Strings. It is also necessary to create a socket pair for the host to control the guest.
//!
//! If the [Command] was configured to have a [crate::Tty] then the host process creates a pseudo-
//! terminal master/slave pair by calling [libc::openpty]. See the [crate::pty] module for how this
//! is converted back into async read/write streams for the [Child] object.
//!
//! Otherwise, the host process will create i/o streams if configured:
//!
//! - if stdio is configured to [Stdio::Inherit], do nothing
//! - if stdio is configured to [Stdio::Piped], create a unix socket pair to send/receive i/o with
//!   the host process
//! - if stdio is configured to [Stdio::Null], open '/dev/null'.
//!
//! Once all state is configured, the host process makes the [clone3]() syscall to fork into the
//! first child process, called the "root" with the `CLONE_NEWUSER` flag to create a new user
//! namespace. This is necessary to make the root process act as if it is run as root within the
//! sandbox.
//!
//! The root process has three roles:
//!
//! - 1. clone3 into the guest process with the appropriate flags (setup mount/pid/network namespaces
//!   if requested)
//! - 2. inform the host process of the PID of the guest process.
//! - 3. call waitpid() on the guest process and send the exit code or signal back to the host.
//!
//! After forking with clone3, the root process sends the PID of the guest process back to the host
//! over the socket pair that the host process previously created.
//!
//! Within the guest process, we first set the death signal to terminate if the parent terminates,
//! and then dup2 the i/o FDs that were passed in the context to configure stdio. If configured, the
//! guest process performs any mounts requested and then chroots into a new root. Finally, it waits
//! for a signal from the host process (sent over the same socket mentioned above) to inform it to
//! continue.
//!
//! Now finally, the system is in a pending state. The root/guest processes are blocked waiting for
//! the host to make progress and the guest process has not yet exec'd the requested program. In
//! this state, the returned `Child` can call `wait()` to trigger the process.
//!
//! If the [Child] is dropped before calling wait(), the root process is killed with SIGKILL, which
//! will also kill the guest process.
//!
//! When the host process calls [Child::wait], it receives the PID over the socket sent by the root
//! process and then sets the UID and GID maps of the guest process if configured. It then signals
//! the guest process to continue, and waits on the pipe to receive the exit code/signal from the
//! root process.
//!
//! If this future returned by wait() is dropped before completion, the root/guest processes
//! are killed.
use crate::{Child, Command, ExitStatus, Stderr, Stdin, Stdio, Stdout, pty::Pty};
use num::ToPrimitive;
use std::{
	ffi::{CString, OsStr},
	os::{
		fd::{AsRawFd, IntoRawFd, RawFd},
		unix::ffi::OsStrExt,
	},
};
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
	stdio: Either<Pty, (Option<RawFd>, Option<RawFd>, Option<RawFd>)>,
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
	let (parent_stdio, child_stdio) = if let Some(tty) = command.tty {
		let (parent, child) = Pty::open(tty).await?;
		(Either::Left(parent), Either::Left(child))
	} else {
		let (stdin_parent, stdin_child) = stdio_pair(command.stdin)?;
		let (stdout_parent, stdout_child) = stdio_pair(command.stdout)?;
		let (stderr_parent, stderr_child) = stdio_pair(command.stderr)?;
		(
			Either::Right((stdin_parent, stdout_parent, stderr_parent)),
			Either::Right((stdin_child, stdout_child, stderr_child)),
		)
	};

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
		stdio: child_stdio,
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

	// Split stdio.
	let (stdin, stdout, stderr) = match parent_stdio {
		Either::Left(pty) => {
			let (stdin, stdout, stderr) = pty.into_stdio()?;
			(Some(stdin), Some(stdout), Some(stderr))
		},
		Either::Right((stdin, stdout, stderr)) => {
			let stdin = stdin.map(|inner| Stdin {
				inner: Either::Right(inner),
			});
			let stdout = stdout.map(|inner| Stdout {
				inner: Either::Right(inner),
			});
			let stderr = stderr.map(|inner| Stderr {
				inner: Either::Right(inner),
			});
			(stdin, stdout, stderr)
		},
	};

	// Close unused fds.
	if let Either::Right((stdin, stdout, stderr)) = context.stdio {
		for fd in [stdin, stdout, stderr].into_iter().flatten() {
			unsafe { libc::close(fd) };
		}
	}

	// Create the child.
	let child = Child {
		chroot: command.chroot.is_some(),
		gid: command.gid,
		pid,
		socket: parent_socket,
		uid: command.uid,
		stdin,
		stdout,
		stderr,
	};

	Ok(child)
}

impl Child {
	pub(crate) async fn wait_linux(&mut self) -> std::io::Result<ExitStatus> {
		// If this future is dropped, then kill the root process.
		let root_process = self.pid;
		let child_gid = self.gid;
		let child_uid = self.uid;

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
		if self.chroot {
			let pid = self.socket.read_i32_le().await?;

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
		self.socket.write_u8(1).await?;

		// Read the exit status from the host socket.
		let kind = self.socket.read_u8().await?;
		let value = self.socket.read_i32_le().await?;
		let exit = match kind {
			0 => ExitStatus::Code(value),
			1 => ExitStatus::Signal(value),
			_ => unreachable!(),
		};

		// Wait for the root process to exit.
		tokio::task::spawn_blocking(move || {
			let mut status: libc::c_int = 0;
			let ret = unsafe {
				libc::waitpid(root_process, std::ptr::addr_of_mut!(status), libc::__WALL)
			};
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

struct CStringVec {
	_strings: Vec<CString>,
	pointers: Vec<*const libc::c_char>,
}

impl CStringVec {
	fn as_ptr(&self) -> *const *const libc::c_char {
		self.pointers.as_ptr()
	}
}

impl FromIterator<CString> for CStringVec {
	fn from_iter<T: IntoIterator<Item = CString>>(iter: T) -> Self {
		let mut _strings = Vec::new();
		let mut pointers = Vec::new();
		for cstr in iter {
			pointers.push(cstr.as_ptr());
			_strings.push(cstr);
		}
		pointers.push(std::ptr::null());
		Self { _strings, pointers }
	}
}

fn stdio_pair(stdio: Stdio) -> std::io::Result<(Option<tokio::net::UnixStream>, Option<RawFd>)> {
	match stdio {
		Stdio::Inherit => Ok((None, None)),
		Stdio::Null => {
			let fd = unsafe { libc::open(c"/dev/null".as_ptr(), libc::O_RDWR) };
			if fd < 0 {
				return Err(std::io::Error::last_os_error());
			}
			Ok((None, Some(fd)))
		},
		Stdio::Piped => {
			let (host, guest) = socket_pair()?;
			Ok((Some(host), Some(guest.into_raw_fd())))
		},
	}
}

fn socket_pair() -> std::io::Result<(tokio::net::UnixStream, std::os::unix::net::UnixStream)> {
	let (r#async, sync) = tokio::net::UnixStream::pair()?;
	let sync = sync.into_std()?;
	sync.set_nonblocking(false)?;
	Ok((r#async, sync))
}

fn root_process(mut context: Context) -> ! {
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
			// Close the unused master/slave FDs if necessary.
			if let Either::Left(pty) = &mut context.stdio {
				pty.close_pty();
				pty.close_tty();
			}

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
		redirect_stdio(&mut context);

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
	abort_errno!(r#"failed to call execve"#);
}

fn redirect_stdio(context: &mut Context) {
	unsafe {
		match &mut context.stdio {
			Either::Left(pty) => {
				// Close the master.
				pty.close_pty();

				// Set the tty as the controlling terminal
				if let Err(error) = pty.set_controlling_terminal() {
					abort_errno!("{error}");
				}

				// Redirect stdin/stdout/stderr.
				let ttyfd = pty.tty_fd.take().unwrap();
				libc::dup2(ttyfd, libc::STDIN_FILENO);
				libc::dup2(ttyfd, libc::STDOUT_FILENO);
				libc::dup2(ttyfd, libc::STDERR_FILENO);

				// Close the child.
				libc::close(ttyfd);
			},
			Either::Right((stdin, stdout, stderr)) => {
				for (fd, fileno) in [
					(stdin, libc::STDIN_FILENO),
					(stdout, libc::STDOUT_FILENO),
					(stderr, libc::STDERR_FILENO),
				] {
					if let Some(fd) = *fd {
						libc::dup2(fd, fileno);
					}
				}
			},
		}
	}
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
		let ret = libc::chdir(context.cwd.as_ptr());
		if ret == -1 {
			abort_errno!("failed to change directory to the root");
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

macro_rules! abort {
	($($t:tt)*) => {{
		eprintln!("an error occurred in the linux runtime guest process");
		eprintln!("{}", format_args!($($t)*));
		std::process::exit(1)
	}};
}

use abort;

macro_rules! abort_errno {
	($($t:tt)*) => {{
		eprintln!("an error occurred in the linux runtime guest process");
		eprintln!("{}", format_args!($($t)*));
		eprintln!("{}", std::io::Error::last_os_error());
		std::process::exit(1)
	}};
}

use abort_errno;
