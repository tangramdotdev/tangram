use super::{chroot::Chroot, TANGRAM_GID, TANGRAM_UID};
use itertools::Itertools;
use std::{
	collections::BTreeMap,
	ffi::CString,
	os::{fd::AsRawFd as _, unix::ffi::OsStrExt},
	path::PathBuf,
};
use tangram_client as tg;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A child process that may or may not be spawned within a chroot jail.
pub struct Child {
	context: Context,
	socket: tokio::net::UnixStream,
	root_process: libc::pid_t,
	pub _stdin: Option<tokio::net::UnixStream>,
	pub stdout: Option<tokio::net::UnixStream>,
	pub stderr: Option<tokio::net::UnixStream>,
}

struct Context {
	argv: CStringVec,
	cwd: CString,
	envp: CStringVec,
	executable: CString,
	chroot: Option<super::chroot::Chroot>,
	network: bool,
	socket: std::os::unix::net::UnixStream,
	stdin: std::os::unix::net::UnixStream,
	stdout: std::os::unix::net::UnixStream,
	stderr: std::os::unix::net::UnixStream,
}

unsafe impl Send for Context {}

#[derive(Clone)]
struct CStringVec {
	_strings: Vec<CString>,
	pointers: Vec<*const libc::c_char>,
}

pub fn spawn(
	args: Vec<String>,
	cwd: PathBuf,
	env: BTreeMap<String, String>,
	executable: String,
	chroot: Option<Chroot>,
	network: bool,
) -> tg::Result<Child> {
	// Create the executable.
	let executable = CString::new(executable)
		.map_err(|source| tg::error!(!source, "the executable is not a valid C string"))?;

	// Create argv.
	let args: Vec<_> = args
		.into_iter()
		.map(CString::new)
		.try_collect()
		.map_err(|source| tg::error!(!source, "failed to convert the args"))?;
	let mut argv = Vec::with_capacity(1 + args.len() + 1);
	argv.push(executable.clone());
	for arg in args {
		argv.push(arg);
	}
	let argv = CStringVec::new(argv);

	// Create cwd.
	let cwd = CString::new(cwd.as_os_str().as_bytes()).unwrap();

	// Create envp.
	let env = env
		.into_iter()
		.map(|(key, value)| format!("{key}={value}"))
		.map(|entry| CString::new(entry).unwrap())
		.collect_vec();
	let mut envp = Vec::with_capacity(env.len() + 1);
	for entry in env {
		envp.push(entry);
	}
	let envp = CStringVec::new(envp);

	// Create a socket for host to guest control.
	let socket = socket_pair()?;

	// Create sockets to redirect stdio.
	let stdin = socket_pair()?;
	let stderr = socket_pair()?;
	let stdout = socket_pair()?;

	// Create the context.
	let context = Context {
		argv,
		cwd,
		envp,
		executable,
		chroot,
		network,
		socket: socket.1,
		stdin: stdin.1,
		stdout: stdout.1,
		stderr: stderr.1,
	};

	// Spawn the root process.
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
		let source = std::io::Error::last_os_error();
		return Err(tg::error!(!source, "failed to spawn child process"));
	}

	// Run the root process.
	if pid == 0 {
		unsafe { root(&context) };
	}

	// Otherwise, Create the child.
	let child = Child {
		root_process: pid,
		context,
		socket: socket.0,
		_stdin: Some(stdin.0),
		stdout: Some(stdout.0),
		stderr: Some(stderr.0),
	};
	Ok(child)
}

impl Child {
	pub async fn wait(mut self) -> tg::Result<tg::process::Exit> {
		// If this future is dropped, then kill the root process.
		let root_process = self.root_process;
		scopeguard::defer! {
			// Kill the root process.
			let ret = unsafe { libc::kill(root_process, libc::SIGKILL) };
			if ret != 0 {
				let error = std::io::Error::last_os_error();
				tracing::trace!(?ret, ?error, "failed to kill the root process");
				return;
			}

			// Wait for the root process to exit.
			tokio::task::spawn_blocking(move || {
				let mut status = 0;
				let ret = unsafe {
					libc::waitpid(
						root_process,
						std::ptr::addr_of_mut!(status),
						libc::__WALL,
					)
				};
				if ret == -1 {
					let error = std::io::Error::last_os_error();
					tracing::trace!(?ret, ?error, "failed to wait for the root process to exit");
				}
			});
		};

		// If the guest process is running in a chroot jail, it's current state is blocked waiting for the host process (the caller) to update its uid and gid maps. We need to wait for the root process to notify the host of the guest's PID after it is cloned.
		if self.context.chroot.is_some() {
			let pid =
				self.socket.read_i32_le().await.map_err(|source| {
					tg::error!(!source, "failed to get PID from guest process")
				})?;

			// Write the guest process's UID map.
			let uid = unsafe { libc::getuid() };
			tokio::fs::write(
				format!("/proc/{pid}/uid_map"),
				format!("{TANGRAM_UID} {uid} 1\n"),
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to set the UID map"))?;

			// Deny setgroups to the process.
			tokio::fs::write(format!("/proc/{pid}/setgroups"), "deny")
				.await
				.map_err(|source| tg::error!(!source, "failed to disable setgroups"))?;

			// Write the guest process's GID map.
			let gid = unsafe { libc::getgid() };
			tokio::fs::write(
				format!("/proc/{pid}/gid_map"),
				format!("{TANGRAM_GID} {gid} 1\n"),
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to set the GID map"))?;
		}

		// Notify the guest that it may continue.
		self.socket
			.write_u8(1)
			.await
			.map_err(|source| tg::error!(!source, "failed to resume guest process"))?;

		// Read the exit status from the host socket.
		let kind = self.socket.read_u8().await.map_err(|error| {
			tg::error!(
				source = error,
				"failed to receive the exit status kind from the root process"
			)
		})?;
		let value = self.socket.read_i32_le().await.map_err(|error| {
			tg::error!(
				source = error,
				"failed to receive the exit status value from the root process"
			)
		})?;
		let exit = match kind {
			0 => tg::process::Exit::Code { code: value },
			1 => tg::process::Exit::Signal { signal: value },
			_ => unreachable!(),
		};

		// Wait for the root process to exit.
		tokio::task::spawn_blocking(move || {
			let mut status: libc::c_int = 0;
			let ret = unsafe {
				libc::waitpid(
					self.root_process,
					std::ptr::addr_of_mut!(status),
					libc::__WALL,
				)
			};
			if ret == -1 {
				return Err(std::io::Error::last_os_error()).map_err(|error| {
					tg::error!(
						source = error,
						"failed to wait for the root process to exit"
					)
				});
			}
			if libc::WIFEXITED(status) {
				let code = libc::WEXITSTATUS(status);
				if code != 0 {
					return Err(tg::error!(%code, "root process exited with nonzero exit code"));
				}
			} else if libc::WIFSIGNALED(status) {
				let signal = libc::WTERMSIG(status);
				if signal != 0 {
					return Err(tg::error!(%signal, "root process exited with signal"));
				}
			} else {
				unreachable!();
			};
			Ok(())
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the root process exit task"))?
		.map_err(|source| tg::error!(!source, "the root process did not exit successfully"))?;

		// Return the exit code.
		Ok(exit)
	}
}

unsafe fn root(context: &Context) -> ! {
	// Ask to receive a SIGKILL signal if the host process exits.
	let ret = libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0);
	if ret == -1 {
		abort_errno!("failed to set PDEATHSIG");
	}

	// Redirect stdio streams.
	for (src, dst) in [
		(&context.stdin, libc::STDIN_FILENO),
		(&context.stdout, libc::STDOUT_FILENO),
		(&context.stderr, libc::STDERR_FILENO),
	] {
		let ret = libc::dup2(src.as_raw_fd(), dst);
		if ret == -1 {
			abort_errno!("failed dup stream");
		}
	}

	// Get the clone flags.
	let mut flags = 0;
	if context.chroot.is_some() {
		flags |= libc::CLONE_NEWNS;
		flags |= libc::CLONE_NEWPID;
	}
	if !context.network {
		flags |= libc::CLONE_NEWNET;
	}

	// Fork.
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
	}

	// Run the guest process.
	if pid == 0 {
		guest(context);
	}

	// Send the guest process's PID to the host process, so the host process can write the UID and GID maps.
	if context.chroot.is_some() {
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

	// Exit.
	std::process::exit(0)
}

unsafe fn guest(context: &Context) -> ! {
	// Ask to receive a SIGKILL signal if the root process exits.
	let ret = libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0);
	if ret == -1 {
		abort_errno!("failed to set PDEATHSIG");
	}

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
	if let Some(chroot) = &context.chroot {
		for mount in &chroot.mounts {
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
			chroot.root.as_ptr(),
			chroot.root.as_ptr(),
			std::ptr::null(),
			libc::MS_BIND | libc::MS_PRIVATE | libc::MS_REC,
			std::ptr::null(),
		);
		if ret == -1 {
			abort_errno!("failed to mount the root");
		}

		// Change the working directory to the pivoted root.
		let ret = libc::chdir(chroot.root.as_ptr());
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

	// Set the working directory.
	let ret = libc::chdir(context.cwd.as_ptr());
	if ret == -1 {
		abort_errno!("failed to set the working directory");
	}

	// Finally, exec the
	libc::execve(
		context.executable.as_ptr(),
		context.argv.as_ptr().cast(),
		context.envp.as_ptr().cast(),
	);

	// If execve returns then abort.
	abort_errno!(r#"failed to call execve"#);
}

impl CStringVec {
	pub fn new(strings: Vec<CString>) -> Self {
		let mut pointers = strings.iter().map(|string| string.as_ptr()).collect_vec();
		pointers.push(std::ptr::null());
		Self {
			_strings: strings,
			pointers,
		}
	}

	pub fn as_ptr(&self) -> *const libc::c_char {
		self.pointers.as_ptr().cast()
	}
}

fn socket_pair() -> tg::Result<(tokio::net::UnixStream, std::os::unix::net::UnixStream)> {
	let (r#async, sync) = tokio::net::UnixStream::pair()
		.map_err(|source| tg::error!(!source, "failed to create stdout socket"))?;
	let sync = sync
		.into_std()
		.map_err(|source| tg::error!(!source, "failed to convert the stdout sender"))?;
	sync.set_nonblocking(false).map_err(|error| {
		tg::error!(
			source = error,
			"failed to set the stdout socket as non-blocking"
		)
	})?;
	Ok((r#async, sync))
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
use num::ToPrimitive;
