use {
	crate::{
		Command, abort, abort_errno,
		client::{Request, RequestKind, Response, ResponseKind, SpawnResponse, WaitResponse},
		common::{CStringVec, cstring, envstring},
		linux::{get_existing_mount_flags, get_user, guest::mount_and_chroot},
		Options,
	},
	num::ToPrimitive,
	std::{
		collections::BTreeMap,
		io::{Read, Write},
		os::{
			fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
			unix::net::UnixStream,
		},
		path::PathBuf,
		ptr::{addr_of, addr_of_mut},
	},
	tangram_client as tg,
};

pub (crate) struct Init {
	socket: UnixStream,
	signal_fd: OwnedFd,
	buffer: Vec<u8>,
	offset: usize,
	processes: BTreeMap<i32, i32>,
	pending_waits: BTreeMap<i32, u32>,
}

pub fn main(socket: UnixStream, options: &Options) -> ! {
	if socket.set_nonblocking(false).is_err() {
		abort!("failed to set the socket as blocking");
	}

	setup_namespaces(options);

	// Block SIGCHLD and create a signalfd.
	let signal_fd = unsafe {
		let mut sigmask = std::mem::zeroed::<libc::sigset_t>();
		libc::sigemptyset(addr_of_mut!(sigmask));
		libc::sigaddset(addr_of_mut!(sigmask), libc::SIGCHLD);
		libc::sigprocmask(libc::SIG_BLOCK, addr_of!(sigmask), std::ptr::null_mut());
		let fd = libc::signalfd(
			-1,
			addr_of!(sigmask),
			libc::SFD_CLOEXEC | libc::SFD_NONBLOCK,
		);
		if fd < 0 {
			abort_errno!("failed to open signalfd")
		}
		OwnedFd::from_raw_fd(fd)
	};

	// Run the init loop.
	let mut init = Init::new(socket, signal_fd);
	init.run();
}

impl Init {
	fn new(socket: UnixStream, signal_fd: OwnedFd) -> Self {
		Self {
			socket,
			buffer: vec![0u8; 2 << 14],
			offset: 0,
			signal_fd,
			processes: BTreeMap::new(),
			pending_waits: BTreeMap::new(),
		}
	}
	fn run(&mut self) -> ! {
		loop {
			self.poll();
		}
	}

	fn poll(&mut self) {
		let mut fds = [
			libc::pollfd {
				fd: self.socket.as_raw_fd(),
				events: libc::POLLIN,
				revents: 0,
			},
			libc::pollfd {
				fd: self.signal_fd.as_raw_fd(),
				events: libc::POLLIN,
				revents: 0,
			},
		];

		unsafe {
			let result = libc::poll(fds.as_mut_ptr(), 2, 10);
			if result < 0 {
				if std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
					eprintln!("poll error: {}", std::io::Error::last_os_error());
				}
				return;
			}
		}

		if fds[0].revents & libc::POLLIN != 0 {
			if let Some(request) = self.try_receive() {
				self.handle_request(request);
			}
		}

		if fds[1].revents & libc::POLLIN != 0 {
			self.reap_children();
		}
	}

	fn try_receive(&mut self) -> Option<Request> {
		let size = self
			.socket
			.read_u32()
			.inspect_err(|error| eprintln!("failed to read length: {error}"))
			.ok()?;
		self.buffer.resize_with(size as _, || 0);
		self.socket
			.read_exact(&mut self.buffer)
			.inspect_err(|error| eprintln!("failed to read message {error}"))
			.ok()?;
		let mut request = serde_json::from_slice::<Request>(&self.buffer)
			.inspect_err(|error| eprintln!("failed to deserialize message: {error}"))
			.ok()?;
		if let RequestKind::Spawn(spawn) = &mut request.kind {
			unsafe {
				// Receive the file descriptors using recvmsg with SCM_RIGHTS.
				let fd = self.socket.as_raw_fd();

				// Receive the message.
				let mut buffer = [0u8; 1];
				let iov = libc::iovec {
					iov_base: buffer.as_mut_ptr().cast(),
					iov_len: 1,
				};
				let length = libc::CMSG_SPACE((3 * std::mem::size_of::<RawFd>()) as _);
				let mut cmsg_buffer = vec![0u8; length as _];
				let mut msg: libc::msghdr = std::mem::zeroed();
				msg.msg_iov = (&raw const iov).cast_mut();
				msg.msg_iovlen = 1;
				msg.msg_control = cmsg_buffer.as_mut_ptr().cast();
				msg.msg_controllen = cmsg_buffer.len() as _;
				let ret = libc::recvmsg(fd, &raw mut msg, 0);
				if ret < 0 {
					let error = std::io::Error::last_os_error();
					eprintln!("failed to receive the fds: {error}");
					return None;
				}

				// Read the fds.
				let mut fds = Vec::new();
				let cmsg = libc::CMSG_FIRSTHDR(&raw const msg);
				if !cmsg.is_null()
					&& (*cmsg).cmsg_level == libc::SOL_SOCKET
					&& (*cmsg).cmsg_type == libc::SCM_RIGHTS
				{
					let data = libc::CMSG_DATA(cmsg);
					let n = ((*cmsg).cmsg_len.to_usize().unwrap()
						- libc::CMSG_LEN(0).to_usize().unwrap())
						/ std::mem::size_of::<RawFd>();
					for i in 0..n {
						fds.push(
							data.add(i * std::mem::size_of::<RawFd>())
								.cast::<RawFd>()
								.read_unaligned(),
						);
					}
				}

				// Convert.
				spawn.command.stdin = spawn
					.command
					.stdin
					.and_then(|index| fds.get(index.to_usize().unwrap()).copied());
				spawn.command.stdout = spawn
					.command
					.stdin
					.and_then(|index| fds.get(index.to_usize().unwrap()).copied());
				spawn.command.stderr = spawn
					.command
					.stdin
					.and_then(|index| fds.get(index.to_usize().unwrap()).copied());
			}
		}
		Some(request)
	}

	fn handle_request(&mut self, request: Request) {
		let kind = match request.kind {
			RequestKind::Spawn(r) => {
				let kind = spawn(r.command)
					.map(|pid| {
						ResponseKind::Spawn(SpawnResponse {
							pid: Some(pid),
							error: None,
						})
					})
					.unwrap_or_else(|error| {
						let error = tg::error::Data {
							message: Some(error.to_string()),
							..tg::error::Data::default()
						};
						ResponseKind::Spawn(SpawnResponse {
							pid: None,
							error: Some(error),
						})
					});
				Some(kind)
			},
			RequestKind::Wait(wait) => {
				let response = self.processes.remove(&wait.pid).map(|status| {
					ResponseKind::Wait(WaitResponse {
						status: Some(status),
					})
				});
				if response.is_none() {
					self.pending_waits.insert(wait.pid, request.id);
				}
				response
			},
		};
		if let Some(kind) = kind {
			let response = Response {
				id: request.id,
				kind,
			};
			self.send_response(response);
		}
	}

	fn send_response(&mut self, response: Response) {
		let bytes = serde_json::to_vec(&response).unwrap();
		let length = bytes.len().to_u32().unwrap();
		if self
			.socket
			.write_all(&length.to_ne_bytes())
			.inspect_err(|error| eprintln!("failed to write response length: {error}"))
			.is_err()
		{
			return;
		}
		if self
			.socket
			.write_all(&bytes)
			.inspect_err(|error| eprintln!("failed to send response: {error}"))
			.is_err()
		{
			return;
		}
	}

	fn reap_children(&mut self) {
		unsafe {
			let mut buf = std::mem::zeroed::<libc::siginfo_t>();
			loop {
				let n = libc::read(
					self.signal_fd.as_raw_fd(),
					addr_of_mut!(buf).cast(),
					std::mem::size_of::<libc::siginfo_t>(),
				);
				if n < 0 {
					let err = std::io::Error::last_os_error();
					if !matches!(err.kind(), std::io::ErrorKind::WouldBlock) {
						eprintln!("error reaping children: {err}");
					}
					break;
				}
				if n == 0 {
					break;
				}
			}
			loop {
				let mut status = 0;
				let pid = libc::waitpid(-1, addr_of_mut!(status), libc::WNOHANG);
				if pid == 0 {
					break;
				}
				if pid < 0 {
					let err = std::io::Error::last_os_error();
					eprintln!("error waiting for children: {err}");
					break;
				}
				if let Some(id) = self.pending_waits.get(&pid).copied() {
					let kind = ResponseKind::Wait(WaitResponse {
						status: Some(status),
					});
					let response = Response { id, kind };
					self.send_response(response);
				} else {
					self.processes.insert(pid, status);
				}
			}
		}
	}

}


	fn spawn(mut command: Command) -> std::io::Result<i32> {
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
				let existing = get_existing_mount_flags(source)?;
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
				let ret = libc::chdir(cwd.as_ptr());
				if ret == -1 {
					abort_errno!("failed to set the working directory");
				}
				libc::execvpe(
					executable.as_ptr(),
					argv.as_ptr().cast(),
					envp.as_ptr().cast(),
				);
				abort_errno!("execvpe failed");
			}
		}

		guest_to_host_pid(pid as _)
	}

trait ReadExt {
	fn read_u32(&mut self) -> std::io::Result<u32>;
}

impl<T> ReadExt for T
where
	T: std::io::Read,
{
	fn read_u32(&mut self) -> std::io::Result<u32> {
		let mut buf = [0; 4];
		self.read_exact(&mut buf)?;
		Ok(u32::from_ne_bytes(buf))
	}
}

fn setup_namespaces(options: &Options) {
	let (uid, gid) = get_user(options.user.as_ref()).expect("failed to get the uid/gid");
	unsafe {
		let result = libc::unshare(libc::CLONE_NEWUSER);
		if result < 0 {
			abort_errno!("failed to enter a new user namespace");
		}

		// Deny setgroups.
		std::fs::write("/proc/self/setgroups", "deny").expect("failed to deny setgroups");

		// Update uid/gid maps
		let proc_uid = libc::getuid();
		std::fs::write(
			format!("/proc/self/uid_map"),
			format!("{uid} {proc_uid} 1\n"),
		)
		.expect("failed to write the uid map");
		let proc_gid = libc::getgid();
		std::fs::write(
			format!("/proc/self/gid_map"),
			format!("{gid} {proc_gid} 1\n"),
		)
		.expect("failed to write the gid map");

		// Enter a new PID and mount namespace. The first child process will have pid 1. Mounts performed here will not be visible outside the sandbox.
		let result = libc::unshare(libc::CLONE_NEWPID | libc::CLONE_NEWNS);
		if result < 0 {
			abort_errno!("failed to enter a new user namespace");
		}

		// If sandboxing in a network, enter a new network namespace.
		if !options.network {
			let result = libc::unshare(libc::CLONE_NEWNET);
			if result < 0 {
				abort_errno!("failed to enter a new network namespace");
			}
		}

		// If a new hostname is requested, enter a new UTS namespace.
		if let Some(hostname) = &options.hostname {
			let result = libc::unshare(libc::CLONE_NEWUTS);
			if result < 0 {
				abort_errno!("failed to enter a new uts namespace");
			}
			let result = libc::sethostname(hostname.as_ptr().cast(), hostname.len());
			if result < 0 {
				abort_errno!("failed to set the hostname");
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
	let target = target.map(cstring);
	let fstype = mount.fstype.as_ref().map(cstring);
	let data = mount
		.data
		.as_ref()
		.map(|bytes| bytes.as_ptr().cast())
		.unwrap_or(std::ptr::null_mut());
	unsafe {
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
			return Err(std::io::Error::last_os_error());
		}
	}
	Ok(())
}

fn guest_to_host_pid(pid: i32) -> std::io::Result<i32> {
	for entry in std::fs::read_dir("/proc")? {
		let entry = entry?;
		let name = entry.file_name();
		let Some(name) = name.to_str() else {
			continue;
		};
		let Some(host_pid) = name.parse::<i32>().ok() else {
			continue;
		};
		let Some(status) = std::fs::read_to_string(format!("/proc/{host_pid}/status")).ok() else {
			continue;
		};
		let Some(nspid_line) = status.lines().find(|line| line.starts_with("NSpid:")) else {
			continue;
		};
		let mut pids = nspid_line["NSpid:".len()..].split_whitespace();
		let has_host = pids.next().is_some();
		if let Some(ns_pid) = pids.next()
			&& has_host
			&& ns_pid.parse::<i32>() == Ok(pid)
		{
			return Ok(host_pid);
		}
	}
	Err(std::io::Error::other("not foudnfound"))
}
