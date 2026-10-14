use {
	std::{
		io::{Read as _, Write as _},
		os::{
			fd::{AsRawFd as _, FromRawFd as _},
			unix::net::UnixStream,
		},
	},
	tangram_client::prelude::*,
};

pub(crate) struct Cleanup {
	socket: tokio::sync::Mutex<Option<UnixStream>>,
}

impl Cleanup {
	#[must_use]
	pub(crate) fn new(socket: UnixStream) -> Self {
		Self {
			socket: tokio::sync::Mutex::new(Some(socket)),
		}
	}

	pub(crate) async fn stop(&self, timeout: std::time::Duration) -> tg::Result<()> {
		// Release the cleanup helper when the sandbox is destroyed.
		let mut state = self.socket.lock().await;
		let Some(socket) = state.as_ref() else {
			return Ok(());
		};
		socket
			.shutdown(std::net::Shutdown::Write)
			.map_err(|error| tg::error!(!error, "failed to stop the sandbox cleanup helper"))?;

		// Wait for confirmation that the descendants have stopped.
		let socket = socket
			.try_clone()
			.map_err(|error| tg::error!(!error, "failed to clone the cleanup socket"))?;
		socket
			.set_nonblocking(true)
			.map_err(|error| tg::error!(!error, "failed to configure the cleanup socket"))?;
		let mut socket = tokio::net::UnixStream::from_std(socket)
			.map_err(|error| tg::error!(!error, "failed to register the cleanup socket"))?;
		let mut response = [0];
		tokio::time::timeout(
			timeout,
			tokio::io::AsyncReadExt::read_exact(&mut socket, &mut response),
		)
		.await
		.map_err(|error| tg::error!(!error, "timed out stopping the sandbox descendants"))?
		.map_err(|error| {
			tg::error!(
				!error,
				"the sandbox cleanup helper exited without completing cleanup"
			)
		})?;
		state.take();

		Ok(())
	}
}

pub(super) fn start(fd: libc::c_int) -> tg::Result<()> {
	// Validate the inherited descriptor before taking ownership.
	if fd <= 2 {
		return Err(tg::error!(
			"the cleanup descriptor must not refer to standard IO"
		));
	}

	// SAFETY: F_GETFD checks the descriptor without modifying it.
	if unsafe { libc::fcntl(fd, libc::F_GETFD) } < 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"invalid cleanup descriptor"
		));
	}

	// Own the inherited control socket before creating the cleanup helper.
	// SAFETY: The runner transfers this descriptor exclusively to the sandbox launcher.
	let socket = unsafe { UnixStream::from_raw_fd(fd) };

	// SAFETY: The launcher is single-threaded and has not started the sandbox server.
	let pid = unsafe { libc::fork() };
	if pid < 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to fork the sandbox cleanup helper"
		));
	}
	if pid != 0 {
		return Ok(());
	}

	// Close inherited descriptors so the helper does not hold the server connection open.
	// SAFETY: The child exclusively owns its descriptor table and retains its control socket.
	unsafe {
		for descriptor in 0..libc::getdtablesize() {
			if descriptor != socket.as_raw_fd() {
				libc::close(descriptor);
			}
		}
	}

	// Wait for the runner to release the sandbox, including when the runner exits.
	let mut socket = socket;
	let mut byte = [0];
	loop {
		match socket.read(&mut byte) {
			Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
			Ok(0) | Err(_) => break,
			Ok(_) => {},
		}
	}

	// Stop descendants even after they leave their original process groups.
	if stop_descendants().is_ok() {
		socket.write_all(&[0]).ok();
	}

	// SAFETY: The forked helper must exit without running launcher destructors.
	unsafe { libc::_exit(0) }
}

fn stop_descendants() -> tg::Result<()> {
	// SAFETY: The process identity getter has no preconditions.
	let own_pid = unsafe { libc::getpid() };
	loop {
		// Enumerate processes with enough room for concurrent forks.
		let mut processes = vec![0; 1024];
		loop {
			let bytes = i32::try_from(processes.len() * std::mem::size_of::<libc::pid_t>())
				.map_err(|_| tg::error!("the process list is too large"))?;

			// SAFETY: The writable buffer is valid for the specified byte count.
			let count = unsafe { libc::proc_listallpids(processes.as_mut_ptr().cast(), bytes) };
			if count <= 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to enumerate the sandbox descendants"
				));
			}
			let count = usize::try_from(count).unwrap();
			if count < processes.len() {
				processes.truncate(count);
				break;
			}
			processes.resize(processes.len() * 2, 0);
		}

		// Let Seatbelt restrict each signal to this sandbox, excluding the cleanup helper itself.
		let mut running = false;
		for pid in processes
			.into_iter()
			.filter(|pid| *pid > 1 && *pid != own_pid)
		{
			// SAFETY: Signal zero checks permission without delivering a signal, and Seatbelt restricts the check to same-sandbox targets.
			if unsafe { libc::kill(pid, 0) } != 0 {
				continue;
			}

			// SAFETY: The output buffer is correctly sized and aligned for PROC_PIDTBSDINFO.
			let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
			let size = i32::try_from(std::mem::size_of_val(&info)).unwrap();

			// SAFETY: The output pointer references the initialized buffer for the requested flavor.
			let result = unsafe {
				libc::proc_pidinfo(pid, libc::PROC_PIDTBSDINFO, 0, (&raw mut info).cast(), size)
			};
			if result != size {
				let error = std::io::Error::last_os_error();
				if error.raw_os_error() == Some(libc::ESRCH) {
					continue;
				}
				return Err(tg::error!(!error, "failed to inspect a sandbox descendant"));
			}
			if info.pbi_status == libc::SZOMB {
				continue;
			}

			// SAFETY: The helper inherited the generated Seatbelt profile, which only permits signaling same-sandbox targets. The kernel checks this permission separately for each target.
			if unsafe { libc::kill(pid, libc::SIGKILL) } == 0 {
				running = true;
			}
		}
		if !running {
			break;
		}
		std::thread::sleep(std::time::Duration::from_millis(10));
	}

	Ok(())
}
