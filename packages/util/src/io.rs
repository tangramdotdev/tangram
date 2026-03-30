use {
	bytes::Bytes,
	futures::Stream,
	num::ToPrimitive as _,
	std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
	tangram_futures::{stream::Ext as _, task::Stopper},
	tokio_stream::wrappers::ReceiverStream,
};

enum Readiness {
	Stdin,
	Stopped,
}

pub fn stdin() -> std::io::Result<impl Stream<Item = std::io::Result<Bytes>> + Send + 'static> {
	let stopper = Stopper::new();
	let (send, recv) = tokio::sync::mpsc::channel(1);
	let (stop_read, stop_write) = pipe()?;
	let stopper_ = stopper.clone();
	tokio::spawn(async move {
		stopper_.wait().await;
		let _ = write_stop(stop_write.as_raw_fd());
	});
	std::thread::spawn(move || stdin_thread(&send, &stop_read));
	Ok(ReceiverStream::new(recv).attach(scopeguard::guard(stopper, |stopper| stopper.stop())))
}

fn pipe() -> std::io::Result<(OwnedFd, OwnedFd)> {
	let mut fds = [0; 2];
	let result = unsafe { libc::pipe(fds.as_mut_ptr()) };
	if result < 0 {
		return Err(std::io::Error::last_os_error());
	}
	let read = unsafe { OwnedFd::from_raw_fd(fds[0]) };
	let write = unsafe { OwnedFd::from_raw_fd(fds[1]) };
	Ok((read, write))
}

fn read(fd: i32, length: usize) -> std::io::Result<Option<Bytes>> {
	let mut buffer = vec![0; length];
	let bytes_read = loop {
		let bytes_read =
			unsafe { libc::read(fd, buffer.as_mut_ptr().cast::<libc::c_void>(), buffer.len()) };
		if bytes_read >= 0 {
			break bytes_read.to_usize().unwrap();
		}
		let error = std::io::Error::last_os_error();
		if error.kind() == std::io::ErrorKind::Interrupted {
			continue;
		}
		return Err(error);
	};
	if bytes_read == 0 {
		return Ok(None);
	}
	buffer.truncate(bytes_read);
	Ok(Some(Bytes::copy_from_slice(&buffer)))
}

fn stdin_thread(sender: &tokio::sync::mpsc::Sender<std::io::Result<Bytes>>, stop_read: &OwnedFd) {
	loop {
		match wait_for_stdin_or_stop(stop_read.as_raw_fd()) {
			Ok(Readiness::Stopped) => break,
			Ok(Readiness::Stdin) => match read(libc::STDIN_FILENO, 4096) {
				Ok(Some(bytes)) => {
					if sender.blocking_send(Ok(bytes)).is_err() {
						break;
					}
				},
				Ok(None) => break,
				Err(error) => {
					let _ = sender.blocking_send(Err(error));
					break;
				},
			},
			Err(error) => {
				let _ = sender.blocking_send(Err(error));
				break;
			},
		}
	}
}

fn wait_for_stdin_or_stop(stop_fd: i32) -> std::io::Result<Readiness> {
	loop {
		let mut fds = [
			libc::pollfd {
				fd: libc::STDIN_FILENO,
				events: libc::POLLIN,
				revents: 0,
			},
			libc::pollfd {
				fd: stop_fd,
				events: libc::POLLIN,
				revents: 0,
			},
		];
		let result = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, -1) };
		if result < 0 {
			let error = std::io::Error::last_os_error();
			if error.kind() == std::io::ErrorKind::Interrupted {
				continue;
			}
			return Err(error);
		}
		let stop_revents = fds[1].revents;
		if stop_revents & (libc::POLLIN | libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
			return Ok(Readiness::Stopped);
		}
		let stdin_revents = fds[0].revents;
		if stdin_revents & libc::POLLNVAL != 0 {
			return Err(std::io::Error::other("failed to poll stdin"));
		}
		if stdin_revents & (libc::POLLIN | libc::POLLERR | libc::POLLHUP) != 0 {
			return Ok(Readiness::Stdin);
		}
	}
}

fn write_stop(stop_fd: i32) -> std::io::Result<()> {
	let byte = [0_u8; 1];
	loop {
		let bytes_written =
			unsafe { libc::write(stop_fd, byte.as_ptr().cast::<libc::c_void>(), byte.len()) };
		if bytes_written >= 0 {
			return Ok(());
		}
		let error = std::io::Error::last_os_error();
		match error.kind() {
			std::io::ErrorKind::BrokenPipe => return Ok(()),
			std::io::ErrorKind::Interrupted => {},
			_ => return Err(error),
		}
	}
}
