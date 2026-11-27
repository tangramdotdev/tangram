use std::{
	os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
	path::Path,
};

/// A watcher that blocks until a file is created, removed, or written in a log directory. On Linux, it watches the directory with inotify, which reports writes to the files in it. On macOS, it watches the directory and the current file with kqueue, because the directory's events do not include writes to its files.
pub struct Watcher {
	#[cfg(target_os = "macos")]
	#[expect(
		dead_code,
		reason = "the watcher holds the descriptor so that its watch lasts"
	)]
	directory: OwnedFd,

	#[cfg(target_os = "macos")]
	file: Option<OwnedFd>,

	fd: OwnedFd,
}

impl Watcher {
	/// Create a watcher for the directory.
	#[cfg(target_os = "linux")]
	pub fn new(directory: &Path) -> std::io::Result<Self> {
		// Create the inotify instance.
		// SAFETY: The call takes only a scalar argument and returns a new descriptor.
		let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC) };
		if fd < 0 {
			return Err(std::io::Error::last_os_error());
		}
		// SAFETY: The call succeeded, so the descriptor is open and nothing else owns it.
		let fd = unsafe { OwnedFd::from_raw_fd(fd) };

		// Watch the directory.
		let path = path_to_c_string(directory)?;
		let mask = libc::IN_CLOSE_WRITE
			| libc::IN_CREATE
			| libc::IN_DELETE
			| libc::IN_MODIFY
			| libc::IN_MOVED_FROM
			| libc::IN_MOVED_TO;
		// SAFETY: The descriptor is an open inotify instance, and the path is a valid C string for the duration of the call.
		let ret = unsafe { libc::inotify_add_watch(fd.as_raw_fd(), path.as_ptr(), mask) };
		if ret < 0 {
			return Err(std::io::Error::last_os_error());
		}

		let watcher = Self { fd };
		Ok(watcher)
	}

	/// Create a watcher for the directory.
	#[cfg(target_os = "macos")]
	pub fn new(directory: &Path) -> std::io::Result<Self> {
		// Create the kqueue.
		// SAFETY: The call takes no arguments and returns a new descriptor.
		let fd = unsafe { libc::kqueue() };
		if fd < 0 {
			return Err(std::io::Error::last_os_error());
		}
		// SAFETY: The call succeeded, so the descriptor is open and nothing else owns it.
		let fd = unsafe { OwnedFd::from_raw_fd(fd) };
		set_cloexec(&fd)?;

		// Open the directory for events only.
		let path = path_to_c_string(directory)?;
		// SAFETY: The path is a valid C string for the duration of the call.
		let directory = unsafe { libc::open(path.as_ptr(), libc::O_EVTONLY | libc::O_CLOEXEC) };
		if directory < 0 {
			return Err(std::io::Error::last_os_error());
		}
		// SAFETY: The call succeeded, so the descriptor is open and nothing else owns it.
		let directory = unsafe { OwnedFd::from_raw_fd(directory) };

		// Watch the directory.
		register(&fd, &directory, libc::NOTE_WRITE)?;

		let watcher = Self {
			directory,
			file: None,
			fd,
		};
		Ok(watcher)
	}

	/// Watch the file for writes in addition to the directory, in place of the file that was watched before.
	#[cfg(target_os = "linux")]
	pub fn watch_file(&mut self, _file: &std::fs::File) -> std::io::Result<()> {
		// Do nothing, because the directory's watch reports writes to its files.
		Ok(())
	}

	/// Watch the file for writes in addition to the directory, in place of the file that was watched before.
	#[cfg(target_os = "macos")]
	pub fn watch_file(&mut self, file: &std::fs::File) -> std::io::Result<()> {
		// Duplicate the descriptor, so that the watch lasts as long as the watcher holds it, and closing it removes the watch.
		let file = file.try_clone()?;
		let file = OwnedFd::from(file);
		register(&self.fd, &file, libc::NOTE_EXTEND | libc::NOTE_WRITE)?;
		self.file = Some(file);
		Ok(())
	}

	/// Block until at least one change happens after the previous wait.
	#[cfg(target_os = "linux")]
	pub fn wait(&self) -> std::io::Result<()> {
		let mut buffer = [0u8; 4096];
		loop {
			// SAFETY: The descriptor is an open inotify instance, and the buffer is valid for writes of its length for the duration of the call.
			let ret = unsafe {
				libc::read(
					self.fd.as_raw_fd(),
					buffer.as_mut_ptr().cast(),
					buffer.len(),
				)
			};
			if ret < 0 {
				let error = std::io::Error::last_os_error();
				if error.kind() == std::io::ErrorKind::Interrupted {
					continue;
				}
				return Err(error);
			}
			return Ok(());
		}
	}

	/// Block until at least one change happens after the previous wait.
	#[cfg(target_os = "macos")]
	pub fn wait(&self) -> std::io::Result<()> {
		// SAFETY: An all-zero `kevent` is a valid value.
		let mut events: [libc::kevent; 8] = unsafe { std::mem::zeroed() };
		loop {
			// SAFETY: The descriptor is an open kqueue, the change list is empty, and the event list is valid for writes of its length for the duration of the call.
			let ret = unsafe {
				libc::kevent(
					self.fd.as_raw_fd(),
					std::ptr::null(),
					0,
					events.as_mut_ptr(),
					8,
					std::ptr::null(),
				)
			};
			if ret < 0 {
				let error = std::io::Error::last_os_error();
				if error.kind() == std::io::ErrorKind::Interrupted {
					continue;
				}
				return Err(error);
			}
			return Ok(());
		}
	}
}

/// Register a vnode watch for the descriptor with the kqueue.
#[cfg(target_os = "macos")]
fn register(kqueue: &OwnedFd, fd: &OwnedFd, fflags: u32) -> std::io::Result<()> {
	#[expect(clippy::cast_sign_loss, reason = "an open descriptor is not negative")]
	let ident = fd.as_raw_fd() as usize;
	let event = libc::kevent {
		data: 0,
		fflags,
		filter: libc::EVFILT_VNODE,
		flags: libc::EV_ADD | libc::EV_CLEAR,
		ident,
		udata: std::ptr::null_mut(),
	};
	// SAFETY: The descriptor is an open kqueue, the change list holds one valid event, and the event list is empty.
	let ret = unsafe {
		libc::kevent(
			kqueue.as_raw_fd(),
			&raw const event,
			1,
			std::ptr::null_mut(),
			0,
			std::ptr::null(),
		)
	};
	if ret < 0 {
		return Err(std::io::Error::last_os_error());
	}
	Ok(())
}

/// Set the close-on-exec flag of the descriptor, so that the server's children do not inherit it.
#[cfg(target_os = "macos")]
fn set_cloexec(fd: &OwnedFd) -> std::io::Result<()> {
	// SAFETY: The descriptor is open, and the call takes only scalar arguments.
	let ret = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) };
	if ret < 0 {
		return Err(std::io::Error::last_os_error());
	}
	Ok(())
}

/// Convert a path to a C string.
fn path_to_c_string(path: &Path) -> std::io::Result<std::ffi::CString> {
	let bytes = std::os::unix::ffi::OsStrExt::as_bytes(path.as_os_str()).to_vec();
	std::ffi::CString::new(bytes).map_err(std::io::Error::other)
}
