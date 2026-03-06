use {
	crate::Command,
	std::{
		ffi::{CStr, CString},
		os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
		path::Path,
		pin::Pin,
		task::{Context, Poll, ready},
	},
	tangram_client as tg,
	tokio::io::{AsyncRead, AsyncWrite, ReadBuf, unix::AsyncFd},
};

pub(crate) struct SpawnContext {
	pub(crate) command: Command,
	pub(crate) stdin: Option<OwnedFd>,
	pub(crate) stdout: Option<OwnedFd>,
	pub(crate) stderr: Option<OwnedFd>,
	pub(crate) pty: Option<CString>,
}

pub enum InputStream {
	Null,
	Pipe(tokio::net::unix::pipe::Sender),
	Pty(AsyncPtyFd),
}

pub enum OutputStream {
	Null,
	Pipe(tokio::net::unix::pipe::Receiver),
	Pty(AsyncPtyFd),
}

pub struct AsyncPtyFd(AsyncFd<OwnedFd>);

pub struct Pty {
	pub master: OwnedFd,
	pub slave: Option<OwnedFd>,
	pub name: CString,
}

/// Resolve a non-absolute executable path by searching the given PATH value.
pub fn which(path: &Path, executable: &std::path::Path) -> Option<std::path::PathBuf> {
	if executable.is_absolute() {
		return Some(executable.to_owned());
	}
	for dir in std::env::split_paths(path) {
		let candidate = dir.join(executable);
		if candidate.is_file() {
			return Some(candidate);
		}
	}
	None
}

impl Pty {
	pub fn new(pty: tg::process::Pty) -> tg::Result<Self> {
		unsafe {
			let mut win_size = libc::winsize {
				ws_col: pty.size.cols,
				ws_row: pty.size.rows,
				ws_xpixel: 0,
				ws_ypixel: 0,
			};
			let mut master = 0;
			let mut slave = 0;
			let mut name = [0; 256];
			let ret = libc::openpty(
				std::ptr::addr_of_mut!(master),
				std::ptr::addr_of_mut!(slave),
				name.as_mut_ptr(),
				std::ptr::null_mut(),
				std::ptr::addr_of_mut!(win_size),
			);
			if ret < 0 {
				let error = std::io::Error::last_os_error();
				let error = tg::error!(!error, "failed to open the pty");
				return Err(error);
			}
			let master = OwnedFd::from_raw_fd(master);
			let slave = Some(OwnedFd::from_raw_fd(slave));
			let name = CStr::from_ptr(name.as_ptr().cast()).to_owned();
			Ok(Self {
				master,
				slave,
				name,
			})
		}
	}
}

impl AsyncPtyFd {
	pub fn new(fd: OwnedFd) -> std::io::Result<Self> {
		// Set non-blocking mode.
		unsafe {
			let flags = libc::fcntl(fd.as_raw_fd(), libc::F_GETFL);
			if flags < 0 {
				return Err(std::io::Error::last_os_error());
			}
			if libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
				return Err(std::io::Error::last_os_error());
			}
		}
		Ok(Self(AsyncFd::new(fd)?))
	}
}

impl AsyncRead for AsyncPtyFd {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		loop {
			let mut guard = ready!(self.0.poll_read_ready(cx))?;
			let fd = self.0.as_raw_fd();
			let unfilled = unsafe { buf.unfilled_mut() };
			let result = unsafe { libc::read(fd, unfilled.as_mut_ptr().cast(), unfilled.len()) };
			if result < 0 {
				let error = std::io::Error::last_os_error();
				if error.kind() == std::io::ErrorKind::WouldBlock {
					guard.clear_ready();
					continue;
				}
				return Poll::Ready(Err(error));
			}
			let n = result as usize;
			unsafe {
				buf.assume_init(n);
			}
			buf.advance(n);
			return Poll::Ready(Ok(()));
		}
	}
}

impl AsyncWrite for AsyncPtyFd {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		loop {
			let mut guard = ready!(self.0.poll_write_ready(cx))?;
			let fd = self.0.as_raw_fd();
			let result = unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) };
			if result < 0 {
				let error = std::io::Error::last_os_error();
				if error.kind() == std::io::ErrorKind::WouldBlock {
					guard.clear_ready();
					continue;
				}
				return Poll::Ready(Err(error));
			}
			return Poll::Ready(Ok(result as usize));
		}
	}

	fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		Poll::Ready(Ok(()))
	}

	fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		Poll::Ready(Ok(()))
	}
}

#[macro_export]
macro_rules! abort {
	($($t:tt)*) => {{
		eprintln!("an error occurred in the child process");
		eprintln!("{}", format_args!($($t)*));
		std::process::exit(105)
	}};
}

#[expect(unused_imports)]
pub use abort;

#[macro_export]
macro_rules! abort_errno {
	($($t:tt)*) => {{
		eprintln!("an error occurred in the child process");
		eprintln!("{}", format_args!($($t)*));
		eprintln!("{}", std::io::Error::last_os_error());
		std::process::exit(std::io::Error::last_os_error().raw_os_error().unwrap_or(1));
	}};
}

pub(crate) fn start_session(pty: &CString, stdin: bool, stdout: bool, stderr: bool) {
	unsafe {
		let tty = libc::open(c"/dev/tty".as_ptr(), libc::O_RDWR | libc::O_NOCTTY);
		if tty >= 0 {
			#[cfg_attr(target_os = "linux", expect(clippy::useless_conversion))]
			libc::ioctl(tty, libc::TIOCNOTTY.into(), std::ptr::null_mut::<()>());
			libc::close(tty);
		}

		// Set the current process as session leader.
		let ret = libc::setsid();
		if ret < 0 {
			abort_errno!("setsid() failed");
		}

		// Open the pty slave.
		let fd = libc::open(pty.as_ptr(), libc::O_RDWR);
		if fd < 0 {
			abort_errno!("failed to open {}", pty.to_string_lossy());
		}

		// Set the pty as the controlling tty.
		#[cfg_attr(target_os = "linux", expect(clippy::useless_conversion))]
		let ret = libc::ioctl(fd, libc::TIOCSCTTY.into(), 0);
		if ret < 0 {
			abort_errno!("failed to set the controlling terminal");
		}

		// Dup the pty slave fd to stdin, stdout, and stderr as needed.
		if stdin {
			libc::dup2(fd, libc::STDIN_FILENO);
		}
		if stdout {
			libc::dup2(fd, libc::STDOUT_FILENO);
		}
		if stderr {
			libc::dup2(fd, libc::STDERR_FILENO);
		}

		// Close the pty slave fd if it is not one of the standard fds.
		if fd > libc::STDERR_FILENO {
			libc::close(fd);
		}
	}
}
