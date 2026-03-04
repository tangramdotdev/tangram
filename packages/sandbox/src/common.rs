use {
	crate::{Command, PtySize},
	std::{
		ffi::{CStr, CString},
		os::fd::{FromRawFd as _, OwnedFd},
		path::Path,
	},
	tangram_client as tg,
};

pub(crate) struct SpawnContext {
	pub(crate) id: tg::process::Id,
	pub(crate) command: Command,
	pub(crate) stdin: OwnedFd,
	pub(crate) stdout: OwnedFd,
	pub(crate) stderr: OwnedFd,
	pub(crate) pty: Option<CString>,
}

pub enum InputStream {
	Null,
	Pipe(tokio::net::unix::pipe::Sender),
	Pty(OwnedFd),
}

pub enum OutputStream {
	Null,
	Pipe(tokio::net::unix::pipe::Receiver),
	Pty(OwnedFd),
}

pub struct Pty {
	pub master: OwnedFd,
	pub slave: OwnedFd,
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
	pub fn new(size: PtySize) -> tg::Result<Self> {
		unsafe {
			let mut win_size = libc::winsize {
				ws_col: size.cols,
				ws_row: size.rows,
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
			let slave = OwnedFd::from_raw_fd(slave);
			let name = CStr::from_ptr(name.as_ptr().cast()).to_owned();
			Ok(Self {
				master,
				slave,
				name,
			})
		}
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

pub(crate) fn start_session(pty: &CString) {
	unsafe {
		let tty = libc::open(c"/dev/tty".as_ptr(), libc::O_RDWR | libc::O_NOCTTY);
		if tty > 0 {
			#[cfg_attr(target_os = "linux", expect(clippy::useless_conversion))]
			libc::ioctl(tty, libc::TIOCNOTTY.into(), std::ptr::null_mut::<()>());
			libc::close(tty);
		}

		// Set the current process as session leader.
		let ret = libc::setsid();
		if ret < 0 {
			abort_errno!("setsid() failed");
		}

		// Open the pty.
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
	}
}
