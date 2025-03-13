use crate::{Stdio, pty::Pty};
use std::{
	ffi::{CString, OsStr},
	os::{
		fd::{IntoRawFd as _, RawFd},
		unix::ffi::OsStrExt as _,
	},
};
use tangram_either::Either;

pub type HostIo = Either<Pty, Option<tokio::net::UnixStream>>;
pub type GuestIo = Either<Pty, Option<RawFd>>;

pub struct CStringVec {
	_strings: Vec<CString>,
	pointers: Vec<*const libc::c_char>,
}

unsafe impl Send for CStringVec {}

impl CStringVec {
	pub fn as_ptr(&self) -> *const *const libc::c_char {
		self.pointers.as_ptr()
	}
}

pub fn redirect_stdio(stdin: &mut GuestIo, stdout: &mut GuestIo, stderr: &mut GuestIo) {
	unsafe {
		// Flag to make sure we only set the controlling terminal once.
		let mut set_controlling_terminal = false;

		// Dup stdin/stdout/stderr.
		for (fd, io) in [
			(libc::STDIN_FILENO, stdin),
			(libc::STDOUT_FILENO, stdout),
			(libc::STDERR_FILENO, stderr),
		] {
			match io {
				Either::Left(pty) => {
					// Set the controlling terminal if we haven't already.
					if !set_controlling_terminal {
						set_controlling_terminal = true;
						if let Err(error) = pty.set_controlling_terminal() {
							abort_errno!("failed to set controlling terminal {error}");
						}
					}

					// Close the pty fd.
					pty.close_pty();

					// Dup the tty fd.
					libc::dup2(pty.tty_fd.take().unwrap(), fd);
				},
				Either::Right(Some(io)) => {
					libc::dup2(*io, fd);
				},
				Either::Right(None) => (),
			}
		}
	}
}

pub async fn stdio_pair(stdio: Stdio, pty: &mut Option<Pty>) -> std::io::Result<(HostIo, GuestIo)> {
	match stdio {
		Stdio::Inherit => Ok((Either::Right(None), Either::Right(None))),
		Stdio::Null => {
			let fd = unsafe { libc::open(c"/dev/null".as_ptr(), libc::O_RDWR) };
			if fd < 0 {
				return Err(std::io::Error::last_os_error());
			}
			Ok((Either::Right(None), Either::Right(Some(fd))))
		},
		Stdio::Piped => {
			let (host, guest) = socket_pair()?;
			Ok((
				Either::Right(Some(host)),
				Either::Right(Some(guest.into_raw_fd())),
			))
		},
		Stdio::Tty(tty) => {
			// Open the PTY if it doesn't exist.
			if pty.is_none() {
				pty.replace(Pty::open(tty).await?);
			}
			let pty = pty.as_ref().unwrap();
			let host = pty.clone();
			let guest = pty.clone();
			Ok((Either::Left(host), Either::Left(guest)))
		},
	}
}

pub fn socket_pair() -> std::io::Result<(tokio::net::UnixStream, std::os::unix::net::UnixStream)> {
	let (r#async, sync) = tokio::net::UnixStream::pair()?;
	let sync = sync.into_std()?;
	sync.set_nonblocking(false)?;
	Ok((r#async, sync))
}

pub fn cstring(s: impl AsRef<OsStr>) -> CString {
	CString::new(s.as_ref().as_bytes()).unwrap()
}

pub fn envstring(k: impl AsRef<OsStr>, v: impl AsRef<OsStr>) -> CString {
	let string = format!(
		"{}={}",
		k.as_ref().to_string_lossy(),
		v.as_ref().to_string_lossy()
	);
	CString::new(string).unwrap()
}

impl FromIterator<CString> for CStringVec {
	fn from_iter<T: IntoIterator<Item = CString>>(iter: T) -> Self {
		let mut strings = Vec::new();
		let mut pointers = Vec::new();
		for cstr in iter {
			pointers.push(cstr.as_ptr());
			strings.push(cstr);
		}
		pointers.push(std::ptr::null());
		Self {
			_strings: strings,
			pointers,
		}
	}
}

#[macro_export]
macro_rules! abort {
	($($t:tt)*) => {{
		eprintln!("an error occurred in the guest process");
		eprintln!("{}", format_args!($($t)*));
		std::process::exit(105)
	}};
}

#[allow(unused_imports)]
pub use abort;

#[macro_export]
macro_rules! abort_errno {
	($($t:tt)*) => {{
		eprintln!("an error occurred in the guest process");
		eprintln!("{}", format_args!($($t)*));
		eprintln!("{}", std::io::Error::last_os_error());
		std::process::exit(std::io::Error::last_os_error().raw_os_error().unwrap_or(1));
	}};
}

pub use abort_errno;
