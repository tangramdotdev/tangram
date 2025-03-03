use crate::{Stdio, pty::Pty};
use std::{
	ffi::{CString, OsStr},
	os::{
		fd::{IntoRawFd as _, RawFd},
		unix::ffi::OsStrExt as _,
	},
};
use tangram_either::Either;

pub type GuestIo = Either<Pty, (Option<RawFd>, Option<RawFd>, Option<RawFd>)>;

pub fn redirect_stdio(stdio: &mut GuestIo) {
	unsafe {
		match stdio {
			Either::Left(pty) => {
				// Set the tty as the controlling terminal
				if let Err(error) = pty.set_controlling_terminal() {
					abort!("failed to set the controlling terminal: {error}");
				}

				// Close the master.
				pty.close_pty();

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

pub fn stdio_pair(
	stdio: Stdio,
) -> std::io::Result<(Option<tokio::net::UnixStream>, Option<RawFd>)> {
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

#[macro_export]
macro_rules! abort {
	($($t:tt)*) => {{
		eprintln!("an error occurred in the guest process");
		eprintln!("{}", format_args!($($t)*));
		std::process::exit(1)
	}};
}

pub use abort;

#[macro_export]
macro_rules! abort_errno {
	($($t:tt)*) => {{
		eprintln!("an error occurred in the guest process");
		eprintln!("{}", format_args!($($t)*));
		eprintln!("{}", std::io::Error::last_os_error());
		std::process::exit(1)
	}};
}

pub use abort_errno;
