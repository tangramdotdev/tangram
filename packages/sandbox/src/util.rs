use std::{
	os::fd::{AsRawFd as _, OwnedFd},
	path::Path,
};

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

pub(crate) fn start_session(tty: &OwnedFd, stdin: bool, stdout: bool, stderr: bool) {
	unsafe {
		let current_tty = libc::open(c"/dev/tty".as_ptr(), libc::O_RDWR | libc::O_NOCTTY);
		if current_tty >= 0 {
			#[cfg_attr(target_os = "linux", expect(clippy::useless_conversion))]
			libc::ioctl(
				current_tty,
				libc::TIOCNOTTY.into(),
				std::ptr::null_mut::<()>(),
			);
			libc::close(current_tty);
		}

		let ret = libc::setsid();
		if ret < 0 {
			abort_errno!("setsid() failed");
		}

		let fd = tty.as_raw_fd();
		#[cfg_attr(target_os = "linux", expect(clippy::useless_conversion))]
		let ret = libc::ioctl(fd, libc::TIOCSCTTY.into(), 0);
		if ret < 0 {
			abort_errno!("failed to set the controlling terminal");
		}

		if stdin {
			libc::dup2(fd, libc::STDIN_FILENO);
		}
		if stdout {
			libc::dup2(fd, libc::STDOUT_FILENO);
		}
		if stderr {
			libc::dup2(fd, libc::STDERR_FILENO);
		}

		if fd > libc::STDERR_FILENO {
			let flags = libc::fcntl(fd, libc::F_GETFD);
			if flags >= 0 {
				libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC);
			}
		}
	}
}
