use std::{
	ffi::{CString, OsStr},
	os::{fd::RawFd, unix::ffi::OsStrExt as _},
};

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

#[allow(dead_code)]
pub fn set_controlling_terminal(tty: RawFd) -> std::io::Result<()> {
	unsafe {
		// Disconnect from the old controlling terminal.
		let fd = libc::open(c"/dev/tty".as_ptr(), libc::O_RDWR | libc::O_NOCTTY);
		#[allow(clippy::useless_conversion)]
		if fd > 0 {
			libc::ioctl(fd, libc::TIOCNOTTY.into(), std::ptr::null_mut::<()>());
			libc::close(fd);
		}

		// Set the current process as session leader.
		if libc::setsid() == -1 {
			return Err(std::io::Error::last_os_error());
		}

		// Verify that we disconnected from the controlling terminal.
		let fd = libc::open(c"/dev/tty".as_ptr(), libc::O_RDWR | libc::O_NOCTTY);
		if fd >= 0 {
			libc::close(fd);
			return Err(std::io::Error::other("failed to remove controlling tty"));
		}

		// Set the slave as the controlling tty.
		#[allow(clippy::useless_conversion)]
		if libc::ioctl(tty, libc::TIOCSCTTY.into(), 0) < 0 {
			return Err(std::io::Error::last_os_error());
		}

		Ok(())
	}
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
		eprintln!("an error occurred in the child process");
		eprintln!("{}", format_args!($($t)*));
		std::process::exit(105)
	}};
}

#[allow(unused_imports)]
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

pub use abort_errno;
