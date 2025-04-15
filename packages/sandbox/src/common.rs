use crate::{
	Stdio,
	pty::Pty,
	stdio::{Reader, Writer},
};
use std::{
	collections::VecDeque,
	ffi::{CString, OsStr},
	os::{
		fd::{FromRawFd, IntoRawFd as _, OwnedFd, RawFd},
		unix::ffi::OsStrExt as _,
	},
};

pub struct HostStdio {
	pub stdin: Option<Writer>,
	pub stdout: Option<Reader>,
	pub stderr: Option<Reader>,
}

pub struct GuestStdio {
	pub pty: Option<Pty>,
	pub stdin: Option<RawFd>,
	pub stdout: Option<RawFd>,
	pub stderr: Option<RawFd>,
}

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

pub(crate) async fn create_stdio(
	command: &crate::Command,
) -> std::io::Result<(HostStdio, GuestStdio)> {
	// Open a pty if necessary.
	let tty = [command.stdin, command.stdout, command.stderr]
		.into_iter()
		.find_map(|io| {
			if let Stdio::Tty(tty) = io {
				Some(tty)
			} else {
				None
			}
		});
	let mut pty = None;
	let mut pty_reader = None;
	let mut pty_writer = None;
	if let Some(tty) = tty {
		let mut pty_ = Pty::open(tty).await?;
		let (reader, writer) = pty_.into_reader_writer()?;
		pty.replace(pty_);
		pty_reader.replace(reader);
		pty_writer.replace(writer);
	}

	// Create stdio.
	let mut host = HostStdio {
		stdin: None,
		stdout: None,
		stderr: None,
	};
	let mut guest = GuestStdio {
		pty,
		stdin: None,
		stdout: None,
		stderr: None,
	};

	// Create stdin.
	match command.stdin {
		Stdio::Inherit | Stdio::Null => (),
		Stdio::Tty(_) => {
			host.stdin = pty_writer;
			guest.stdin = guest.pty.as_ref().unwrap().tty_fd;
		},
		Stdio::Pipe => {
			let (reader, writer) = pipe()?;
			host.stdin.replace(writer);
			guest.stdin.replace(reader.into_raw_fd());
		},
	}

	// Create stdout.
	match command.stdout {
		Stdio::Inherit | Stdio::Null => (),
		Stdio::Tty(_) => {
			host.stdout = pty_reader.take();
			guest.stdout = guest.pty.as_ref().unwrap().tty_fd;
		},
		Stdio::Pipe => {
			eprintln!("stdout pipe");
			let (reader, writer) = pipe()?;
			host.stdout.replace(reader);
			guest.stdout.replace(writer.into_raw_fd());
		},
	}

	// Create stderr.
	match command.stderr {
		Stdio::Inherit | Stdio::Null => (),
		Stdio::Tty(_) => {
			host.stderr = pty_reader.take();
			guest.stderr = guest.pty.as_ref().unwrap().tty_fd;
		},
		Stdio::Pipe => {
			eprintln!("stderr pipe");
			let (reader, writer) = pipe()?;
			host.stderr.replace(reader);
			guest.stderr.replace(writer.into_raw_fd());
		},
	}

	Ok((host, guest))
}

pub fn pipe() -> std::io::Result<(Reader, Writer)> {
	unsafe {
		let mut fds = [0; 2];
		if libc::pipe(fds.as_mut_ptr()) != 0 {
			return Err(std::io::Error::last_os_error());
		}
		eprintln!("pipes: {fds:?}");
		let reader = Reader {
			file: OwnedFd::from_raw_fd(fds[0]),
			buffer: VecDeque::new(),
			future: None,
		};
		let writer = Writer {
			file: OwnedFd::from_raw_fd(fds[1]),
			isatty: false,
			future: None,
		};
		Ok((reader, writer))
	}
}

pub fn redirect_stdio(io: &mut GuestStdio) {
	// Flag to make sure we only set the controlling terminal once.
	if let Some(pty) = &mut io.pty {
		if let Err(error) = pty.set_controlling_terminal() {
			abort_errno!("failed to set controlling terminal {error}");
		}
		pty.close_pty();
	}

	// Dup stdio
	for (src, dst) in [
		(io.stdin, libc::STDIN_FILENO),
		(io.stdout, libc::STDOUT_FILENO),
		(io.stderr, libc::STDERR_FILENO),
	] {
		let Some(src) = src else {
			continue;
		};
		unsafe {
			libc::dup2(src, dst);
		}
	}

	// Close unused. This is in a separate loop to avoid closing the same fd twice.
	for src in [io.stdin, io.stdout, io.stderr] {
		let Some(src) = src else {
			continue;
		};
		unsafe {
			libc::close(src);
		}
	}
}

#[cfg(target_os = "linux")]
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
