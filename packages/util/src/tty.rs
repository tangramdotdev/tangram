#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Size {
	pub cols: u16,
	pub rows: u16,
}

#[cfg(unix)]
pub fn is_tty(fd: libc::c_int) -> bool {
	unsafe { libc::isatty(fd) == 1 }
}

#[cfg(not(unix))]
pub fn is_tty(_fd: libc::c_int) -> bool {
	false
}

#[cfg(unix)]
pub fn is_foreground_controlling_tty(fd: libc::c_int) -> bool {
	if !is_tty(fd) {
		return false;
	}

	let foreground_pgid = unsafe { libc::tcgetpgrp(fd) };
	if foreground_pgid < 0 {
		return false;
	}

	foreground_pgid == unsafe { libc::getpgrp() }
}

#[cfg(not(unix))]
pub fn is_foreground_controlling_tty(fd: libc::c_int) -> bool {
	is_tty(fd)
}

#[cfg(unix)]
pub fn open_controlling_tty() -> Option<std::fs::File> {
	std::fs::OpenOptions::new()
		.read(true)
		.write(true)
		.open("/dev/tty")
		.ok()
}

#[cfg(not(unix))]
pub fn open_controlling_tty() -> Option<std::fs::File> {
	None
}

#[cfg(unix)]
pub fn get_tty_size(fd: libc::c_int) -> Option<Size> {
	let mut size = unsafe { std::mem::zeroed::<libc::winsize>() };
	if unsafe { libc::ioctl(fd, libc::TIOCGWINSZ, &mut size) } < 0
		|| size.ws_col == 0
		|| size.ws_row == 0
	{
		return None;
	}

	Some(Size {
		cols: size.ws_col,
		rows: size.ws_row,
	})
}

#[cfg(not(unix))]
pub fn get_tty_size(_fd: libc::c_int) -> Option<Size> {
	None
}

#[cfg(unix)]
pub fn get_cursor_position(fd: libc::c_int) -> std::io::Result<(u16, u16)> {
	if !is_foreground_controlling_tty(fd) {
		return Err(std::io::Error::other("tty is not in the foreground"));
	}

	let mut original = std::mem::MaybeUninit::<libc::termios>::uninit();
	if unsafe { libc::tcgetattr(fd, original.as_mut_ptr()) } != 0 {
		return Err(std::io::Error::last_os_error());
	}
	let original = unsafe { original.assume_init() };
	let mut raw = original;
	unsafe {
		libc::cfmakeraw(std::ptr::addr_of_mut!(raw));
	}
	if unsafe { libc::tcsetattr(fd, libc::TCSAFLUSH, std::ptr::addr_of!(raw)) } != 0 {
		return Err(std::io::Error::last_os_error());
	}
	let _guard = scopeguard::guard(original, move |original| unsafe {
		libc::tcsetattr(fd, libc::TCSAFLUSH, std::ptr::addr_of!(original));
	});

	let query = b"\x1b[6n";
	let written = unsafe { libc::write(fd, query.as_ptr().cast(), query.len()) };
	if written < 0 {
		return Err(std::io::Error::last_os_error());
	}

	let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
	let mut buffer = Vec::new();
	let mut pollfd = libc::pollfd {
		fd,
		events: libc::POLLIN,
		revents: 0,
	};
	loop {
		let now = std::time::Instant::now();
		if now >= deadline {
			return Err(std::io::Error::other(
				"The cursor position could not be read within a normal duration",
			));
		}
		let timeout_millis = (deadline - now).as_millis();
		let timeout = i32::try_from(timeout_millis).unwrap_or(i32::MAX);
		let ready = unsafe { libc::poll(std::ptr::addr_of_mut!(pollfd), 1, timeout) };
		if ready < 0 {
			let error = std::io::Error::last_os_error();
			if error.kind() == std::io::ErrorKind::Interrupted {
				continue;
			}
			return Err(error);
		}
		if ready == 0 {
			return Err(std::io::Error::other(
				"The cursor position could not be read within a normal duration",
			));
		}
		let mut chunk = [0; 64];
		let read = unsafe { libc::read(fd, chunk.as_mut_ptr().cast(), chunk.len()) };
		if read < 0 {
			let error = std::io::Error::last_os_error();
			if error.kind() == std::io::ErrorKind::Interrupted {
				continue;
			}
			return Err(error);
		}
		if read == 0 {
			continue;
		}
		let read = read.cast_unsigned();
		buffer.extend_from_slice(&chunk[..read]);
		if let Some(position) = parse_cursor_position_response(&buffer) {
			return Ok(position);
		}
		if buffer.len() > 4096 {
			buffer.drain(..buffer.len() - 1024);
		}
	}
}

#[cfg(not(unix))]
pub fn get_cursor_position(_fd: libc::c_int) -> std::io::Result<(u16, u16)> {
	Err(std::io::Error::other("cursor position is unsupported"))
}

pub fn get_controlling_tty_size() -> Option<Size> {
	let tty = open_controlling_tty()?;
	#[cfg(unix)]
	{
		use std::os::fd::AsRawFd as _;
		get_tty_size(tty.as_raw_fd())
	}
	#[cfg(not(unix))]
	{
		let _ = tty;
		None
	}
}

#[cfg(unix)]
fn parse_cursor_position_response(bytes: &[u8]) -> Option<(u16, u16)> {
	let mut index = bytes.len();
	while let Some(escape) = bytes[..index].iter().rposition(|byte| *byte == b'\x1b') {
		let sequence = bytes.get(escape + 1..)?;
		let Some(sequence) = sequence.strip_prefix(b"[") else {
			index = escape;
			continue;
		};
		let end = sequence.iter().position(|byte| *byte == b'R')?;
		let sequence = std::str::from_utf8(&sequence[..end]).ok()?;
		let (row, column) = sequence.split_once(';')?;
		let row = row.parse::<u16>().ok()?;
		let column = column.parse::<u16>().ok()?;
		let row = row.checked_sub(1)?;
		let column = column.checked_sub(1)?;
		return Some((column, row));
	}
	None
}
