use std::{
	ffi::OsStr,
	io::ErrorKind,
	os::{
		fd::{AsFd, AsRawFd, BorrowedFd, RawFd},
		unix::net::{SocketAddr, UnixDatagram},
	},
	path::Path,
};

/// A socket that receives service manager notifications, such as `READY=1`.
pub struct Listener {
	datagram: UnixDatagram,
	guard: crate::io::unix::Guard,
}

pub fn ready(socket: &OsStr) -> std::io::Result<()> {
	let address = address(socket)?;
	let datagram = UnixDatagram::unbound()?;
	datagram.send_to_addr(b"READY=1", &address)?;
	Ok(())
}

impl Listener {
	/// Bind a nonblocking socket at a new path in the temp directory, which is removed when the listener is dropped.
	pub fn new() -> std::io::Result<Self> {
		let name = format!("tangram-notify-{}", uuid::Uuid::now_v7().simple());
		let path = std::env::temp_dir().join(name);
		let (fd, guard) = crate::io::unix::bind_datagram(&path)?;
		let datagram = UnixDatagram::from(fd);
		Ok(Self { datagram, guard })
	}

	/// Get the path of the socket, which fits in a socket address.
	#[must_use]
	pub fn path(&self) -> &Path {
		self.guard.path()
	}

	/// Receive the available notifications without blocking, and return whether any of them was `READY=1`.
	pub fn try_receive_ready(&self) -> std::io::Result<bool> {
		let mut ready = false;
		let mut buffer = [0u8; 4096];
		loop {
			let size = match self.datagram.recv(&mut buffer) {
				Ok(size) => size,
				Err(error) if error.kind() == ErrorKind::WouldBlock => break,
				Err(error) if error.kind() == ErrorKind::Interrupted => continue,
				Err(error) => return Err(error),
			};
			ready |= buffer[..size]
				.split(|byte| *byte == b'\n')
				.any(|line| line == b"READY=1");
		}
		Ok(ready)
	}
}

impl AsFd for Listener {
	fn as_fd(&self) -> BorrowedFd<'_> {
		self.datagram.as_fd()
	}
}

impl AsRawFd for Listener {
	fn as_raw_fd(&self) -> RawFd {
		self.datagram.as_raw_fd()
	}
}

fn address(socket: &OsStr) -> std::io::Result<SocketAddr> {
	// Parse an abstract socket name, which is written with a leading `@`.
	#[cfg(target_os = "linux")]
	{
		use std::os::{linux::net::SocketAddrExt as _, unix::ffi::OsStrExt as _};
		if let Some(name) = socket.as_bytes().strip_prefix(b"@") {
			let address = SocketAddr::from_abstract_name(name)?;
			return Ok(address);
		}
	}

	let address = SocketAddr::from_pathname(socket)?;

	Ok(address)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn ready_path() {
		let directory = std::env::temp_dir().join(format!("tangram_notify_{}", std::process::id()));
		std::fs::create_dir_all(&directory).unwrap();
		let path = directory.join("socket");
		std::fs::remove_file(&path).ok();
		let receiver = UnixDatagram::bind(&path).unwrap();
		ready(path.as_os_str()).unwrap();
		let mut buffer = [0u8; 64];
		let size = receiver.recv(&mut buffer).unwrap();
		std::fs::remove_dir_all(&directory).unwrap();
		assert_eq!(&buffer[..size], b"READY=1");
	}

	#[cfg(target_os = "linux")]
	#[test]
	fn ready_abstract() {
		use std::os::linux::net::SocketAddrExt as _;
		let name = format!("tangram_notify_{}", std::process::id());
		let address = SocketAddr::from_abstract_name(name.as_bytes()).unwrap();
		let receiver = UnixDatagram::bind_addr(&address).unwrap();
		let socket = format!("@{name}");
		ready(OsStr::new(&socket)).unwrap();
		let mut buffer = [0u8; 64];
		let size = receiver.recv(&mut buffer).unwrap();
		assert_eq!(&buffer[..size], b"READY=1");
	}

	#[test]
	fn listener() {
		// Receive nothing before a notification is sent.
		let listener = Listener::new().unwrap();
		let path = listener.path().to_owned();
		assert!(!listener.try_receive_ready().unwrap());

		// Ignore a notification without `READY=1`, and receive one that has it among other assignments.
		let sender = UnixDatagram::unbound().unwrap();
		sender.send_to(b"STATUS=starting", &path).unwrap();
		assert!(!listener.try_receive_ready().unwrap());
		sender.send_to(b"STATUS=started\nREADY=1\n", &path).unwrap();
		assert!(listener.try_receive_ready().unwrap());

		// Receive a notification sent with the sending half.
		ready(path.as_os_str()).unwrap();
		assert!(listener.try_receive_ready().unwrap());

		// Remove the socket when the listener is dropped.
		drop(listener);
		assert!(!path.exists());
	}
}
