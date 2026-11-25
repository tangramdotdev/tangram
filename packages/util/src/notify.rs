use std::{
	ffi::OsStr,
	os::unix::net::{SocketAddr, UnixDatagram},
};

pub fn ready(socket: &OsStr) -> std::io::Result<()> {
	let address = address(socket)?;
	let datagram = UnixDatagram::unbound()?;
	datagram.send_to_addr(b"READY=1", &address)?;
	Ok(())
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
}
