use {
	std::{
		borrow::Cow,
		os::fd::OwnedFd,
		path::{Path, PathBuf},
	},
	uuid::Uuid,
};

pub struct Guard {
	entry: PathBuf,
	indirect: bool,
	path: PathBuf,
	directory: Option<PathBuf>,
}

impl Guard {
	#[must_use]
	pub fn entry(&self) -> &Path {
		&self.entry
	}

	#[must_use]
	pub fn indirect(&self) -> bool {
		self.indirect
	}

	#[must_use]
	pub fn path(&self) -> &Path {
		&self.path
	}
}

impl Drop for Guard {
	fn drop(&mut self) {
		std::fs::remove_file(&self.entry).ok();
		if self.path != self.entry {
			std::fs::remove_file(&self.path).ok();
		}
		if let Some(directory) = &self.directory {
			std::fs::remove_dir(directory).ok();
		}
	}
}

pub fn bind_datagram(path: &Path) -> std::io::Result<(OwnedFd, Guard)> {
	if rustix::net::SocketAddrUnix::new(path).is_ok() {
		let socket = datagram_socket()?;
		let address = rustix::net::SocketAddrUnix::new(path)
			.map_err(|_| invalid_input("the socket path is too long"))?;
		rustix::net::bind(&socket, &address).map_err(std::io::Error::from)?;
		let guard = Guard {
			entry: path.to_owned(),
			path: path.to_owned(),
			indirect: false,
			directory: None,
		};
		Ok((socket, guard))
	} else {
		bind_datagram_indirect(path)
	}
}

pub fn bind_listener(path: &Path) -> std::io::Result<(tokio::net::UnixListener, Guard)> {
	if rustix::net::SocketAddrUnix::new(path).is_ok() {
		let listener = std::os::unix::net::UnixListener::bind(path)?;
		listener.set_nonblocking(true)?;
		let listener = tokio::net::UnixListener::from_std(listener)?;
		let guard = Guard {
			entry: path.to_owned(),
			path: path.to_owned(),
			indirect: false,
			directory: None,
		};
		Ok((listener, guard))
	} else {
		bind_listener_indirect(path)
	}
}

pub fn resolve(path: &Path) -> std::io::Result<Cow<'_, Path>> {
	match std::fs::read_link(path) {
		Ok(path) => Ok(Cow::Owned(path)),
		Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => Ok(Cow::Borrowed(path)),
		Err(error) => Err(error),
	}
}

fn bind_datagram_indirect(entry: &Path) -> std::io::Result<(OwnedFd, Guard)> {
	let mut last_error = None;
	for base in temp_paths() {
		for _ in 0..16 {
			let (directory, path) = indirect_path(&base, entry);
			match bind_datagram_at(entry, &directory, &path) {
				Ok(output) => return Ok(output),
				Err(error) => last_error = Some(error),
			}
		}
	}
	Err(last_error.unwrap_or_else(|| invalid_input("failed to bind the socket")))
}

fn bind_listener_indirect(entry: &Path) -> std::io::Result<(tokio::net::UnixListener, Guard)> {
	let mut last_error = None;
	for base in temp_paths() {
		for _ in 0..16 {
			let (directory, path) = indirect_path(&base, entry);
			match bind_listener_at(entry, &directory, &path) {
				Ok(output) => return Ok(output),
				Err(error) => last_error = Some(error),
			}
		}
	}
	Err(last_error.unwrap_or_else(|| invalid_input("failed to bind the socket")))
}

fn bind_datagram_at(
	entry: &Path,
	directory: &Path,
	path: &Path,
) -> std::io::Result<(OwnedFd, Guard)> {
	let socket = datagram_socket()?;
	let address = rustix::net::SocketAddrUnix::new(path)
		.map_err(|_| invalid_input("the socket path is too long"))?;
	std::fs::create_dir(directory)?;
	match rustix::net::bind(&socket, &address) {
		Ok(()) => {},
		Err(error) => {
			std::fs::remove_dir(directory).ok();
			return Err(error.into());
		},
	}
	match std::os::unix::fs::symlink(path, entry) {
		Ok(()) => {
			let guard = Guard {
				entry: entry.to_owned(),
				path: path.to_owned(),
				indirect: true,
				directory: Some(directory.to_owned()),
			};
			Ok((socket, guard))
		},
		Err(error) => {
			std::fs::remove_file(path).ok();
			std::fs::remove_dir(directory).ok();
			Err(error)
		},
	}
}

fn bind_listener_at(
	entry: &Path,
	directory: &Path,
	path: &Path,
) -> std::io::Result<(tokio::net::UnixListener, Guard)> {
	if rustix::net::SocketAddrUnix::new(path).is_err() {
		return Err(invalid_input("the socket path is too long"));
	}
	std::fs::create_dir(directory)?;
	let listener = match std::os::unix::net::UnixListener::bind(path) {
		Ok(listener) => listener,
		Err(error) => {
			std::fs::remove_dir(directory).ok();
			return Err(error);
		},
	};
	match std::os::unix::fs::symlink(path, entry) {
		Ok(()) => {},
		Err(error) => {
			std::fs::remove_file(path).ok();
			std::fs::remove_dir(directory).ok();
			return Err(error);
		},
	}
	listener.set_nonblocking(true)?;
	let listener = tokio::net::UnixListener::from_std(listener)?;
	let guard = Guard {
		entry: entry.to_owned(),
		path: path.to_owned(),
		indirect: true,
		directory: Some(directory.to_owned()),
	};
	Ok((listener, guard))
}

fn datagram_socket() -> std::io::Result<OwnedFd> {
	let socket = rustix::net::socket(
		rustix::net::AddressFamily::UNIX,
		rustix::net::SocketType::DGRAM,
		None,
	)
	.map_err(std::io::Error::from)?;
	set_nonblocking(&socket).map_err(std::io::Error::from)?;
	Ok(socket)
}

fn indirect_path(base: &Path, entry: &Path) -> (PathBuf, PathBuf) {
	let directory = base.join(format!("tangram-{}", Uuid::now_v7()));
	let name = entry
		.file_name()
		.unwrap_or_else(|| std::ffi::OsStr::new("socket"));
	let path = directory.join(name);
	(directory, path)
}

fn invalid_input(message: &'static str) -> std::io::Error {
	std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

fn set_nonblocking(socket: &OwnedFd) -> rustix::io::Result<()> {
	use std::os::fd::AsFd as _;
	let flags = rustix::fs::fcntl_getfl(socket.as_fd())?;
	rustix::fs::fcntl_setfl(socket.as_fd(), flags | rustix::fs::OFlags::NONBLOCK)
}

fn temp_paths() -> Vec<PathBuf> {
	let mut paths = vec![std::env::temp_dir()];
	let tmp = PathBuf::from("/tmp");
	if !paths.iter().any(|path| path == &tmp) {
		paths.push(tmp);
	}
	paths
}
