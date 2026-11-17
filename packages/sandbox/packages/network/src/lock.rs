use {
	std::{
		os::{fd::AsRawFd as _, unix::fs::OpenOptionsExt as _},
		path::Path,
	},
	tangram_client::prelude::*,
};

pub const DIRECTORY: &str = "/run/tangram/network";

/// Lock the file at a path, waiting for another holder to release it.
pub fn lock(path: &Path) -> tg::Result<std::fs::File> {
	let file = open(path)?;

	// SAFETY: flock has no memory safety preconditions.
	let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		let path = path.display();
		return Err(tg::error!(!error, %path, "failed to lock the file"));
	}

	Ok(file)
}

/// Lock the file at a path, returning `None` if another holder has it locked.
pub fn try_lock(path: &Path) -> tg::Result<Option<std::fs::File>> {
	let file = open(path)?;

	// SAFETY: flock has no memory safety preconditions.
	let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
			return Ok(None);
		}
		let path = path.display();
		return Err(tg::error!(!error, %path, "failed to lock the file"));
	}

	Ok(Some(file))
}

fn open(path: &Path) -> tg::Result<std::fs::File> {
	if let Some(parent) = path.parent() {
		std::fs::create_dir_all(parent).map_err(|error| {
			let path = parent.display();
			tg::error!(!error, %path, "failed to create the lock directory")
		})?;
	}
	std::fs::OpenOptions::new()
		.create(true)
		.mode(0o600)
		.truncate(false)
		.write(true)
		.open(path)
		.map_err(|error| {
			let path = path.display();
			tg::error!(!error, %path, "failed to open the lock file")
		})
}
