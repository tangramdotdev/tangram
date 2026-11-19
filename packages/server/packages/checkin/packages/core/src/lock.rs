use {
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

pub fn try_read_lock(path: &Path) -> tg::Result<Option<tg::graph::data::Leaf>> {
	// Try to read the lock contents.
	let contents = if path.is_dir() {
		try_read_lockfile(&lockfile_path(path))?
	} else if std::fs::metadata(path).is_ok_and(|metadata| metadata.is_file()) {
		let contents = try_read_lockfile(&lockfile_path(path))?;
		if contents.is_some() {
			contents
		} else {
			// Fall back to xattr.
			tg::file::xattrs::read_lock(path).ok().flatten()
		}
	} else {
		None
	};

	// Return early if no contents found.
	let Some(contents) = contents else {
		return Ok(None);
	};

	// Deserialize the lock.
	let lock = serde_json::from_slice::<tg::graph::data::Leaf>(&contents).map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to deserialize the lock"),
	)?;

	Ok(Some(lock))
}

/// Get the path of the lockfile of a checkin root, which is `tangram.lock` in a directory and the sibling with the `lock` extension of a file.
#[must_use]
pub fn lockfile_path(path: &Path) -> PathBuf {
	if path.is_dir() {
		path.join(tg::module::LOCKFILE_FILE_NAME)
	} else {
		path.with_extension("lock")
	}
}

fn try_read_lockfile(path: &Path) -> tg::Result<Option<Vec<u8>>> {
	match std::fs::read(path) {
		Ok(contents) => Ok(Some(contents)),
		Err(error)
			if matches!(
				error.kind(),
				std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
			) =>
		{
			Ok(None)
		},
		Err(error) => {
			Err(tg::error!(!error, path = %path.display(), "failed to read the lockfile"))
		},
	}
}
