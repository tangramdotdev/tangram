use {
	crate::Result,
	std::{
		ffi::{CString, OsStr},
		io,
		os::unix::{ffi::OsStrExt as _, fs::PermissionsExt as _},
		path::{Path, PathBuf},
	},
};

/// Create a uniquely named directory with the prefix `tangram_test_` in a parent directory.
pub fn create(parent: &Path) -> Result<PathBuf> {
	std::fs::create_dir_all(parent)?;
	let template = parent.join("tangram_test_XXXXXX");
	let template = CString::new(template.as_os_str().as_bytes())?;
	let mut template = template.into_bytes_with_nul();
	// SAFETY: The template is a valid, mutable, nul-terminated string.
	let result = unsafe { libc::mkdtemp(template.as_mut_ptr().cast()) };
	if result.is_null() {
		return Err(io::Error::last_os_error().into());
	}
	template.pop();
	let path = std::fs::canonicalize(OsStr::from_bytes(&template))?;
	Ok(path)
}

/// Remove a directory. If removal fails, grant the owner full permissions on every directory and try again.
pub fn remove(path: &Path) -> Result<()> {
	if std::fs::remove_dir_all(path).is_ok() || !path.exists() {
		return Ok(());
	}
	grant(path);
	std::fs::remove_dir_all(path).map_err(|error| {
		format!(
			"failed to remove the test temp directory {}: {error}",
			path.display()
		)
	})?;
	Ok(())
}

/// Visit every file under a directory without following symlinks.
pub fn walk(path: &Path, visit: &mut dyn FnMut(&Path)) {
	let Ok(read_dir) = std::fs::read_dir(path) else {
		return;
	};
	for entry in read_dir.flatten() {
		let Ok(file_type) = entry.file_type() else {
			continue;
		};
		let path = entry.path();
		if file_type.is_dir() {
			walk(&path, visit);
		} else {
			visit(&path);
		}
	}
}

/// Grant the owner full permissions on a directory and every directory under it.
fn grant(path: &Path) {
	if let Ok(metadata) = std::fs::symlink_metadata(path) {
		let mut permissions = metadata.permissions();
		permissions.set_mode(permissions.mode() | 0o700);
		std::fs::set_permissions(path, permissions).ok();
	}
	let Ok(read_dir) = std::fs::read_dir(path) else {
		return;
	};
	for entry in read_dir.flatten() {
		if entry.file_type().is_ok_and(|file_type| file_type.is_dir()) {
			grant(&entry.path());
		}
	}
}
