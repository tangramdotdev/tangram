use {
	std::{
		ffi::{CString, OsStr},
		os::unix::{ffi::OsStrExt as _, fs::PermissionsExt as _},
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

/// A temporary directory, which is removed when it is dropped unless `TANGRAM_TEST_PRESERVE_TEMPS` is set.
pub struct Temp {
	path: PathBuf,
}

impl Temp {
	/// Create a uniquely named directory with the prefix `tangram_test_` in the temporary directory.
	pub fn new() -> tg::Result<Self> {
		let parent = std::env::temp_dir();
		std::fs::create_dir_all(&parent).map_err(
			|error| tg::error!(!error, path = %parent.display(), "failed to create the temporary directory"),
		)?;
		let template = parent.join("tangram_test_XXXXXX");
		let template = CString::new(template.as_os_str().as_bytes())
			.map_err(|error| tg::error!(!error, "invalid temporary directory path"))?;
		let mut template = template.into_bytes_with_nul();
		// SAFETY: The template is a valid, mutable, nul-terminated string.
		let result = unsafe { libc::mkdtemp(template.as_mut_ptr().cast()) };
		if result.is_null() {
			let error = std::io::Error::last_os_error();
			return Err(tg::error!(
				!error,
				"failed to create the temporary directory"
			));
		}
		template.pop();
		let path = std::fs::canonicalize(OsStr::from_bytes(&template)).map_err(|error| {
			tg::error!(!error, "failed to canonicalize the temporary directory")
		})?;
		let temp = Self { path };

		Ok(temp)
	}

	#[must_use]
	pub fn path(&self) -> &Path {
		&self.path
	}
}

impl Drop for Temp {
	fn drop(&mut self) {
		if std::env::var_os("TANGRAM_TEST_PRESERVE_TEMPS").is_some() {
			eprintln!("preserved the temporary directory {}", self.path.display());
			return;
		}
		remove(&self.path);
	}
}

/// Remove a directory. If removal fails, grant the owner full permissions on every directory and try again.
fn remove(path: &Path) {
	if std::fs::remove_dir_all(path).is_ok() || !path.exists() {
		return;
	}
	grant(path);
	std::fs::remove_dir_all(path).ok();
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
