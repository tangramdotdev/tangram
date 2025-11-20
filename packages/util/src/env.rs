use std::{ffi::OsStr, os::unix::ffi::OsStrExt as _, path::PathBuf};

pub fn current_exe() -> std::io::Result<PathBuf> {
	let path = std::env::current_exe()?;
	let path = if cfg!(target_os = "linux")
		&& path.as_os_str().as_encoded_bytes().ends_with(b" (deleted)")
	{
		let path = path.into_os_string();
		let path = path.as_encoded_bytes();
		let path = path.strip_suffix(b" (deleted)").unwrap();
		let path = OsStr::from_bytes(path);
		PathBuf::from(path)
	} else {
		path
	};
	Ok(path)
}
