use std::{ffi::CString, io::Write as _};

/// Record the semaphore names in the manifest named by `TANGRAM_TEST_SEMAPHORES`, so that they are unlinked after the test even if it does not drop its servers.
pub fn record(names: &[String]) {
	let Some(path) = std::env::var_os("TANGRAM_TEST_SEMAPHORES") else {
		return;
	};
	let Ok(mut file) = std::fs::File::options()
		.append(true)
		.create(true)
		.open(path)
	else {
		return;
	};
	for name in names {
		writeln!(file, "{name}").ok();
	}
}

/// Unlink POSIX semaphores, ignoring semaphores that do not exist.
pub fn unlink(names: &[String]) {
	for name in names {
		let Ok(name) = CString::new(name.as_str()) else {
			continue;
		};
		// SAFETY: The name is a valid C string.
		unsafe { libc::sem_unlink(name.as_ptr()) };
	}
}
