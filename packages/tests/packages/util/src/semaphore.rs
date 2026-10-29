use std::{ffi::CString, io::Write as _};

/// Semaphore names that are unlinked when the guard is dropped.
pub struct Guard {
	names: Vec<String>,
}

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

impl Guard {
	/// Record the semaphore names, and unlink them when the guard is dropped.
	pub fn new(names: Vec<String>) -> Self {
		record(&names);
		Self { names }
	}

	/// Keep the semaphores after the guard is dropped.
	pub fn forget(mut self) {
		self.names.clear();
	}
}

impl Drop for Guard {
	fn drop(&mut self) {
		unlink(&self.names);
	}
}
