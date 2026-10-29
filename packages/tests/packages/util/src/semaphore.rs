use std::ffi::CString;

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
