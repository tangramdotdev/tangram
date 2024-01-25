#[cfg(any(target_os = "linux", target_os = "macos"))]
#[must_use]
pub fn errno() -> i32 {
	std::io::Error::last_os_error().raw_os_error().unwrap()
}
