use std::path::Path;

#[macro_export]
macro_rules! abort {
	($($t:tt)*) => {{
		eprintln!("an error occurred in the child process");
		eprintln!("{}", format_args!($($t)*));
		std::process::exit(105)
	}};
}

#[expect(unused_imports)]
pub use abort;

#[macro_export]
macro_rules! abort_errno {
	($($t:tt)*) => {{
		eprintln!("an error occurred in the child process");
		eprintln!("{}", format_args!($($t)*));
		eprintln!("{}", std::io::Error::last_os_error());
		std::process::exit(std::io::Error::last_os_error().raw_os_error().unwrap_or(1));
	}};
}

#[expect(unused_imports)]
pub use abort_errno;

/// Resolve a non-absolute executable path by searching the given PATH value.
pub fn which(path: &Path, executable: &std::path::Path) -> Option<std::path::PathBuf> {
	if executable.is_absolute() {
		return Some(executable.to_owned());
	}
	for dir in std::env::split_paths(path) {
		let candidate = dir.join(executable);
		if candidate.is_file() {
			return Some(candidate);
		}
	}
	None
}

pub fn user_is_root() -> bool {
	unsafe { libc::geteuid() == 0 }
}
