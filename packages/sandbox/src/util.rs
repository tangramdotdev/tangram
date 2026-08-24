use std::path::Path;

#[cfg(target_os = "linux")]
use tangram_client::prelude::*;

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

#[cfg(target_os = "linux")]
pub(crate) fn set_dumpable(value: bool) -> tg::Result<()> {
	let value: libc::c_ulong = if value { 1 } else { 0 };
	// SAFETY: PR_SET_DUMPABLE only reads the scalar second argument.
	let result = unsafe { libc::prctl(libc::PR_SET_DUMPABLE, value, 0, 0, 0) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to set process dumpability"));
	}
	Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) fn set_parent_death_signal(signal: libc::c_int) -> tg::Result<()> {
	let parent = unsafe { libc::getppid() };
	// SAFETY: PR_SET_PDEATHSIG only reads the scalar second argument.
	let result = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, signal, 0, 0, 0) };
	if result != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to set the parent death signal"));
	}
	if unsafe { libc::getppid() } != parent {
		return Err(tg::error!(
			"the parent exited while setting the parent death signal"
		));
	}
	Ok(())
}
