use std::{os::unix::ffi::OsStrExt as _, path::Path};

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

/// Resolve an executable using PATH entries relative to the command working directory.
pub fn which(path: &Path, executable: &std::path::Path, cwd: &Path) -> Option<std::path::PathBuf> {
	if executable.is_absolute() || executable.components().count() > 1 {
		return Some(cwd.join(executable));
	}
	for dir in std::env::split_paths(path) {
		let candidate = cwd.join(dir).join(executable);
		let Ok(path) = std::ffi::CString::new(candidate.as_os_str().as_bytes()) else {
			continue;
		};

		// SAFETY: The path is a valid, terminated string.
		let executable = unsafe { libc::access(path.as_ptr(), libc::X_OK) } == 0;
		if candidate.is_file() && executable {
			return Some(candidate);
		}
	}

	None
}

/// Create the command line arguments that configure the HTTP/2 settings of the sandbox server.
pub(crate) fn http2_args(http2: &tangram_http::http2::Config) -> Vec<String> {
	let mut args = vec![
		"--http2-connection-window-size".to_owned(),
		http2.connection_window_size.to_string(),
		"--http2-max-concurrent-reset-streams".to_owned(),
		http2.max_concurrent_reset_streams.to_string(),
		"--http2-stream-window-size".to_owned(),
		http2.stream_window_size.to_string(),
	];
	if let Some(max_concurrent_streams) = http2.max_concurrent_streams {
		args.push("--http2-max-concurrent-streams".to_owned());
		args.push(max_concurrent_streams.to_string());
	}
	args
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

#[cfg(test)]
mod tests {
	use {
		super::which,
		std::{os::unix::fs::PermissionsExt as _, path::Path},
	};

	#[test]
	fn executable_search_uses_the_command_directory() {
		// Create an executable beneath the command working directory.
		let temp = tangram_util::fs::Temp::new().unwrap();
		let bin = temp.path().join("bin");
		std::fs::create_dir_all(&bin).unwrap();
		let executable = bin.join("tool");
		std::fs::write(&executable, "").unwrap();
		std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();

		// Resolve relative and empty PATH entries without changing the server directory.
		assert_eq!(
			which(Path::new("bin"), Path::new("tool"), temp.path()),
			Some(executable.clone())
		);
		assert_eq!(
			which(Path::new(""), Path::new("tool"), &bin),
			Some(executable)
		);
	}

	#[test]
	fn executable_search_skips_nonexecutable_files() {
		// Place a nonexecutable file before the executable in PATH.
		let temp = tangram_util::fs::Temp::new().unwrap();
		for (directory, mode) in [("first", 0o644), ("second", 0o755)] {
			let directory = temp.path().join(directory);
			std::fs::create_dir_all(&directory).unwrap();
			let path = directory.join("tool");
			std::fs::write(&path, "").unwrap();
			std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
		}

		// Continue searching after the first file fails the execution check.
		assert_eq!(
			which(Path::new("first:second"), Path::new("tool"), temp.path()),
			Some(temp.path().join("second/tool"))
		);
	}
}
