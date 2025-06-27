use crate::{
	Command,
	common::{CStringVec, abort_errno, cstring, envstring},
};
use indoc::writedoc;
use num::ToPrimitive as _;
use std::{
	ffi::{CStr, CString},
	fmt::Write,
	os::unix::ffi::OsStrExt as _,
	path::Path,
};

struct Context {
	argv: CStringVec,
	cwd: CString,
	envp: CStringVec,
	executable: CString,
	profile: CString,
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn spawn(command: Command) -> std::io::Result<libc::c_int> {
	// Create argv, cwd, and envp strings.
	let argv = std::iter::once(cstring(&command.executable))
		.chain(command.trailing.iter().map(cstring))
		.collect::<CStringVec>();
	let cwd = command
		.cwd
		.clone()
		.map_or_else(std::env::current_dir, Ok::<_, std::io::Error>)
		.inspect_err(|_| eprintln!("failed to get cwd"))
		.map(cstring)?;
	let envp = command
		.env
		.iter()
		.map(|(k, v)| envstring(k, v))
		.collect::<CStringVec>();
	let executable = cstring(&command.executable);

	if command.chroot.is_some() {
		return Err(std::io::Error::other("chroot is not allowed on darwin"));
	}

	if command.user.is_some() {
		return Err(std::io::Error::other("uid/gid is not allowed on darwin"));
	}

	// Create the sandbox profile.
	let profile = create_sandbox_profile(&command)?;

	// Create the context.
	let context = Context {
		argv,
		cwd,
		envp,
		executable,
		profile,
	};

	// Fork.
	let pid = unsafe { libc::fork() };
	if pid < 0 {
		return Err(std::io::Error::last_os_error());
	}
	if pid == 0 {
		// Initialize the sandbox.
		unsafe {
			let error = std::ptr::null_mut::<*const libc::c_char>();
			let ret = sandbox_init(context.profile.as_ptr(), 0, error);

			// Handle an error from `sandbox_init`.
			if ret != 0 {
				let error = *error;
				let message = CStr::from_ptr(error);
				sandbox_free_error(error);
				abort_errno!("failed to setup the sandbox: {}", message.to_string_lossy());
			}
		}

		// Change directories if necessary.
		if unsafe { libc::chdir(context.cwd.as_ptr()) } != 0 {
			abort_errno!("failed to change working directory");
		}

		// Exec.
		unsafe {
			libc::execve(
				context.executable.as_ptr(),
				context.argv.as_ptr(),
				context.envp.as_ptr(),
			);
			abort_errno!("failed to exec");
		}
	}

	// Wait for the child process to exit.
	let mut status = 0;
	unsafe {
		libc::waitpid(pid, std::ptr::addr_of_mut!(status), 0);
	}

	// Reap its children.
	kill_process_tree(pid);

	if libc::WIFEXITED(status) {
		let status = libc::WEXITSTATUS(status);
		return Ok(status);
	}
	if libc::WIFSIGNALED(status) {
		let signal = libc::WTERMSIG(status);
		return Ok(signal + 128);
	}

	eprintln!("unknown process termination");
	Ok(1)
}

fn create_sandbox_profile(command: &Command) -> std::io::Result<CString> {
	let mut profile = String::new();
	writedoc!(
		profile,
		"
			(version 1)
		"
	)
	.unwrap();

	let root_mount = command.mounts.iter().any(|mount| {
		mount.source == mount.target
			&& mount
				.target
				.as_ref()
				.is_some_and(|path| path == Path::new("/"))
	});

	if root_mount {
		writedoc!(
			profile,
			"
				;; Allow everything by default.
				(allow default)
			"
		)
		.unwrap();
	} else {
		writedoc!(
			profile,
			r#"
				;; See /System/Library/Sandbox/Profiles/system.sb for more info.

				;; Deny everything by default.
				(deny default)

				;; Allow most system operations.
				(allow syscall*)
				(allow system-socket)
				(allow mach*)
				(allow ipc*)
				(allow sysctl*)

				;; Allow most process operations, except for `process-exec`. `process-exec` will let you execute binaries without having been granted the corresponding `file-read*` permission.
				(allow process-fork process-info*)

				;; Allow limited exploration of the root.
				(allow file-read* file-test-existence
					(literal "/"))

				(allow file-read* file-test-existence
					(subpath "/Library/Apple/System")
					(subpath "/Library/Filesystems/NetFSPlugins")
					(subpath "/Library/Preferences/Logging")
					(subpath "/System")
					(subpath "/private/var/db/dyld")
					(subpath "/private/var/db/timezone")
					(subpath "/usr/lib")
					(subpath "/usr/share"))

				(allow file-read-metadata
					(literal "/Library")
					(literal "/Users")
					(literal "/Volumes")
					(literal "/tmp")
					(literal "/var")
					(literal "/etc"))

				;; Map system frameworks + dylibs.
				(allow file-map-executable
					(subpath "/Library/Apple/System/Library/Frameworks")
					(subpath "/Library/Apple/System/Library/PrivateFrameworks")
					(subpath "/System/Library/Frameworks")
					(subpath "/System/Library/PrivateFrameworks")
					(subpath "/System/iOSSupport/System/Library/Frameworks")
					(subpath "/System/iOSSupport/System/Library/PrivateFrameworks")
					(subpath "/usr/lib"))

				;; Allow writing to common devices.
				(allow file-read* file-write-data file-ioctl
					(literal "/dev/null")
					(literal "/dev/zero")
					(literal "/dev/dtracehelper"))

				;; Allow reading and writing temporary files.
				(allow file-write* file-read*
					(subpath "/tmp")
					(subpath "/private/tmp")
					(subpath "/private/var")
					(subpath "/var"))

				;; Allow reading some system devices and files.
				(allow file-read*
					(literal "/dev/autofs_nowait")
					(literal "/dev/random")
					(literal "/dev/urandom")
					(literal "/private/etc/localtime")
					(literal "/private/etc/protocols")
					(literal "/private/etc/services")
					(subpath "/private/etc/ssl"))

				(allow file-read* file-test-existence file-write-data file-ioctl
					(literal "/dev/dtracehelper"))

				;; Allow executing /usr/bin/env and /bin/sh.
				(allow file-read* process-exec
					(literal "/usr/bin/env")
					(literal "/bin/sh")
					(literal "/bin/bash"))

				;; Support Rosetta.
				(allow file-read* file-test-existence
					(literal "/Library/Apple/usr/libexec/oah/libRosettaRuntime"))

				;; Allow accessing the dyld shared cache.
				(allow file-read* process-exec
					(literal "/System/Volumes/Preboot/Cryptexes/OS/System/Library/dyld")
					(subpath "/System/Volumes/Preboot/Cryptexes/OS/System/Library/dyld"))

				;; Allow querying the macOS system version metadata.
				(allow file-read* file-test-existence
					(literal "/System/Library/CoreServices/SystemVersion.plist"))

				;; Allow bash to create and use file descriptors for pipes.
				(allow file-read* file-write* file-ioctl process-exec
					(literal "/dev/fd")
					(subpath "/dev/fd"))
			"#
		).unwrap();
	}

	// Write the network profile.
	if command.network {
		writedoc!(
			profile,
			r#"
				;; Allow network access.
				(allow network*)

				;; Allow reading network preference files.
				(allow file-read*
					(literal "/Library/Preferences/com.apple.networkd.plist")
					(literal "/private/var/db/com.apple.networkextension.tracker-info")
					(literal "/private/var/db/nsurlstoraged/dafsaData.bin")
				)
				(allow user-preference-read (preference-domain "com.apple.CFNetwork"))
			"#
		)
		.unwrap();
	} else {
		writedoc!(
			profile,
			r#"
				;; Disable global network access.
				(deny network*)

				;; Allow network access to localhost and Unix sockets.
				(allow network* (remote ip "localhost:*"))
				(allow network* (remote unix-socket))
			"#
		)
		.unwrap();
	}

	for mount in &command.mounts {
		if !root_mount {
			if mount.source != mount.target {
				return Err(std::io::Error::other(
					"the source and target paths must be the same",
				));
			}
			let path = mount.source.as_ref().unwrap();
			if (mount.flags & libc::MNT_RDONLY.to_u64().unwrap()) != 0 {
				writedoc!(
					profile,
					r"
						(allow process-exec* (subpath {0}))
						(allow file-read* (subpath {0}))
					",
					escape(path.as_os_str().as_bytes()),
				)
				.unwrap();
				if path != Path::new("/") {
					writedoc!(
						profile,
						r"
							(allow file-read* (path-ancestors {0}))
						",
						escape(path.as_os_str().as_bytes()),
					)
					.unwrap();
				}
			} else {
				writedoc!(
					profile,
					r"
						(allow process-exec* (subpath {0}))
						(allow file-read* (subpath {0}))
						(allow file-write* (subpath {0}))
					",
					escape(path.as_os_str().as_bytes()),
				)
				.unwrap();
				if path != Path::new("/") {
					writedoc!(
						profile,
						r"
							(allow file-read* (path-ancestors {0}))
						",
						escape(path.as_os_str().as_bytes()),
					)
					.unwrap();
				}
			}
		}
	}

	Ok(CString::new(profile).unwrap())
}

fn kill_process_tree(pid: i32) {
	let mut pids = vec![pid];
	let mut i = 0;
	while i < pids.len() {
		let ppid = pids[i];
		let n = unsafe { libc::proc_listchildpids(ppid, std::ptr::null_mut(), 0) };
		if n < 0 {
			return;
		}
		pids.resize(i + n.to_usize().unwrap() + 1, 0);
		let n = unsafe { libc::proc_listchildpids(ppid, pids[(i + 1)..].as_mut_ptr().cast(), n) };
		if n < 0 {
			return;
		}
		pids.truncate(i + n.to_usize().unwrap() + 1);
		i += 1;
	}
	for pid in pids.iter().rev() {
		unsafe { libc::kill(*pid, libc::SIGKILL) };
		let mut status = 0;
		unsafe { libc::waitpid(*pid, std::ptr::addr_of_mut!(status), 0) };
	}
}

unsafe extern "C" {
	fn sandbox_init(
		profile: *const libc::c_char,
		flags: u64,
		errorbuf: *mut *const libc::c_char,
	) -> libc::c_int;
	fn sandbox_free_error(errorbuf: *const libc::c_char) -> libc::c_void;
}

/// Escape a string using the string literal syntax rules for `TinyScheme`. See <https://github.com/dchest/tinyscheme/blob/master/Manual.txt#L130>.
fn escape(bytes: impl AsRef<[u8]>) -> String {
	let bytes = bytes.as_ref();
	let mut output = String::new();
	output.push('"');
	for byte in bytes {
		let byte = *byte;
		match byte {
			b'"' => {
				output.push('\\');
				output.push('"');
			},
			b'\\' => {
				output.push('\\');
				output.push('\\');
			},
			b'\t' => {
				output.push('\\');
				output.push('t');
			},
			b'\n' => {
				output.push('\\');
				output.push('n');
			},
			b'\r' => {
				output.push('\\');
				output.push('r');
			},
			byte if char::from(byte).is_ascii_alphanumeric()
				|| char::from(byte).is_ascii_punctuation()
				|| byte == b' ' =>
			{
				output.push(byte.into());
			},
			byte => {
				write!(output, "\\x{byte:02X}").unwrap();
			},
		}
	}
	output.push('"');
	output
}
