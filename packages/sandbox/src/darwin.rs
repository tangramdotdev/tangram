use {
	crate::{
		Config,
		common::{SpawnContext, start_session, which},
	},
	indoc::writedoc,
	num::ToPrimitive as _,
	std::{
		ffi::{CStr, CString},
		fmt::Write,
		os::unix::ffi::OsStrExt as _,
		path::Path,
	},
	tangram_client::prelude::*,
};

pub fn enter(config: &Config) -> std::io::Result<()> {
	std::fs::create_dir_all(&config.output_path).ok();
	let profile = create_sandbox_profile(&config);
	unsafe {
		let mut error = std::ptr::null::<std::ffi::c_char>();
		let ret = sandbox_init(profile.as_ptr(), 0, std::ptr::addr_of_mut!(error));
		if ret != 0 {
			let error = error;
			let message = CStr::from_ptr(error);
			let result = std::io::Error::other(format!(
				"failed to enter sandbox: {}",
				message.to_string_lossy()
			));
			sandbox_free_error(error);
			return Err(result);
		}
	}
	Ok(())
}

pub fn spawn(context: SpawnContext) -> tg::Result<tokio::process::Child> {
	let executable = context
		.command
		.env
		.iter()
		.find_map(|(key, value)| {
			(key == "PATH")
				.then_some(value)
				.and_then(|path| which(path.as_ref(), &context.command.executable))
		})
		.unwrap_or_else(|| context.command.executable.clone());
	let stdin = context.stdin.is_none();
	let stdout = context.stdout.is_none();
	let stderr = context.stderr.is_none();
	let mut command = tokio::process::Command::new(&executable);
	command
		.env_clear()
		.args(context.command.args)
		.envs(context.command.env)
		.current_dir(context.command.cwd);
	if let Some(fd) = context.stdin {
		command.stdin(fd);
	}
	if let Some(fd) = context.stdout {
		command.stdout(fd);
	}
	if let Some(fd) = context.stderr {
		command.stderr(fd);
	}
	unsafe {
		command
			.pre_exec(move || {
				if let Some(pty) = &context.pty {
					start_session(pty, stdin, stdout, stderr);
				}
				Ok(())
			})
			.spawn()
			.map_err(|source| tg::error!(!source, executable = %executable.display(), "failed to spawn the child process"))
	}
}

fn create_sandbox_profile(config: &Config) -> CString {
	let mut profile = String::new();
	writedoc!(
		profile,
		"
			(version 1)
		"
	)
	.unwrap();

	let root_mount = config.mounts.iter().any(|mount| {
		mount
			.as_ref()
			.right()
			.is_some_and(|m| m.source == m.target && m.target == Path::new("/"))
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
					(subpath "/usr/lib")
					(subpath "/opt/homebew"))

				;; Allow writing to common devices.
				(allow file-read* file-write-data file-ioctl
					(literal "/dev/null")
					(literal "/dev/zero")
					(literal "/dev/dtracehelper"))

				;; Allow reading and writing temporary files.
				(allow file-write* file-read* process-exec*
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
				
				;; Allow opening pseudo-terminals.
				(allow file-read* file-write* file-ioctl
					(literal "/dev/ptmx")
					(regex #"^/dev/ttys[0-9]+$"))
			"#
		).unwrap();
	}

	writedoc!(
		profile,
		r#"
		;; Allow exec'ing the tg binary itself.
		(allow file-read* process-exec
			(literal "{}"))

		;; Allow reading/writing to the socket path.
		(allow file-read* file-write*
			(literal "{}")
			(literal "{}"))
		(allow file-read* file-write* process-exec
			(subpath "{}"))
	"#,
		config.tangram_path.display(),
		config.socket_path.display(),
		config.listen_path.display(),
		config.scratch_path.parent().unwrap().display(),
	)
	.unwrap();

	// Write the network profile.
	if config.network {
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

	for mount in &config.mounts {
		if !root_mount {
			let path = match mount {
				tg::Either::Left(mount) => config.artifacts_path.join(mount.source.to_string()),
				tg::Either::Right(mount) => mount.source.clone(),
			};
			let read_only = mount.as_ref().right().is_none_or(|mount| mount.readonly);
			if read_only {
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

	CString::new(profile).unwrap()
}

#[allow(dead_code)]
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
