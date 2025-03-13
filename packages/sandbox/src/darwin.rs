use crate::{
	Child, Command, ExitStatus, Stderr, Stdin, Stdout,
	common::{CStringVec, GuestIo, abort_errno, cstring, envstring, redirect_stdio, stdio_pair},
	pty::Pty,
};
use indoc::writedoc;
use num::ToPrimitive;
use std::{
	ffi::{CStr, CString},
	fmt::Write,
	os::unix::ffi::OsStrExt,
};
use tangram_either::Either;

struct Context {
	argv: CStringVec,
	cwd: CString,
	envp: CStringVec,
	executable: CString,
	profile: CString,
	stdin: GuestIo,
	stdout: GuestIo,
	stderr: GuestIo,
}

pub(crate) async fn spawn(command: &Command) -> std::io::Result<Child> {
	// Create argv, cwd, and envp strings.
	let argv = std::iter::once(cstring(&command.executable))
		.chain(command.args.iter().map(cstring))
		.collect::<CStringVec>();
	let cwd = cstring(&command.cwd);
	let envp = command
		.envs
		.iter()
		.map(|(k, v)| envstring(k, v))
		.collect::<CStringVec>();
	let executable = cstring(&command.executable);

	// Check that the caller didn't request something that requires root or disabling SIP...
	if command.chroot.is_some() {
		return Err(std::io::Error::other(
			"chroot unsupported on darwin targets",
		));
	}
	if !command.mounts.is_empty() {
		return Err(std::io::Error::other(
			"mount is unsupported on darwin targets",
		));
	}

	// Create stdio.
	let mut pty = None;
	let (parent_stdin, child_stdin) = stdio_pair(command.stdin, &mut pty).await?;
	let (parent_stdout, child_stdout) = stdio_pair(command.stdout, &mut pty).await?;
	let (parent_stderr, child_stderr) = stdio_pair(command.stderr, &mut pty).await?;

	// Create the sandbox profile.
	let profile = create_sandbox_profile(command);

	// Create the context.
	let context = Context {
		argv,
		cwd,
		envp,
		executable,
		profile,
		stdin: child_stdin,
		stdout: child_stdout,
		stderr: child_stderr,
	};

	// Fork.
	let pid = unsafe { libc::fork() };
	if pid < 0 {
		return Err(std::io::Error::last_os_error());
	}
	if pid == 0 {
		guest_process(context);
	}

	// Create stdio
	// Split stdio.
	let pty = pty.map(Pty::into_writer);
	let stdout = match parent_stdout {
		Either::Left(_) => Some(Either::Left(pty.as_ref().unwrap().get_reader()?)),
		Either::Right(Some(io)) => Some(Either::Right(io)),
		Either::Right(None) => None,
	};
	let stderr = match parent_stderr {
		Either::Left(_) => Some(Either::Left(pty.as_ref().unwrap().get_reader()?)),
		Either::Right(Some(io)) => Some(Either::Right(io)),
		Either::Right(None) => None,
	};
	let stdin = match parent_stdin {
		Either::Left(_) => Some(Either::Left(pty.unwrap())),
		Either::Right(Some(io)) => Some(Either::Right(io)),
		Either::Right(None) => None,
	};

	// Create the child.
	let child = Child {
		pid,
		stdin: stdin.map(|inner| Stdin { inner }),
		stdout: stdout.map(|inner| Stdout { inner }),
		stderr: stderr.map(|inner| Stderr { inner }),
	};

	Ok(child)
}

pub(crate) async fn wait(child: &mut Child) -> std::io::Result<ExitStatus> {
	let pid = child.pid;

	// Defer closing the process.
	scopeguard::defer! {
		// Kill the root process.
		let ret = unsafe { libc::kill(pid, libc::SIGKILL) };
		if ret != 0 {
			return;
		}

		// Wait for the root process to exit.
		tokio::task::spawn_blocking(move || {
			// Wait for exit.
			let mut status = 0;
			unsafe {
				libc::waitpid(
					pid,
					std::ptr::addr_of_mut!(status),
					libc::WEXITED,
				);
			}

			// Reap all its children.
			kill_process_tree(pid);
		});
	};

	// Spawn a blocking task to wait for the output.
	tokio::task::spawn_blocking(move || unsafe {
		let mut status = 0;
		if libc::waitpid(pid, std::ptr::addr_of_mut!(status), 0) == -1 {
			return Err(std::io::Error::last_os_error());
		};
		if libc::WIFEXITED(status) {
			let code = libc::WEXITSTATUS(status);
			Ok(ExitStatus::Code(code))
		} else if libc::WIFSIGNALED(status) {
			let signal = libc::WTERMSIG(status);
			Ok(ExitStatus::Signal(signal))
		} else {
			unreachable!();
		}
	})
	.await
	.unwrap()
}

fn guest_process(mut context: Context) -> ! {
	// Redirect
	redirect_stdio(&mut context.stdin, &mut context.stdout, &mut context.stderr);

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

fn create_sandbox_profile(command: &Command) -> CString {
	// Write the default profile.
	let mut profile = String::new();
	writedoc!(
		profile,
		"
			(version 1)
		"
	)
	.unwrap();
	if command.sandbox {
		writedoc!(
			profile,
			r#"
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
				(allow file-read-data (literal "/"))
				(allow file-read-metadata
					(literal "/Library")
					(literal "/System")
					(literal "/Users")
					(literal "/Volumes")
					(literal "/etc")
				)

				;; Allow writing to common devices.
				(allow file-read* file-write-data file-ioctl
					(literal "/dev/null")
					(literal "/dev/zero")
					(literal "/dev/dtracehelper")
				)

				;; Allow reading and writing temporary files.
				(allow file-write* file-read*
					(subpath "/tmp")
					(subpath "/private/tmp")
					(subpath "/private/var")
					(subpath "/var")
				)

				;; Allow reading some system devices and files.
				(allow file-read*
					(literal "/dev/autofs_nowait")
					(literal "/dev/random")
					(literal "/dev/urandom")
					(literal "/private/etc/localtime")
					(literal "/private/etc/protocols")
					(literal "/private/etc/services")
					(subpath "/private/etc/ssl")
				)

				;; Allow executing /usr/bin/env and /bin/sh.
				(allow file-read* process-exec
					(literal "/usr/bin/env")
					(literal "/bin/sh")
					(literal "/bin/bash")
				)

				;; Support Rosetta.
				(allow file-read* file-test-existence
					(literal "/Library/Apple/usr/libexec/oah/libRosettaRuntime")
				)

				;; Allow accessing the dyld shared cache.
				(allow file-read* process-exec
					(literal "/System/Volumes/Preboot/Cryptexes/OS/System/Library/dyld")
					(subpath "/System/Volumes/Preboot/Cryptexes/OS/System/Library/dyld")
				)

				;; Allow querying the macOS system version metadata.
				(allow file-read* file-test-existence
					(literal "/System/Library/CoreServices/SystemVersion.plist")
				)

				;; Allow bash to create and use file descriptors for pipes.
				(allow file-read* file-write* file-ioctl process-exec
					(literal "/dev/fd")
					(subpath "/dev/fd")
				)
			"#
		).unwrap();
	} else {
		writedoc!(
			profile,
			"
				;; Allow everything by default.
				(allow default)
			"
		)
		.unwrap();
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

	// Allow write access to the home directory.
	for path in &command.paths {
		if path.readonly {
			writedoc!(
				profile,
				r"
                        (allow process-exec* (subpath {0}))
                        (allow file-read* (path-ancestors {0}))
                        (allow file-read* (subpath {0}))
                ",
				escape(path.path.as_os_str().as_bytes()),
			)
			.unwrap();
		} else {
			writedoc!(
				profile,
				r"
                        (allow process-exec* (subpath {0}))
                        (allow file-read* (path-ancestors {0}))
                        (allow file-read* (subpath {0}))
                        (allow file-write* (subpath {0}))
                ",
				escape(path.path.as_os_str().as_bytes()),
			)
			.unwrap();
		}
	}

	CString::new(profile).unwrap()
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
