use super::{proxy::Proxy, util::render};
use crate::{temp::Temp, Server};
use futures::{
	future,
	stream::{FuturesOrdered, FuturesUnordered},
	TryStreamExt as _,
};
use indoc::writedoc;
use num::ToPrimitive as _;
use std::{
	ffi::{CStr, CString},
	fmt::Write as _,
	os::unix::{ffi::OsStrExt as _, process::ExitStatusExt as _},
	path::Path,
};
use tangram_client as tg;
use tangram_futures::task::Task;
use url::Url;

#[derive(Clone)]
pub struct Runtime {
	pub(crate) server: Server,
}

impl Runtime {
	pub fn new(server: &Server) -> Self {
		Self {
			server: server.clone(),
		}
	}

	pub async fn run(&self, process: &tg::Process) -> super::Output {
		let (error, exit, value) = match self.run_inner(process).await {
			Ok((exit, value)) => (None, exit, value),
			Err(error) => (Some(error), None, None),
		};
		super::Output {
			error,
			exit,
			output: value,
		}
	}

	pub async fn run_inner(
		&self,
		process: &tg::Process,
	) -> tg::Result<(Option<tg::process::Exit>, Option<tg::Value>)> {
		let state = process.load(&self.server).await?;
		let command = process.command(&self.server).await?;
		let remote = process.remote();

		// If the VFS is disabled, then check out the command's children.
		if self.server.vfs.lock().unwrap().is_none() {
			command
				.data(&self.server)
				.await?
				.children()
				.into_iter()
				.filter_map(|id| id.try_into().ok())
				.map(|id| async move {
					let artifact = tg::Artifact::with_id(id);
					let arg = tg::artifact::checkout::Arg::default();
					artifact.check_out(&self.server, arg).await?;
					Ok::<_, tg::Error>(())
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await?;
		}

		// Get the artifacts directory path.
		let artifacts_path = self.server.artifacts_path();

		// Create temps for the output
		let output_parent = Temp::new(&self.server);
		let output = output_parent.path().join("output");
		tokio::fs::create_dir_all(output_parent.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to create output directory"))?;

		// Get or create the home/working directory.
		let root = Temp::new(&self.server);
		let (home, cwd) = if let Some(cwd) = state.cwd.as_ref() {
			(None, cwd.clone())
		} else {
			let home = root.path().join("Users/tangram");
			tokio::fs::create_dir_all(&home)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the home directory"))?;
			let cwd = home.join("work");
			tokio::fs::create_dir_all(&cwd)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the working directory"))?;
			(Some(home), cwd)
		};

		// Create the proxy if running without cwd.
		let proxy = if let Some(home) = &home {
			let path = home.join(".tangram");
			tokio::fs::create_dir_all(&path)
				.await
				.map_err(|source| tg::error!(!source, %path = path.display(), "failed to create the proxy server directory"))?;
			let proxy = Proxy::new(self.server.clone(), process, remote.cloned(), None);

			let socket = path.join("socket").display().to_string();
			let path = urlencoding::encode(&socket);
			let url = format!("http+unix://{path}").parse::<Url>().unwrap();
			let listener = Server::listen(&url).await?;

			let task = Task::spawn(|stop| Server::serve(proxy, listener, stop));
			Some((task, url))
		} else {
			None
		};

		// Render the executable.
		let Some(tg::command::Executable::Artifact(executable)) =
			command.executable(&self.server).await?.as_ref().cloned()
		else {
			return Err(tg::error!("invalid executable"));
		};
		let executable = render(&self.server, &executable.into(), &artifacts_path).await?;

		// Render the env.
		let command_env = command.env(&self.server).await?;
		let process_env = state.env.as_ref();
		let mut env =
			super::util::merge_env(&self.server, &artifacts_path, process_env, &command_env)
				.await?;

		// Render the args.
		let args = command.args(&self.server).await?;
		let args: Vec<String> = args
			.iter()
			.map(|value| async {
				let value = render(&self.server, value, &artifacts_path).await?;
				Ok::<_, tg::Error>(value)
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		// Set `$HOME`.
		if let Some(home) = &home {
			env.insert("HOME".to_owned(), home.display().to_string());
		}

		// Set `$OUTPUT`.
		env.insert("OUTPUT".to_owned(), output.display().to_string());

		// Set `$TANGRAM_URL`.
		let url = proxy.as_ref().map_or_else(
			|| {
				let path = self.server.path.join("socket").display().to_string();
				let path = urlencoding::encode(&path);
				format!("http+unix://{path}")
			},
			|(_, url)| url.to_string(),
		);
		env.insert("TANGRAM_URL".to_owned(), url.to_string());

		// Create the sandbox profile.
		let canonical_artifacts_path = tokio::fs::canonicalize(&artifacts_path).await.map_err(
			|source| tg::error!(!source, %path = artifacts_path.display(), "failed to canonicalize"),
		)?;
		let canonical_home_path = if let Some(path) = home {
			Some(tokio::fs::canonicalize(&path).await.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to canonicalize"),
			)?)
		} else {
			None
		};
		let canonical_output_path = tokio::fs::canonicalize(&output_parent.path())
			.await
			.map_err(
				|source| tg::error!(!source, %path = output_parent.path().display(), "failed to canonicalize"),
			)?
			.join("output");
		let profile = create_sandbox_profile(
			&state,
			&canonical_artifacts_path,
			canonical_home_path.as_deref(),
			&canonical_output_path,
		);

		// Create the command.
		let mut command = tokio::process::Command::new(executable);
		command
			.args(args)
			.current_dir(cwd)
			.env_clear()
			.envs(env)
			.stdin(std::process::Stdio::piped())
			.stdout(std::process::Stdio::piped())
			.stderr(std::process::Stdio::piped());

		// Add a pre_exec hook to initialize the sandbox.
		unsafe {
			command.pre_exec(move || {
				// Call `sandbox_init`.
				let error = std::ptr::null_mut::<*const libc::c_char>();
				let ret = sandbox_init(profile.as_ptr(), 0, error);

				// Handle an error from `sandbox_init`.
				if ret != 0 {
					let error = *error;
					let _message = CStr::from_ptr(error);
					sandbox_free_error(error);
					tracing::debug!(?_message, "sandbox_init error");
					return Err(std::io::Error::last_os_error());
				}

				Ok(())
			});
		}

		// Spawn the child process.
		let mut child = command
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn child process"))?;

		// If this future is dropped, then kill its process tree.
		let pid = child.id().unwrap();
		scopeguard::defer! {
			kill_process_tree(pid);
		}

		// Spawn the log task.
		let log_task = super::util::post_log_task(
			&self.server,
			process,
			remote,
			child.stdout.take().unwrap(),
			child.stderr.take().unwrap(),
		);

		// Wait for the process to exit.
		let exit = child
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "failed to wait for the process to exit"))?;
		let exit = if let Some(code) = exit.code() {
			tg::process::Exit::Code { code }
		} else if let Some(signal) = exit.signal() {
			tg::process::Exit::Signal { signal }
		} else {
			return Err(tg::error!(%process = process.id(), "expected an exit code or signal"));
		};

		// Stop the proxy task.
		if let Some(task) = proxy.as_ref().map(|(proxy, _)| proxy) {
			task.stop();
			task.wait().await.unwrap();
		}

		// Join the i/o tasks.
		let (input, output) = future::try_join(future::ok(Ok::<_, tg::Error>(())), log_task)
			.await
			.map_err(|source| tg::error!(!source, "failed to join the i/o tasks"))?;
		input.map_err(|source| tg::error!(!source, "the stdin task failed"))?;
		output.map_err(|source| tg::error!(!source, "the log task failed"))?;

		// Create the output.
		let value = if tokio::fs::try_exists(output_parent.path().join("output"))
			.await
			.map_err(|source| tg::error!(!source, "failed to determine if the path exists"))?
		{
			let arg = tg::artifact::checkin::Arg {
				cache: true,
				destructive: true,
				deterministic: true,
				ignore: false,
				path: output_parent.path().join("output"),
				locked: true,
				lockfile: false,
			};
			let artifact = tg::Artifact::check_in(&self.server, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to check in the output"))?;
			Some(tg::Value::from(artifact))
		} else {
			None
		};

		Ok((Some(exit), value))
	}
}

fn create_sandbox_profile(
	process: &tg::process::State,
	artifacts: &Path,
	home: Option<&Path>,
	output: &Path,
) -> CString {
	// Write the default profile.
	let mut profile = String::new();
	writedoc!(
		profile,
		"
			(version 1)
		"
	)
	.unwrap();
	if process.cwd.is_some() {
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
	}

	// Write the network profile.
	if process.network {
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

	// Allow read access to the artifacts directory.
	writedoc!(
		profile,
		r"
			(allow process-exec* (subpath {0}))
			(allow file-read* (path-ancestors {0}))
			(allow file-read* (subpath {0}))
		",
		escape(artifacts.as_os_str().as_bytes())
	)
	.unwrap();

	// Allow write access to the home directory.
	if let Some(home) = home {
		writedoc!(
			profile,
			r"
				(allow process-exec* (subpath {0}))
				(allow file-read* (path-ancestors {0}))
				(allow file-read* (subpath {0}))
				(allow file-write* (subpath {0}))
			",
			escape(home.as_os_str().as_bytes())
		)
		.unwrap();
	}

	// Allow write access to the output parent directory.
	writedoc!(
		profile,
		r"
			(allow process-exec* (subpath {0}))
			(allow file-read* (path-ancestors {0}))
			(allow file-read* (subpath {0}))
			(allow file-write* (subpath {0}))
		",
		escape(output.as_os_str().as_bytes())
	)
	.unwrap();

	CString::new(profile).unwrap()
}

fn kill_process_tree(pid: u32) {
	let mut pids = vec![pid.to_i32().unwrap()];
	let mut i = 0;
	while i < pids.len() {
		let ppid = pids[i];
		let n = unsafe { libc::proc_listchildpids(ppid, std::ptr::null_mut(), 0) };
		if n < 0 {
			let error = std::io::Error::last_os_error();
			tracing::error!(?pid, ?error);
			return;
		}
		pids.resize(i + n.to_usize().unwrap() + 1, 0);
		let n = unsafe { libc::proc_listchildpids(ppid, pids[(i + 1)..].as_mut_ptr().cast(), n) };
		if n < 0 {
			let error = std::io::Error::last_os_error();
			tracing::error!(?pid, ?error);
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_escape_string() {
		assert_eq!(escape(r#"quote ""#), r#""quote \"""#);
		assert_eq!(escape("backslash \\"), r#""backslash \\""#);
		assert_eq!(escape("newline \n"), r#""newline \n""#);
		assert_eq!(escape("tab \t"), r#""tab \t""#);
		assert_eq!(escape("return \r"), r#""return \r""#);
		assert_eq!(escape("nul \0"), r#""nul \x00""#);
		assert_eq!(escape("many \r\t\n\\\r\n"), r#""many \r\t\n\\\r\n""#);
	}
}
