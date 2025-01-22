use super::{proxy::Proxy, util::render};
use crate::{temp::Temp, Server};
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	TryStreamExt as _,
};
use indoc::writedoc;
use num::ToPrimitive as _;
use std::{
	collections::BTreeMap,
	ffi::{CStr, CString},
	fmt::Write as _,
	os::unix::{ffi::OsStrExt as _, process::ExitStatusExt as _},
	path::{Path, PathBuf},
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

	pub async fn run(
		&self,
		process: &tg::process::Id,
		command: &tg::Command,
		remote: Option<String>,
	) -> super::Output {
		let (error, exit, value) = match self.run_inner(process, command, remote).await {
			Ok((exit, value)) => (None, exit, value),
			Err(error) => (Some(error), None, None),
		};
		super::Output { error, exit, value }
	}

	pub async fn run_inner(
		&self,
		process: &tg::process::Id,
		command: &tg::Command,
		remote: Option<String>,
	) -> tg::Result<(Option<tg::process::Exit>, Option<tg::Value>)> {
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

		// Create a temp for the output.
		let output_parent = Temp::new(&self.server);
		tokio::fs::create_dir_all(output_parent.path())
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to create parent directory for output")
			})?;

		// Run with or without the sandbox.
		let sandbox = command.sandbox(&self.server).await?;
		let exit = if let Some(sandbox) = sandbox.as_ref() {
			self.run_sandboxed(process, command, sandbox, remote, output_parent.path())
				.await?
		} else {
			self.run_unsandboxed(process, command, remote, output_parent.path())
				.await?
		};

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

	async fn run_unsandboxed(
		&self,
		process: &tg::process::Id,
		command: &tg::Command,
		remote: Option<String>,
		output_parent: &Path,
	) -> tg::Result<tg::process::Exit> {
		// Render the executable.
		let Some(tg::command::Executable::Artifact(executable)) =
			command.executable(&self.server).await?.as_ref().cloned()
		else {
			return Err(tg::error!("invalid executable"));
		};
		let executable = render(
			&self.server,
			&executable.into(),
			&self.server.artifacts_path(),
		)
		.await?;

		// Get or create the current working directory.
		let cwd = Temp::new(&self.server);
		let cwd = if let Some(cwd) = command.cwd(&self.server).await?.as_ref() {
			cwd.clone()
		} else {
			tokio::fs::create_dir_all(cwd.path()).await.map_err(
				|source| tg::error!(!source, %process, "failed to create working directory for process"),
			)?;
			cwd.path().to_owned()
		};

		// Render the args.
		let args = command.args(&self.server).await?;
		let args: Vec<String> = args
			.iter()
			.map(|value| async {
				let value = render(&self.server, value, &self.server.artifacts_path()).await?;
				Ok::<_, tg::Error>(value)
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		// Render the env.
		let env = command.env(&self.server).await?;
		let env: BTreeMap<String, String> = env
			.iter()
			.map(|(key, value)| async {
				let key = key.clone();
				let value = render(&self.server, value, &self.server.artifacts_path()).await?;
				Ok::<_, tg::Error>((key, value))
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		// Spawn the child process.
		let mut child = tokio::process::Command::new(executable)
			.kill_on_drop(true)
			.env_clear()
			.current_dir(cwd)
			.args(args)
			.envs(env)
			.env("OUTPUT", output_parent.join("output"))
			.stdin(std::process::Stdio::null())
			.stdout(std::process::Stdio::piped())
			.stderr(std::process::Stdio::piped())
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;

		// If this future is dropped, then kill its process tree.
		let pid = child.id().unwrap();
		scopeguard::defer! {
			kill_process_tree(pid);
		}

		// Spawn the log task.
		let log_task = super::util::post_log_task(
			&self.server,
			process,
			remote.as_ref(),
			child.stdout.take().unwrap(),
		);

		// Wait for the child to complete.
		let exit = child
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "failed to wait for the child process"))?;
		let exit = if let Some(code) = exit.code() {
			tg::process::Exit::Code { code }
		} else if let Some(signal) = exit.signal() {
			tg::process::Exit::Signal { signal }
		} else {
			return Err(tg::error!(%process, "expected an exit code or signal"));
		};

		// Wait for the log task to complete.
		log_task
			.await
			.map_err(|source| tg::error!(!source, "failed to join the log task"))?
			.map_err(|source| tg::error!(!source, "the log task panicked"))?;

		// Return the exit status.
		Ok(exit)
	}

	async fn run_sandboxed(
		&self,
		process: &tg::process::Id,
		command: &tg::Command,
		sandbox: &tg::command::Sandbox,
		remote: Option<String>,
		output_parent: &Path,
	) -> tg::Result<tg::process::Exit> {
		let server = &self.server;

		// Get the artifacts directory path.
		let artifacts_directory_path = server.artifacts_path();

		// Create the output path.
		let output_path = output_parent.join("output");

		// Create the home directory.
		let root_directory_temp = Temp::new(server);
		tokio::fs::create_dir_all(&root_directory_temp)
			.await
			.map_err(|error| {
				tg::error!(
					source = error,
					"failed to create the root temporary directory"
				)
			})?;
		let root_directory_path = PathBuf::from(root_directory_temp.as_ref());
		let home_directory_path = root_directory_path.join("Users/tangram");
		tokio::fs::create_dir_all(&home_directory_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the home directory"))?;

		// Create the server directory.
		let server_directory_path = home_directory_path.join(".tangram");
		tokio::fs::create_dir_all(&server_directory_path)
			.await
			.map_err(|error| tg::error!(source = error, "failed to create the server directory"))?;

		// Create the working directory.
		let working_directory = if let Some(cwd) = command.cwd(&self.server).await?.as_ref() {
			if !sandbox.filesystem {
				return Err(
					tg::error!(%process, "cannot run a process with cwd set and sandbox.filesystem = false"),
				);
			};
			cwd.clone()
		} else {
			let working_directory = root_directory_path.join("Users/tangram/work");
			tokio::fs::create_dir_all(&working_directory)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the working directory"))?;
			working_directory
		};

		// Create the proxy server URL.
		let proxy_server_socket_path = home_directory_path.join(".tangram/socket");
		let proxy_server_socket_path = proxy_server_socket_path.display().to_string();
		let proxy_server_socket_path = urlencoding::encode(&proxy_server_socket_path);
		let proxy_server_url = format!("http+unix://{proxy_server_socket_path}");
		let proxy_server_url = Url::parse(&proxy_server_url)
			.map_err(|source| tg::error!(!source, "failed to parse the proxy server url"))?;

		// Start the proxy server.
		let proxy = Proxy::new(server.clone(), process.clone(), remote.clone(), None);
		let listener = Server::listen(&proxy_server_url).await?;
		let proxy_task = Task::spawn(|stop| Server::serve(proxy, listener, stop));

		// Render the executable.
		let Some(tg::command::Executable::Artifact(executable)) =
			command.executable(server).await?.as_ref().cloned()
		else {
			return Err(tg::error!("invalid executable"));
		};
		let executable = render(server, &executable.into(), &artifacts_directory_path).await?;

		// Render the env.
		let env = command.env(server).await?;
		let mut env: BTreeMap<String, String> = env
			.iter()
			.map(|(key, value)| async {
				let key = key.clone();
				let value = render(server, value, &artifacts_directory_path).await?;
				Ok::<_, tg::Error>((key, value))
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		// Render the args.
		let args = command.args(server).await?;
		let args: Vec<String> = args
			.iter()
			.map(|value| async {
				let value = render(server, value, &artifacts_directory_path).await?;
				Ok::<_, tg::Error>(value)
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		// Set `$HOME`.
		env.insert(
			"HOME".to_owned(),
			home_directory_path.to_str().unwrap().to_owned(),
		);

		// Set `$OUTPUT`.
		env.insert(
			"OUTPUT".to_owned(),
			output_path.to_str().unwrap().to_owned(),
		);

		// Set `$TANGRAM_URL`.
		env.insert("TANGRAM_URL".to_owned(), proxy_server_url.to_string());

		// Create the sandbox profile.
		let profile = create_sandbox_profile(
			sandbox,
			&server.artifacts_path(),
			&home_directory_path,
			output_parent,
		);

		// Create the command.
		let mut command = tokio::process::Command::new(&executable);

		// Set the working directory.
		command.current_dir(&working_directory);

		// Set the envs.
		command.env_clear();
		command.envs(env);

		// Set the args.
		command.args(args);

		// Redirect stdout and stderr to a pipe.
		command.stdin(std::process::Stdio::null());
		command.stdout(std::process::Stdio::piped());
		command.stderr(std::process::Stdio::piped());

		// Set up the sandbox.
		unsafe {
			command.pre_exec(move || {
				// Redirect stderr to stdout.
				if libc::dup2(libc::STDOUT_FILENO, libc::STDERR_FILENO) < 0 {
					return Err(std::io::Error::last_os_error());
				}

				// Call `sandbox_init`.
				let error = std::ptr::null_mut::<*const libc::c_char>();
				let ret = sandbox_init(profile.as_ptr(), 0, error);

				// Handle an error from `sandbox_init`.
				if ret != 0 {
					let error = *error;
					let _message = CStr::from_ptr(error);
					sandbox_free_error(error);
					return Err(std::io::Error::last_os_error());
				}

				Ok(())
			})
		};

		// Spawn the child.
		let mut child = command
			.spawn()
			.map_err(|source| tg::error!(!source, %executable, "failed to spawn the process"))?;

		// If this future is dropped, then kill its process tree.
		let pid = child.id().unwrap();
		scopeguard::defer! {
			kill_process_tree(pid);
		}

		// Spawn the log task.
		let log_task = super::util::post_log_task(
			&self.server,
			process,
			remote.as_ref(),
			child.stdout.take().unwrap(),
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
			return Err(tg::error!(%process, "expected an exit code or signal"));
		};

		// Wait for the log task to complete.
		log_task
			.await
			.map_err(|source| tg::error!(!source, "failed to join the log task"))?
			.map_err(|source| tg::error!(!source, "the log task panicked"))?;

		// Stop the proxy task.
		proxy_task.stop();
		proxy_task.wait().await.unwrap();

		Ok(exit)
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

fn create_sandbox_profile(
	sandbox: &tg::command::Sandbox,
	artifacts_path: &Path,
	home_directory: &Path,
	output_parent: &Path,
) -> CString {
	// Write the default profile.
	let mut profile = String::new();
	writedoc!(
		profile,
			r#"
				(version 1)

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

	// Write the filesystem profile.
	if sandbox.filesystem {
		let home = std::env::var_os("HOME").unwrap();
		writedoc!(
			profile,
			r#"
					;; Allow full read access of the file system.
					(allow file-read*
						(subpath "/")
					)

					;; Allow write access under the current user's home directory
					(allow file-write*
						(subpath "{0}")
					)
				"#,
			escape(home.as_bytes())
		)
		.unwrap();
	}

	// Write the network profile.
	if sandbox.network {
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
		escape(artifacts_path.as_os_str().as_bytes())
	)
	.unwrap();

	// Allow write access to the home directory.
	writedoc!(
		profile,
		r"
				(allow process-exec* (subpath {0}))
				(allow file-read* (path-ancestors {0}))
				(allow file-read* (subpath {0}))
				(allow file-write* (subpath {0}))
			",
		escape(home_directory.as_os_str().as_bytes())
	)
	.unwrap();

	// Allow write access to the output parent directory.
	writedoc!(
		profile,
		r"
				(allow process-exec* (subpath {0}))
				(allow file-read* (path-ancestors {0}))
				(allow file-read* (subpath {0}))
				(allow file-write* (subpath {0}))
			",
		escape(output_parent.as_os_str().as_bytes())
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
