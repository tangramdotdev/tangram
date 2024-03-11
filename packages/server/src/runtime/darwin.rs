use super::{proxy::Proxy, util::render};
use crate::Server;
use bytes::Bytes;
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	TryStreamExt,
};
use indoc::writedoc;
use num::ToPrimitive;
use std::{
	collections::BTreeMap,
	ffi::{CStr, CString},
	fmt::Write,
	os::unix::prelude::OsStrExt,
};
use tangram_client as tg;
use tangram_error::{error, Error, Result, Wrap, WrapErr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub async fn build(server: &Server, build: &tg::Build) -> Result<tg::Value> {
	// Get the target.
	let target = build.target(server).await?;

	// If the VFS is disabled, then perform an internal checkout of the target's references.
	if server.inner.vfs.lock().unwrap().is_none() {
		target
			.data(server)
			.await?
			.children()
			.into_iter()
			.filter_map(|id| id.try_into().ok())
			.map(|id| async move {
				let artifact = tg::Artifact::with_id(id);
				artifact.check_out(server, None).await
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
	}

	// Get the artifacts path.
	let artifacts_directory_path = server.inner.options.path.join("artifacts");

	// Get the server temp directory path.
	let server_directory_temp_path = server.inner.options.path.join("tmp");
	tokio::fs::create_dir_all(&server_directory_temp_path)
		.await
		.wrap_err("Failed to create the server temp directory.")?;

	let root_directory_tempdir = tempfile::TempDir::new_in(&server_directory_temp_path)
		.wrap_err("Failed to create the temporary directory.")?;
	let root_directory_path = root_directory_tempdir.path().to_owned();

	// Create a tempdir for the output.
	let output_tempdir = tempfile::TempDir::new_in(&server_directory_temp_path)
		.wrap_err("Failed to create the temporary directory.")?;

	// Create the output parent directory.
	let output_parent_directory_path = output_tempdir.path().to_owned();
	tokio::fs::create_dir_all(&output_parent_directory_path)
		.await
		.wrap_err("Failed to create the output parent directory.")?;

	// Create the output path.
	let output_path = output_parent_directory_path.join("output");

	// Create the home directory.
	let home_directory_path = root_directory_path.join("Users/tangram");
	tokio::fs::create_dir_all(&home_directory_path)
		.await
		.wrap_err("Failed to create the home directory.")?;

	// Create the working directory.
	let working_directory_path = root_directory_path.join("Users/tangram/work");
	tokio::fs::create_dir_all(&working_directory_path)
		.await
		.wrap_err("Failed to create the working directory.")?;

	// Render the executable.
	let executable = target.executable(server).await?;
	let executable = render(
		server,
		&executable.clone().into(),
		&artifacts_directory_path,
	)
	.await?;

	// Render the env.
	let env = target.env(server).await?;
	let mut env: BTreeMap<String, String> = env
		.iter()
		.map(|(key, value)| async {
			let key = key.clone();
			let value = render(server, value, &artifacts_directory_path).await?;
			Ok::<_, Error>((key, value))
		})
		.collect::<FuturesOrdered<_>>()
		.try_collect()
		.await?;

	// Render the args.
	let args = target.args(server).await?;
	let args: Vec<String> = args
		.iter()
		.map(|value| async {
			let value = render(server, value, &artifacts_directory_path).await?;
			Ok::<_, Error>(value)
		})
		.collect::<FuturesOrdered<_>>()
		.try_collect()
		.await?;

	// Enable the network if a checksum was provided.
	let network_enabled = target.checksum(server).await?.is_some();

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

	// Set `$TANGRAM_ADDRESS`
	tokio::fs::create_dir_all(home_directory_path.join(".tangram"))
		.await
		.wrap_err("Failed to create the guest .tangram directory.")?;
	let proxy_server_socket_path = home_directory_path.join(".tangram/socket");
	let proxy_server_address = tg::Address::Unix(proxy_server_socket_path.clone());
	env.insert(
		"TANGRAM_ADDRESS".to_owned(),
		proxy_server_address.to_string(),
	);

	// Create a proxied server handle and start listening on a new socket.
	let proxy = Proxy::start(server, build.id(), proxy_server_address)
		.await
		.wrap_err("Could not create proxy server")?;

	// Create the sandbox profile.
	let mut profile = String::new();

	// Write the default profile.
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

			;; Allow bash to create and use file descriptors for pipes.
			(allow file-read* file-write* file-ioctl process-exec
				(literal "/dev/fd")
				(subpath "/dev/fd")
			)
		"#
	).unwrap();

	// Write the network profile.
	if network_enabled {
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
		r#"
			(allow process-exec* (subpath {0}))
			(allow file-read* (path-ancestors {0}))
			(allow file-read* (subpath {0}))
			(allow file-write* (subpath {0}))
		"#,
		escape(artifacts_directory_path.as_os_str().as_bytes())
	)
	.unwrap();

	// Allow write access to the home directory.
	writedoc!(
		profile,
		r#"
			(allow process-exec* (subpath {0}))
			(allow file-read* (path-ancestors {0}))
			(allow file-read* (subpath {0}))
			(allow file-write* (subpath {0}))
		"#,
		escape(home_directory_path.as_os_str().as_bytes())
	)
	.unwrap();

	// Allow write access to the output parent directory.
	writedoc!(
		profile,
		r#"
			(allow process-exec* (subpath {0}))
			(allow file-read* (path-ancestors {0}))
			(allow file-read* (subpath {0}))
			(allow file-write* (subpath {0}))
		"#,
		escape(output_parent_directory_path.as_os_str().as_bytes())
	)
	.unwrap();

	// Make the profile a C string.
	let profile = CString::new(profile).unwrap();

	// Create the command.
	let mut command = tokio::process::Command::new(&executable);

	// Set the working directory.
	command.current_dir(&working_directory_path);

	// Set the envs.
	command.env_clear();
	command.envs(env);

	// Set the args.
	command.args(args);

	// Redirect stdout to a pipe.
	command.stdout(std::process::Stdio::piped());

	// Set up the sandbox.
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
				return Err(std::io::Error::last_os_error());
			}

			// Redirect stderr to stdout.
			if libc::dup2(libc::STDOUT_FILENO, libc::STDERR_FILENO) < 0 {
				return Err(std::io::Error::last_os_error());
			}

			Ok(())
		})
	};

	// Spawn the child.
	let mut child = command
		.spawn()
		.wrap_err_with(|| format!("Failed to spawn the process ({executable})."))?;

	// Create the log task.
	let mut stdout = child.stdout.take().unwrap();
	let log_task = tokio::task::spawn({
		let build = build.clone();
		let server = server.clone();
		async move {
			let mut buf = [0; 512];
			loop {
				match stdout.read(&mut buf).await {
					Err(error) => return Err(error.wrap("Failed to read from the log.")),
					Ok(0) => return Ok(()),
					Ok(size) => {
						let log = Bytes::copy_from_slice(&buf[0..size]);
						if server.inner.options.write_build_logs_to_stderr {
							tokio::io::stderr()
								.write_all(&log)
								.await
								.inspect_err(|e| {
									tracing::error!(?e, "Failed to write build log to stderr.");
								})
								.ok();
						}
						build.add_log(&server, log).await?;
					},
				}
			}
		}
	});

	// If this future is dropped, then kill its process tree.
	let pid = child.id().unwrap();
	scopeguard::defer! {
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
			let n = unsafe {
				libc::proc_listchildpids(ppid, pids[(i + 1)..].as_mut_ptr().cast(), n)
			};
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

	// Wait for the process to exit.
	let exit_status = child
		.wait()
		.await
		.wrap_err("Failed to wait for the process to exit.")?;

	// Wait for the log task to complete.
	log_task
		.await
		.wrap_err("Failed to join the log task.")?
		.wrap_err("The log task failed.")?;

	// Stop and join the proxy server.
	proxy.stop();
	proxy.join().await?;

	// Return an error if the process did not exit successfully.
	if !exit_status.success() {
		return Err(error!("The process did not exit successfully."));
	}

	// Create the output.
	let value = if tokio::fs::try_exists(&output_path)
		.await
		.wrap_err("Failed to determine if the path exists.")?
	{
		// Check in the output.
		let artifact = tg::Artifact::check_in(server, &output_path.clone().try_into()?)
			.await
			.wrap_err("Failed to check in the output.")?;

		// Verify the checksum if one was provided.
		if let Some(expected) = target.checksum(server).await?.clone() {
			let actual = artifact
				.checksum(server, expected.algorithm())
				.await
				.wrap_err("Failed to compute the checksum.")?;
			if expected != tg::Checksum::Unsafe && expected != actual {
				error!(r#"The checksum did not match. Expected "{expected}" but got "{actual}"."#);
			}
		}

		artifact.into()
	} else {
		tg::Value::Null
	};

	Ok(value)
}

extern "C" {
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
