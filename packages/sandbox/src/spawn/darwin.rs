use {
	crate::Sandbox,
	indoc::writedoc,
	std::{ffi::CString, fmt::Write as _, os::unix::ffi::OsStrExt as _, path::Path},
	tangram_client::prelude::*,
};

pub fn spawn(arg: &crate::Arg, init_arg: &crate::init::Arg) -> tg::Result<tokio::process::Child> {
	for path in [
		Sandbox::host_output_path_from_root(&arg.path),
		Sandbox::host_tangram_socket_path_from_root(&arg.path)
			.parent()
			.unwrap()
			.to_owned(),
		Sandbox::host_scratch_path_from_root(&arg.path),
		Sandbox::host_profile_path_from_root(&arg.path)
			.parent()
			.unwrap()
			.to_owned(),
	] {
		std::fs::create_dir_all(&path).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to create the sandbox path"),
		)?;
	}
	let profile = create_sandbox_profile(arg);
	std::fs::write(
		Sandbox::host_profile_path_from_root(&arg.path),
		profile.as_bytes(),
	)
	.map_err(|source| tg::error!(!source, "failed to write the sandbox profile"))?;
	let mut command = tokio::process::Command::new("sandbox-exec");
	command
		.arg("-f")
		.arg(Sandbox::host_profile_path_from_root(&arg.path));
	if init_arg.library_paths.is_empty() {
		command.arg(&arg.tangram_path);
		crate::append_init_args(&mut command, init_arg);
	} else {
		let mut paths = init_arg.library_paths.clone();
		if let Some(existing) = std::env::var_os("DYLD_LIBRARY_PATH") {
			paths.extend(std::env::split_paths(&existing));
		}
		let path = std::env::join_paths(paths)
			.map_err(|source| tg::error!(!source, "failed to build `DYLD_LIBRARY_PATH`"))?;
		command
			.arg("/bin/sh")
			.arg("-c")
			.arg(r#"export DYLD_LIBRARY_PATH="$1"; shift; exec "$@""#)
			.arg("sh")
			.arg(path)
			.arg(&arg.tangram_path);
		crate::append_init_args(&mut command, init_arg);
	}
	command
		.kill_on_drop(true)
		.stdin(std::process::Stdio::null())
		.stdout(std::process::Stdio::inherit())
		.stderr(std::process::Stdio::inherit());
	command
		.spawn()
		.map_err(|source| tg::error!(!source, "failed to spawn sandbox-exec"))
}

fn create_sandbox_profile(arg: &crate::Arg) -> CString {
	let tangram_parent = arg.tangram_path.parent();
	let home_path = std::env::var_os("HOME").map(std::path::PathBuf::from);
	let mut profile = String::new();
	writedoc!(
		profile,
		"
			(version 1)
		"
	)
	.unwrap();

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

			;; Allow signaling child processes spawned by the sandbox server.
			(allow signal (target children))

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
			(allow file-write* file-write-create file-write-mode file-write-unlink
				file-link file-read* process-exec*
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
	)
	.unwrap();

	writedoc!(
		profile,
		r#"
			;; Allow exec'ing the tg binary itself.
			(allow file-read* process-exec
				(literal "{}"))

			;; Allow mapping the staged runtime libraries.
			(allow file-read* file-map-executable
				(subpath "{}"))

			;; Allow reading and writing to the sandbox paths.
			(allow file-read* file-write* file-write-create file-write-mode
				file-write-unlink file-link
				(literal "{}")
				(literal "{}"))
			(allow file-read* file-write* file-write-create file-write-mode
				file-write-unlink file-link process-exec
				(subpath "{}")
				(subpath "{}")
				(subpath "{}"))
		"#,
		arg.tangram_path.display(),
		arg.rootfs_path.join("lib").display(),
		Sandbox::host_tangram_socket_path_from_root(&arg.path).display(),
		Sandbox::host_listen_path_from_root(&arg.path).display(),
		Sandbox::host_tangram_socket_path_from_root(&arg.path).display(),
		Sandbox::host_output_path_from_root(&arg.path).display(),
		Sandbox::host_scratch_path_from_root(&arg.path)
			.parent()
			.unwrap()
			.display(),
	)
	.unwrap();

	if let Some(tangram_parent) = tangram_parent {
		writedoc!(
			profile,
			r"
				;; Allow CoreFoundation to inspect the Tangram binary path.
				(allow file-read* file-test-existence
					(subpath {}))
			",
			escape(tangram_parent.as_os_str().as_bytes()),
		)
		.unwrap();
		for ancestor in tangram_parent
			.ancestors()
			.skip(1)
			.take_while(|path| *path != Path::new("/"))
		{
			writedoc!(
				profile,
				r"
					(allow file-read-metadata
						(literal {}))
				",
				escape(ancestor.as_os_str().as_bytes()),
			)
			.unwrap();
		}
	}

	if let Some(home_path) = &home_path {
		let cf_user_text_encoding = home_path.join(".CFUserTextEncoding");
		writedoc!(
			profile,
			r"
				;; Allow CoreFoundation to read the user locale metadata.
				(allow file-read* file-test-existence
					(literal {})
					(literal {}))
			",
			escape(home_path.as_os_str().as_bytes()),
			escape(cf_user_text_encoding.as_os_str().as_bytes()),
		)
		.unwrap();
	}

	if arg.network {
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
				(allow user-preference-read
					(preference-domain "com.apple.CFNetwork")
					(preference-domain "com.apple.SystemConfiguration")
					(preference-domain "kCFPreferencesAnyApplication"))
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

	for mount in &arg.mounts {
		if mount.readonly {
			let path = &mount.source;
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
			let path = &mount.source;
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

	CString::new(profile).unwrap()
}

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
