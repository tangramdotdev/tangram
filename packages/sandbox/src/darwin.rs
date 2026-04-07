use {
	crate::{InitArg, PrepareRootfsArg, Sandbox, SpawnArg},
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

pub fn spawn_jailer(arg: &SpawnArg, init_arg: &InitArg) -> tg::Result<tokio::process::Child> {
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
		.stdin(std::process::Stdio::null())
		.stdout(std::process::Stdio::inherit())
		.stderr(std::process::Stdio::inherit())
		.kill_on_drop(true);
	command
		.spawn()
		.map_err(|source| tg::error!(!source, "failed to spawn sandbox-exec"))
}

pub fn prepare_command_for_spawn(
	command: &mut crate::Command,
	tangram_path: &Path,
	library_paths: &[std::path::PathBuf],
) -> tg::Result<()> {
	let mut paths = Vec::new();
	if let Some(parent) = tangram_path.parent() {
		paths.push(parent);
	}
	paths.push(Path::new("/usr/bin"));
	paths.push(Path::new("/bin"));
	crate::append_directories_to_path(command, &paths)?;

	if library_paths.is_empty() || !crate::command_resolves_to_path(command, tangram_path) {
		return Ok(());
	}
	let mut paths = library_paths.to_vec();
	if let Some(existing) = command.env.get("DYLD_LIBRARY_PATH") {
		paths.extend(std::env::split_paths(existing));
	}
	let path = std::env::join_paths(paths)
		.map_err(|source| tg::error!(!source, "failed to build `DYLD_LIBRARY_PATH`"))?;
	let path = path
		.to_str()
		.ok_or_else(|| tg::error!("failed to encode `DYLD_LIBRARY_PATH` as valid UTF-8"))?;
	command
		.env
		.insert("DYLD_LIBRARY_PATH".to_owned(), path.to_owned());
	Ok(())
}

pub fn prepare_runtime_libraries(arg: &PrepareRootfsArg) -> tg::Result<()> {
	std::fs::remove_dir_all(&arg.path).ok();
	std::fs::create_dir_all(&arg.path)
		.map_err(|source| tg::error!(!source, "failed to create the sandbox directory"))?;
	let libraries = collect_dynamic_libraries(&arg.tangram_path)?;
	if libraries.is_empty() {
		return Ok(());
	}
	let libraries_path = arg.path.join("lib");
	std::fs::create_dir_all(&libraries_path).map_err(|source| {
		tg::error!(!source, "failed to create the sandbox libraries directory")
	})?;
	for source in libraries {
		let name = source.file_name().ok_or_else(|| {
			tg::error!(
				path = %source.display(),
				"failed to get the dynamic library file name"
			)
		})?;
		let target = libraries_path.join(name);
		if target.exists() {
			continue;
		}
		if std::fs::hard_link(&source, &target).is_err() {
			std::fs::copy(&source, &target).map_err(|error| {
				tg::error!(
					!error,
					source = %source.display(),
					target = %target.display(),
					"failed to stage the dynamic library"
				)
			})?;
		}
	}
	Ok(())
}

fn collect_dynamic_libraries(executable: &Path) -> tg::Result<Vec<std::path::PathBuf>> {
	let executable = std::fs::canonicalize(executable).map_err(|source| {
		tg::error!(
			!source,
			path = %executable.display(),
			"failed to canonicalize the executable path"
		)
	})?;
	let loaded_images = loaded_images();
	let mut queue = std::collections::VecDeque::from([executable]);
	let mut visited = std::collections::BTreeSet::new();
	let mut libraries = std::collections::BTreeSet::new();
	while let Some(path) = queue.pop_front() {
		if !visited.insert(path.clone()) {
			continue;
		}
		let rpaths = otool_rpaths(&path)?;
		let dependencies = otool_dependencies(&path)?;
		for dependency in dependencies {
			if path
				.extension()
				.is_some_and(|extension| extension == "dylib")
				&& Path::new(&dependency).file_name() == path.file_name()
			{
				continue;
			}
			let Some(dependency) =
				resolve_dynamic_library(&path, &rpaths, &loaded_images, &dependency)?
			else {
				continue;
			};
			if libraries.insert(dependency.clone()) {
				queue.push_back(dependency);
			}
		}
	}
	Ok(libraries.into_iter().collect())
}

fn otool_dependencies(path: &Path) -> tg::Result<Vec<String>> {
	let output = std::process::Command::new("otool")
		.args(["-L"])
		.arg(path)
		.output()
		.map_err(|source| {
			tg::error!(
				!source,
				path = %path.display(),
				"failed to execute `otool -L`"
			)
		})?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		let stdout = String::from_utf8_lossy(&output.stdout);
		return Err(tg::error!(
			status = %output.status,
			path = %path.display(),
			stderr = %stderr.trim(),
			stdout = %stdout.trim(),
			"`otool -L` failed"
		));
	}
	let stdout = String::from_utf8(output.stdout)
		.map_err(|source| tg::error!(!source, "failed to parse the `otool -L` output"))?;
	let mut dependencies = Vec::new();
	for line in stdout.lines().skip(1) {
		let line = line.trim();
		if line.is_empty() {
			continue;
		}
		let dependency = line
			.split_once(" (")
			.map_or(line, |(dependency, _)| dependency);
		dependencies.push(dependency.to_owned());
	}
	Ok(dependencies)
}

fn otool_rpaths(path: &Path) -> tg::Result<Vec<std::path::PathBuf>> {
	let output = std::process::Command::new("otool")
		.args(["-l"])
		.arg(path)
		.output()
		.map_err(|source| {
			tg::error!(
				!source,
				path = %path.display(),
				"failed to execute `otool -l`"
			)
		})?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		let stdout = String::from_utf8_lossy(&output.stdout);
		return Err(tg::error!(
			status = %output.status,
			path = %path.display(),
			stderr = %stderr.trim(),
			stdout = %stdout.trim(),
			"`otool -l` failed"
		));
	}
	let stdout = String::from_utf8(output.stdout)
		.map_err(|source| tg::error!(!source, "failed to parse the `otool -l` output"))?;
	let mut rpaths = Vec::new();
	let mut in_rpath = false;
	for line in stdout.lines() {
		let line = line.trim();
		if in_rpath && line.starts_with("path ") {
			let rpath = line.strip_prefix("path ").unwrap();
			let rpath = rpath.split_once(" (").map_or(rpath, |(rpath, _)| rpath);
			rpaths.push(resolve_dyld_path(path, rpath));
			in_rpath = false;
			continue;
		}
		in_rpath = line == "cmd LC_RPATH";
	}
	Ok(rpaths)
}

fn resolve_dynamic_library(
	binary: &Path,
	rpaths: &[std::path::PathBuf],
	loaded_images: &[std::path::PathBuf],
	dependency: &str,
) -> tg::Result<Option<std::path::PathBuf>> {
	let resolved = if let Some(suffix) = dependency.strip_prefix("@rpath/") {
		let path =
			resolve_rpath_dynamic_library(rpaths, loaded_images, suffix).ok_or_else(|| {
				tg::error!(
					binary = %binary.display(),
					%dependency,
					"failed to resolve the dynamic library"
				)
			})?;
		std::fs::canonicalize(&path).map_err(|source| {
			tg::error!(
				!source,
				path = %path.display(),
				"failed to canonicalize the dynamic library path"
			)
		})?
	} else {
		let path = resolve_dyld_path(binary, dependency);
		if is_system_library_path(&path) {
			return Ok(None);
		}
		std::fs::canonicalize(&path).map_err(|source| {
			tg::error!(
				!source,
				path = %path.display(),
				"failed to canonicalize the dynamic library path"
			)
		})?
	};
	if is_system_library_path(&resolved) {
		return Ok(None);
	}
	Ok(Some(resolved))
}

fn resolve_rpath_dynamic_library(
	rpaths: &[std::path::PathBuf],
	loaded_images: &[std::path::PathBuf],
	suffix: &str,
) -> Option<std::path::PathBuf> {
	resolve_dynamic_library_in_directories(rpaths, suffix)
		.or_else(|| resolve_dynamic_library_in_dyld_environment(suffix))
		.or_else(|| resolve_dynamic_library_in_loaded_images(loaded_images, suffix))
}

fn resolve_dynamic_library_in_directories(
	directories: &[std::path::PathBuf],
	suffix: &str,
) -> Option<std::path::PathBuf> {
	directories
		.iter()
		.map(|directory| directory.join(suffix))
		.find(|path| path.exists())
}

fn resolve_dynamic_library_in_dyld_environment(suffix: &str) -> Option<std::path::PathBuf> {
	let file_name = Path::new(suffix).file_name()?;
	[
		"DYLD_LIBRARY_PATH",
		"DYLD_FALLBACK_LIBRARY_PATH",
		"FDB_LIB_PATH",
	]
	.into_iter()
	.filter_map(std::env::var_os)
	.flat_map(|value| std::env::split_paths(&value).collect::<Vec<_>>())
	.map(|directory| directory.join(file_name))
	.find(|path| path.exists())
}

fn loaded_images() -> Vec<std::path::PathBuf> {
	unsafe extern "C" {
		fn _dyld_image_count() -> u32;
		fn _dyld_get_image_name(index: u32) -> *const libc::c_char;
	}
	let image_count = unsafe { _dyld_image_count() };
	(0..image_count)
		.filter_map(|index| {
			let path = unsafe { _dyld_get_image_name(index) };
			if path.is_null() {
				return None;
			}
			let path = unsafe { CStr::from_ptr(path) };
			let path = std::ffi::OsStr::from_bytes(path.to_bytes());
			let path = std::path::PathBuf::from(path);
			path.exists().then_some(path)
		})
		.collect()
}

fn resolve_dynamic_library_in_loaded_images(
	loaded_images: &[std::path::PathBuf],
	suffix: &str,
) -> Option<std::path::PathBuf> {
	let file_name = Path::new(suffix).file_name()?;
	loaded_images
		.iter()
		.find(|path| path.file_name() == Some(file_name))
		.cloned()
}

fn resolve_dyld_path(binary: &Path, path: &str) -> std::path::PathBuf {
	let parent = binary.parent().unwrap();
	if path == "@executable_path" || path == "@loader_path" {
		return parent.to_owned();
	}
	if let Some(path) = path.strip_prefix("@executable_path/") {
		return parent.join(path);
	}
	if let Some(path) = path.strip_prefix("@loader_path/") {
		return parent.join(path);
	}
	path.into()
}

fn is_system_library_path(path: &Path) -> bool {
	path.starts_with("/Library/Apple/System/")
		|| path.starts_with("/System/")
		|| path.starts_with("/System/iOSSupport/")
		|| path.starts_with("/usr/lib/")
		|| path.starts_with("/System/Volumes/Preboot/Cryptexes/OS/usr/lib/")
}

fn create_sandbox_profile(arg: &SpawnArg) -> CString {
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

	// Write the network profile.
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
