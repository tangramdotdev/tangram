use {
	crate::{Directory, Manager, ManagerArg, RunArg, SpawnArg},
	indoc::indoc,
	std::{
		ffi::{CStr, CString, OsStr},
		fmt::Write as _,
		os::{fd::RawFd, unix::ffi::OsStrExt as _},
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

struct User {
	gid: libc::gid_t,
	home: PathBuf,
	name: String,
	uid: libc::uid_t,
}

pub fn prepare_runtime_libraries(arg: &ManagerArg) -> tg::Result<Vec<PathBuf>> {
	std::fs::remove_dir_all(&arg.rootfs_path).ok();
	std::fs::create_dir_all(&arg.rootfs_path)
		.map_err(|source| tg::error!(!source, "failed to create the sandbox directory"))?;
	let permissions = <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o755);
	crate::ROOTFS
		.extract(&arg.rootfs_path)
		.map_err(|source| tg::error!(!source, "failed to extract the sandbox rootfs"))?;
	crate::set_rootfs_permissions(&arg.rootfs_path, &crate::ROOTFS, &permissions)?;
	prepare_rootfs_mountpoints(&arg.rootfs_path)?;

	let lib_path = arg.rootfs_path.join("opt/tangram/lib");
	let output = std::process::Command::new("ldd")
		.arg(&arg.tangram_path)
		.output()
		.map_err(|source| {
			if source.kind() == std::io::ErrorKind::NotFound {
				tg::error!(
					"failed to prepare the sandbox rootfs: could not execute `ldd`; install `ldd` on this Linux host"
				)
			} else {
				tg::error!(
					!source,
					path = %arg.tangram_path.display(),
					"failed to execute `ldd`"
				)
			}
		})?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		let stdout = String::from_utf8_lossy(&output.stdout);
		return Err(tg::error!(
			status = %output.status,
			path = %arg.tangram_path.display(),
			stderr = %stderr.trim(),
			stdout = %stdout.trim(),
			"`ldd` failed"
		));
	}
	let stdout = String::from_utf8(output.stdout)
		.map_err(|source| tg::error!(!source, "failed to parse the `ldd` output"))?;
	for line in stdout.lines() {
		let line = line.trim();
		if line.is_empty() || line.starts_with("linux-vdso") {
			continue;
		}
		let parsed = if let Some((name, path)) = line.split_once("=>") {
			let name = name.trim();
			let path = path.trim();
			if path == "not found" {
				return Err(tg::error!(
					dependency = %name,
					executable = %arg.tangram_path.display(),
					"`ldd` reported a missing dependency"
				));
			}
			let path = path
				.split_whitespace()
				.next()
				.ok_or_else(|| tg::error!("failed to parse a path from the `ldd` output"))?;
			path.starts_with('/').then(|| PathBuf::from(path))
		} else if line.starts_with('/') {
			let path = line
				.split_whitespace()
				.next()
				.ok_or_else(|| tg::error!("failed to parse a path from the `ldd` output"))?;
			Some(PathBuf::from(path))
		} else {
			None
		};
		let Some(dependency_path) = parsed else {
			continue;
		};
		let source = std::fs::canonicalize(&dependency_path).map_err(|source| {
			tg::error!(
				!source,
				path = %dependency_path.display(),
				"failed to canonicalize the library path"
			)
		})?;
		let name = dependency_path
			.file_name()
			.and_then(|name| name.to_str())
			.ok_or_else(|| {
				tg::error!(
					path = %dependency_path.display(),
					"failed to get the library file name"
				)
			})?;
		let target = lib_path.join(name);
		if target.exists() {
			continue;
		}
		if std::fs::hard_link(&source, &target).is_err() {
			std::fs::copy(&source, &target).map_err(|error| {
				tg::error!(
					!error,
					source = %source.display(),
					target = %target.display(),
					"failed to stage the shared library"
				)
			})?;
		}
		std::fs::set_permissions(&target, permissions.clone()).map_err(|source| {
			tg::error!(
				!source,
				path = %target.display(),
				"failed to set sandbox file permissions"
			)
		})?;
	}

	Ok(Vec::new())
}

pub fn prepare_command_for_spawn(
	command: &mut crate::Command,
	_tangram_path: &Path,
	_library_paths: &[PathBuf],
) -> tg::Result<()> {
	crate::append_directories_to_path(
		command,
		&[
			Path::new("/opt/tangram/bin"),
			Path::new("/usr/bin"),
			Path::new("/bin"),
		],
	)
}

pub fn spawn_jailer(
	manager: &Manager,
	arg: &SpawnArg,
	run_arg: &RunArg,
	ready_fd: RawFd,
) -> tg::Result<tokio::process::Child> {
	let directory = Directory::new(arg.path.clone());
	prepare_sandbox_directory(&directory)?;
	let user = prepare_etc_files(&directory, arg.network, arg.user.as_deref())?;
	prepare_mount_targets(
		&manager.rootfs_path,
		&directory.host_upper_path(),
		&arg.mounts,
	)?;

	let mut command = tokio::process::Command::new("bwrap");
	command
		.arg("--unshare-all")
		.arg("--as-pid-1")
		.arg("--die-with-parent")
		.arg("--new-session")
		.arg("--sync-fd")
		.arg(ready_fd.to_string())
		.arg("--uid")
		.arg(user.uid.to_string())
		.arg("--gid")
		.arg(user.gid.to_string())
		.arg("--chdir")
		.arg("/")
		.arg("--overlay-src")
		.arg(&manager.rootfs_path)
		.arg("--overlay")
		.arg(directory.host_upper_path())
		.arg(directory.host_work_path())
		.arg("/")
		.arg("--dev")
		.arg("/dev")
		.arg("--proc")
		.arg("/proc")
		.arg("--bind")
		.arg(directory.host_tmp_path())
		.arg(directory.guest_tmp_path());
	if arg.network {
		command.arg("--share-net");
	}
	if let Some(hostname) = &arg.hostname {
		command.arg("--hostname").arg(hostname);
	}

	command
		.arg("--setenv")
		.arg("HOME")
		.arg(&user.home)
		.arg("--ro-bind")
		.arg(directory.host_passwd_path())
		.arg("/etc/passwd")
		.arg("--ro-bind")
		.arg(directory.host_nsswitch_path())
		.arg("/etc/nsswitch.conf")
		.arg("--ro-bind")
		.arg(&manager.artifacts_path)
		.arg(directory.guest_artifacts_path())
		.arg("--ro-bind")
		.arg(&manager.tangram_path)
		.arg(directory.guest_libexec_tangram_path())
		.arg("--bind")
		.arg(directory.host_output_path())
		.arg(directory.guest_output_path())
		.arg("--bind")
		.arg(directory.host_socket_path())
		.arg(directory.guest_socket_path());
	if arg.network && directory.host_resolv_conf_path().exists() {
		command
			.arg("--ro-bind")
			.arg(directory.host_resolv_conf_path())
			.arg("/etc/resolv.conf");
	}
	for mount in &arg.mounts {
		command
			.arg(if mount.readonly {
				"--ro-bind"
			} else {
				"--bind"
			})
			.arg(&mount.source)
			.arg(&mount.target);
	}

	command.arg(directory.guest_tangram_path());
	crate::append_run_args(&mut command, run_arg, ready_fd);
	command
		.stdin(std::process::Stdio::null())
		.stdout(std::process::Stdio::inherit())
		.stderr(std::process::Stdio::inherit())
		.kill_on_drop(true);
	command
		.spawn()
		.map_err(|source| tg::error!(!source, "failed to spawn bwrap"))
}

fn prepare_sandbox_directory(directory: &Directory) -> tg::Result<()> {
	for path in [
		directory.host_output_path(),
		directory.host_socket_path(),
		directory.host_scratch_path(),
		directory.host_tmp_path(),
		directory.host_etc_path(),
		directory.host_upper_path(),
		directory.host_work_path(),
	] {
		std::fs::create_dir_all(&path).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to create the sandbox path"),
		)?;
	}
	let permissions =
		<std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o1777);
	std::fs::set_permissions(directory.host_tmp_path(), permissions).map_err(|source| {
		tg::error!(
			!source,
			path = %directory.host_tmp_path().display(),
			"failed to set sandbox path permissions"
		)
	})?;
	Ok(())
}

fn prepare_rootfs_mountpoints(rootfs_path: &Path) -> tg::Result<()> {
	for path in [
		Path::new("/dev"),
		Path::new("/proc"),
		Path::new("/tmp"),
		Path::new("/opt/tangram/artifacts"),
		Path::new("/opt/tangram/libexec"),
		Path::new("/opt/tangram/output"),
		Path::new("/opt/tangram/socket"),
	] {
		create_guest_directory(rootfs_path, path)?;
	}
	for path in [
		Path::new("/etc/passwd"),
		Path::new("/etc/nsswitch.conf"),
		Path::new("/etc/resolv.conf"),
		Path::new("/opt/tangram/libexec/tangram"),
	] {
		create_guest_file(rootfs_path, path)?;
	}
	Ok(())
}

fn prepare_mount_targets(
	rootfs_path: &Path,
	upper_path: &Path,
	mounts: &[tg::sandbox::Mount],
) -> tg::Result<()> {
	for mount in mounts {
		let source_metadata = std::fs::metadata(&mount.source).map_err(|source| {
			tg::error!(
				!source,
				source = %mount.source.display(),
				"failed to stat the mount source"
			)
		})?;
		let target_path = map_guest_path(rootfs_path, &mount.target)?;
		if let Ok(target_metadata) = std::fs::metadata(&target_path) {
			if source_metadata.is_dir() != target_metadata.is_dir() {
				let expected = if source_metadata.is_dir() {
					"a directory"
				} else {
					"a file"
				};
				let found = if target_metadata.is_dir() {
					"a directory"
				} else {
					"a file"
				};
				return Err(tg::error!(
					path = %mount.target.display(),
					"expected mount target to be {expected}, but found {found}"
				));
			}
			continue;
		}
		if source_metadata.is_dir() {
			create_guest_directory(upper_path, &mount.target)?;
		} else {
			create_guest_file(upper_path, &mount.target)?;
		}
	}
	Ok(())
}

fn create_guest_directory(root_path: &Path, guest_path: &Path) -> tg::Result<()> {
	let path = map_guest_path(root_path, guest_path)?;
	std::fs::create_dir_all(&path).map_err(|source| {
		tg::error!(
			!source,
			path = %path.display(),
			"failed to create a guest directory"
		)
	})?;
	Ok(())
}

fn create_guest_file(root_path: &Path, guest_path: &Path) -> tg::Result<()> {
	let path = map_guest_path(root_path, guest_path)?;
	if let Ok(metadata) = std::fs::metadata(&path) {
		if metadata.is_dir() {
			return Err(tg::error!(
				path = %path.display(),
				"expected a guest file, but found a directory"
			));
		}
		return Ok(());
	}
	if let Some(parent) = path.parent() {
		std::fs::create_dir_all(parent).map_err(|source| {
			tg::error!(
				!source,
				path = %parent.display(),
				"failed to create a guest parent directory"
			)
		})?;
	}
	std::fs::OpenOptions::new()
		.create(true)
		.write(true)
		.truncate(false)
		.open(&path)
		.map_err(|source| {
			tg::error!(
				!source,
				path = %path.display(),
				"failed to create a guest file"
			)
		})?;
	Ok(())
}

fn map_guest_path(root_path: &Path, guest_path: &Path) -> tg::Result<PathBuf> {
	let suffix = guest_path.strip_prefix("/").map_err(|source| {
		tg::error!(
			!source,
			path = %guest_path.display(),
			"expected an absolute guest path"
		)
	})?;
	Ok(root_path.join(suffix))
}

fn prepare_etc_files(directory: &Directory, network: bool, user: Option<&str>) -> tg::Result<User> {
	let user = resolve_user(user)?;
	let passwd = render_passwd(&user);
	std::fs::write(directory.host_passwd_path(), passwd)
		.map_err(|source| tg::error!(!source, "failed to write /etc/passwd"))?;
	std::fs::write(
		directory.host_nsswitch_path(),
		indoc!(
			"
			passwd: files compat
			shadow: files compat
			hosts: files dns compat
		"
		),
	)
	.map_err(|source| tg::error!(!source, "failed to write /etc/nsswitch.conf"))?;
	if network {
		std::fs::copy("/etc/resolv.conf", directory.host_resolv_conf_path())
			.map_err(|source| tg::error!(!source, "failed to stage /etc/resolv.conf"))?;
	}
	Ok(user)
}

fn render_passwd(user: &User) -> String {
	let mut passwd = String::from(
		"root:!:0:0:root:/root:/bin/false\nnobody:!:65534:65534:nobody:/nonexistent:/bin/false\n",
	);
	if user.uid != 0 && user.uid != 65534 {
		writeln!(
			passwd,
			"{}:!:{}:{}:{}:{}:/bin/false",
			user.name,
			user.uid,
			user.gid,
			user.name,
			user.home.display(),
		)
		.unwrap();
	}
	passwd
}

fn resolve_user(name: Option<&str>) -> tg::Result<User> {
	let ptr = unsafe {
		if let Some(name) = name {
			let name = CString::new(OsStr::new(name).as_bytes())
				.map_err(|source| tg::error!(!source, "failed to encode the user name"))?;
			libc::getpwnam(name.as_ptr())
		} else {
			libc::getpwuid(libc::getuid())
		}
	};
	if ptr.is_null() {
		return Err(tg::error!("failed to resolve the user"));
	}
	let passwd = unsafe { &*ptr };
	let name = unsafe { CStr::from_ptr(passwd.pw_name) }
		.to_string_lossy()
		.into_owned();
	let home = unsafe { CStr::from_ptr(passwd.pw_dir) }
		.to_string_lossy()
		.into_owned();
	Ok(User {
		gid: passwd.pw_gid,
		home: PathBuf::from(home),
		name,
		uid: passwd.pw_uid,
	})
}
