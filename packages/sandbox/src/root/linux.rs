use {
	super::Arg,
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

const ROOTFS: include_dir::Dir<'static> = include_dir::include_dir!("$OUT_DIR/rootfs");

pub fn create_runtime_libraries(arg: &Arg) -> tg::Result<()> {
	// Skip the rebuild when the rootfs already exists, plus the squashfs
	// image when one is requested. Recreate by deleting them externally.
	let path_exists = arg.path.exists();
	let image_exists = arg.image_path.as_ref().is_none_or(|p| p.exists());
	if path_exists && image_exists {
		return Ok(());
	}

	std::fs::remove_dir_all(&arg.path).ok();
	std::fs::create_dir_all(&arg.path)
		.map_err(|error| tg::error!(!error, "failed to create the sandbox directory"))?;
	let permissions = <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o755);
	ROOTFS.extract(&arg.path).map_err(
		|error| tg::error!(!error, path = %arg.path.display(), "failed to extract the sandbox rootfs"),
	)?;
	set_rootfs_permissions(&arg.path, &ROOTFS, &permissions)?;
	restore_rootfs_symlinks(&arg.path)?;
	create_rootfs_mountpoints(&arg.path)?;

	let lib_path = arg.path.join("opt/tangram/lib");
	let output = std::process::Command::new("ldd")
		.arg(&arg.tangram_path)
		.output()
		.map_err(|error| {
			if error.kind() == std::io::ErrorKind::NotFound {
				tg::error!(
					"failed to create the sandbox rootfs: could not execute `ldd`; install `ldd` on this Linux host"
				)
			} else {
				tg::error!(
					!error,
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
		.map_err(|error| tg::error!(!error, "failed to parse the `ldd` output"))?;
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
		let source = std::fs::canonicalize(&dependency_path).map_err(|error| {
			tg::error!(
				!error,
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
		std::fs::set_permissions(&target, permissions.clone()).map_err(|error| {
			tg::error!(
				!error,
				path = %target.display(),
				"failed to set sandbox file permissions"
			)
		})?;
	}

	if let Some(image_path) = &arg.image_path {
		build_rootfs_image(&arg.path, &arg.tangram_path, image_path)?;
	}

	Ok(())
}

fn build_rootfs_image(
	rootfs_path: &Path,
	tangram_path: &Path,
	image_path: &Path,
) -> tg::Result<()> {
	// Stage the running tangram binary into the libexec slot so the
	// /opt/tangram/bin/tangram wrapper has something to exec inside the VM.
	let libexec_path = rootfs_path.join("opt/tangram/libexec/tangram");
	std::fs::copy(tangram_path, &libexec_path).map_err(|error| {
		tg::error!(
			!error,
			src = %tangram_path.display(),
			dst = %libexec_path.display(),
			"failed to stage the tangram binary into the rootfs",
		)
	})?;

	if let Some(parent) = image_path.parent() {
		std::fs::create_dir_all(parent).map_err(|error| {
			tg::error!(!error, path = %parent.display(), "failed to create the rootfs image parent directory")
		})?;
	}
	std::fs::remove_file(image_path).ok();

	let status = std::process::Command::new("mksquashfs")
		.arg(rootfs_path)
		.arg(image_path)
		.arg("-comp")
		.arg("zstd")
		.arg("-all-root")
		.arg("-noappend")
		.arg("-quiet")
		.status()
		.map_err(|error| tg::error!(!error, "failed to invoke mksquashfs"))?;
	if !status.success() {
		return Err(tg::error!(%status, "mksquashfs failed"));
	}
	Ok(())
}

fn restore_rootfs_symlinks(rootfs_path: &Path) -> tg::Result<()> {
	let tg_path = rootfs_path.join("opt/tangram/bin/tg");
	std::fs::remove_file(&tg_path).ok();
	std::os::unix::fs::symlink("tangram", &tg_path)
		.map_err(|error| tg::error!(!error, "failed to restore the tg symlink"))?;
	Ok(())
}

fn set_rootfs_permissions(
	rootfs_path: &Path,
	directory: &include_dir::Dir<'_>,
	permissions: &std::fs::Permissions,
) -> tg::Result<()> {
	for entry in directory.entries() {
		match entry {
			include_dir::DirEntry::Dir(directory) => {
				set_rootfs_permissions(rootfs_path, directory, permissions)?;
			},
			include_dir::DirEntry::File(file) => {
				let path = rootfs_path.join(file.path());
				std::fs::set_permissions(&path, permissions.clone()).map_err(|error| {
					tg::error!(
						!error,
						path = %path.display(),
						"failed to set sandbox file permissions"
					)
				})?;
			},
		}
	}
	Ok(())
}

fn create_rootfs_mountpoints(rootfs_path: &Path) -> tg::Result<()> {
	for path in [
		"/dev",
		"/dev/pts",
		"/mnt",
		"/mnt/host",
		"/mnt/root",
		"/proc",
		"/run",
		"/run/vmm",
		"/snapshot",
		"/sys",
		"/opt/tangram",
		"/tmp",
		"/opt/tangram/artifacts",
		"/opt/tangram/libexec",
		"/opt/tangram/output",
	] {
		create_guest_directory(rootfs_path, Path::new(path))?;
	}
	for path in [
		"/socket",
		"/etc/passwd",
		"/etc/nsswitch.conf",
		"/etc/resolv.conf",
		"/opt/tangram/libexec/tangram",
		"/opt/tangram/socket",
	] {
		create_guest_file(rootfs_path, Path::new(path))?;
	}
	Ok(())
}

#[allow(dead_code)]
pub(crate) fn ensure_mount_target(
	rootfs_path: &Path,
	upper_path: &Path,
	mount: &tg::sandbox::Mount,
) -> tg::Result<()> {
	let source_metadata = std::fs::metadata(&mount.source).map_err(|error| {
		tg::error!(
			!error,
			error = %mount.source.display(),
			"failed to stat the mount error"
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
		return Ok(());
	}
	if source_metadata.is_dir() {
		create_guest_directory(upper_path, &mount.target)?;
	} else {
		create_guest_file(upper_path, &mount.target)?;
	}
	Ok(())
}

fn create_guest_directory(root_path: &Path, guest_path: &Path) -> tg::Result<()> {
	let path = map_guest_path(root_path, guest_path)?;
	std::fs::create_dir_all(&path).map_err(|error| {
		tg::error!(
			!error,
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
		std::fs::create_dir_all(parent).map_err(|error| {
			tg::error!(
				!error,
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
		.map_err(|error| {
			tg::error!(
				!error,
				path = %path.display(),
				"failed to create a guest file"
			)
		})?;
	Ok(())
}

fn map_guest_path(root_path: &Path, guest_path: &Path) -> tg::Result<PathBuf> {
	let suffix = guest_path.strip_prefix("/").map_err(|error| {
		tg::error!(
			!error,
			path = %guest_path.display(),
			"expected an absolute guest path"
		)
	})?;
	Ok(root_path.join(suffix))
}
