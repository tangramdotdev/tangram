use {
	crate::libraries,
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

const ROOTFS: include_dir::Dir<'static> = include_dir::include_dir!("$OUT_DIR/rootfs");

#[derive(Clone, Debug)]
pub struct Arg {
	pub path: PathBuf,
	pub tangram_path: PathBuf,
}

pub fn create(arg: &Arg) -> tg::Result<()> {
	if arg.path.exists() {
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

	let libraries = libraries::resolve(&arg.tangram_path)?;
	let lib_path = arg.path.join("opt/tangram/lib");
	libraries::stage(&lib_path, &libraries)?;
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
