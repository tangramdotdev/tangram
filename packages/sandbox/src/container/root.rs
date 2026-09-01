use {
	crate::libraries,
	serde::{Deserialize, Serialize},
	std::{
		io::Write as _,
		os::unix::ffi::OsStrExt as _,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

const VERSION_FILE_NAME: &str = ".tangram-version";
// Bump this when the generated structure changes independently of its hashed inputs.
const VERSION_SCHEMA: u64 = 1;
const ROOTFS: include_dir::Dir<'static> = include_dir::include_dir!("$OUT_DIR/rootfs");

#[derive(Clone, Debug)]
pub struct Arg {
	pub path: PathBuf,
	pub version: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Version {
	fingerprint: String,
	schema: u64,
	tangram: String,
}

pub fn create(arg: &Arg) -> tg::Result<()> {
	// Resolve the build inputs.
	let libraries = libraries::resolve()?;
	let version = version(arg, &libraries)?;
	if root_is_valid(&arg.path, &version, &libraries) {
		if let Ok(temp_path) = temporary_path(&arg.path) {
			remove_path(&temp_path).ok();
		}
		return Ok(());
	}

	// Prepare a temporary sibling so that installation stays on one filesystem.
	let parent_path = arg.path.parent().ok_or_else(|| {
		tg::error!(
			path = %arg.path.display(),
			"failed to get the sandbox directory parent"
		)
	})?;
	std::fs::create_dir_all(parent_path).map_err(|error| {
		tg::error!(
			!error,
			path = %parent_path.display(),
			"failed to create the sandbox directory parent"
		)
	})?;
	let temp_path = temporary_path(&arg.path)?;
	remove_path(&temp_path)?;

	// Build the root without modifying the installed root.
	let result = build(&temp_path, &libraries, &version);
	if let Err(error) = result {
		remove_path(&temp_path).ok();
		return Err(error);
	}

	// Install the complete root atomically.
	let result = install(&temp_path, &arg.path, parent_path);
	if result.is_err() {
		remove_path(&temp_path).ok();
	}
	result
}

fn build(path: &Path, libraries: &[libraries::Library], version: &Version) -> tg::Result<()> {
	std::fs::create_dir(path).map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to create the sandbox directory"
		)
	})?;
	let permissions = <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o755);
	ROOTFS.extract(path).map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to extract the sandbox rootfs"),
	)?;
	set_rootfs_permissions(path, &ROOTFS, &permissions)?;
	restore_rootfs_symlinks(path)?;
	create_rootfs_mountpoints(path)?;

	let lib_path = path.join("opt/tangram/lib");
	libraries::stage(&lib_path, libraries)?;
	sync_directory(path)?;
	write_version(path, version)?;
	Ok(())
}

fn version(arg: &Arg, libraries: &[libraries::Library]) -> tg::Result<Version> {
	let mut hasher = blake3::Hasher::new();
	hash_rootfs(&mut hasher, &ROOTFS);
	for library in libraries {
		hash_bytes(&mut hasher, library.name.as_bytes());
		hash_file(&mut hasher, &library.source)?;
	}
	let fingerprint = hasher.finalize().to_hex().to_string();
	let schema = VERSION_SCHEMA;
	let tangram = arg.version.clone();
	let version = Version {
		fingerprint,
		schema,
		tangram,
	};
	Ok(version)
}

fn hash_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
	hasher.update(&(bytes.len() as u64).to_le_bytes());
	hasher.update(bytes);
}

fn hash_file(hasher: &mut blake3::Hasher, path: &Path) -> tg::Result<()> {
	let mut file = std::fs::File::open(path).map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to open a sandbox input"
		)
	})?;
	let mut file_hasher = blake3::Hasher::new();
	std::io::copy(&mut file, &mut file_hasher).map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to read a sandbox input"
		)
	})?;
	hash_bytes(hasher, file_hasher.finalize().as_bytes());
	Ok(())
}

fn hash_rootfs(hasher: &mut blake3::Hasher, directory: &include_dir::Dir<'_>) {
	let mut entries = directory.entries().iter().collect::<Vec<_>>();
	entries.sort_by_key(|entry| entry.path());
	for entry in entries {
		hash_bytes(hasher, entry.path().as_os_str().as_bytes());
		match entry {
			include_dir::DirEntry::Dir(directory) => {
				hash_bytes(hasher, b"directory");
				hash_rootfs(hasher, directory);
			},
			include_dir::DirEntry::File(file) => {
				hash_bytes(hasher, b"file");
				hash_bytes(hasher, file.contents());
			},
		}
	}
}

fn install(temp_path: &Path, path: &Path, parent_path: &Path) -> tg::Result<()> {
	let exists = match std::fs::symlink_metadata(path) {
		Ok(_) => true,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
		Err(error) => {
			return Err(tg::error!(
				!error,
				path = %path.display(),
				"failed to stat the sandbox directory"
			));
		},
	};
	if exists {
		rustix::fs::renameat_with(
			rustix::fs::CWD,
			temp_path,
			rustix::fs::CWD,
			path,
			rustix::fs::RenameFlags::EXCHANGE,
		)
		.map_err(|error| {
			tg::error!(
				!error,
				from = %temp_path.display(),
				to = %path.display(),
				"failed to install the sandbox directory"
			)
		})?;
	} else {
		tangram_util::fs::rename_noreplace_sync(temp_path, path).map_err(|error| {
			tg::error!(
				!error,
				from = %temp_path.display(),
				to = %path.display(),
				"failed to install the sandbox directory"
			)
		})?;
	}
	sync_file(parent_path)?;
	if let Err(error) = remove_path(temp_path) {
		tracing::warn!(?error, path = %temp_path.display(), "failed to clean up the old sandbox directory");
	} else {
		sync_file(parent_path)?;
	}
	Ok(())
}

fn remove_path(path: &Path) -> tg::Result<()> {
	match tangram_util::fs::remove_sync(path) {
		Ok(()) => Ok(()),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(error) => Err(tg::error!(
			!error,
			path = %path.display(),
			"failed to remove the sandbox directory"
		)),
	}
}

fn root_is_valid(path: &Path, version: &Version, libraries: &[libraries::Library]) -> bool {
	let version_path = path.join(VERSION_FILE_NAME);
	let Ok(file) = std::fs::File::open(version_path) else {
		return false;
	};
	let Ok(found_version) = serde_json::from_reader::<_, Version>(file) else {
		return false;
	};
	if found_version != *version {
		return false;
	}
	let tangram_path = path.join("opt/tangram/bin/tangram");
	if !tangram_path.is_file() {
		return false;
	}
	for library in libraries {
		if !path.join("opt/tangram/lib").join(&library.name).is_file() {
			return false;
		}
	}
	true
}

fn sync_directory(path: &Path) -> tg::Result<()> {
	let entries = std::fs::read_dir(path).map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to read the sandbox directory"
		)
	})?;
	for entry in entries {
		let path = entry
			.map_err(|error| tg::error!(!error, "failed to read a sandbox directory entry"))?
			.path();
		let metadata = std::fs::symlink_metadata(&path).map_err(|error| {
			tg::error!(
				!error,
				path = %path.display(),
				"failed to stat a sandbox directory entry"
			)
		})?;
		if metadata.is_dir() {
			sync_directory(&path)?;
		} else if metadata.is_file() {
			sync_file(&path)?;
		}
	}
	sync_file(path)?;
	Ok(())
}

fn sync_file(path: &Path) -> tg::Result<()> {
	let file = std::fs::File::open(path).map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to open a sandbox path"
		)
	})?;
	file.sync_all().map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to sync a sandbox path"
		)
	})?;
	Ok(())
}

fn temporary_path(path: &Path) -> tg::Result<PathBuf> {
	let name = path.file_name().ok_or_else(|| {
		tg::error!(
			path = %path.display(),
			"failed to get the sandbox directory name"
		)
	})?;
	let mut temp_name = name.to_owned();
	temp_name.push(".tmp");
	let path = path.with_file_name(temp_name);
	Ok(path)
}

fn write_version(path: &Path, version: &Version) -> tg::Result<()> {
	let version_path = path.join(VERSION_FILE_NAME);
	let mut file = std::fs::File::create(&version_path).map_err(|error| {
		tg::error!(
			!error,
			path = %version_path.display(),
			"failed to create the sandbox version file"
		)
	})?;
	serde_json::to_writer(&mut file, version).map_err(|error| {
		tg::error!(
			!error,
			path = %version_path.display(),
			"failed to write the sandbox version file"
		)
	})?;
	writeln!(file).map_err(|error| {
		tg::error!(
			!error,
			path = %version_path.display(),
			"failed to write the sandbox version file"
		)
	})?;
	file.sync_all().map_err(|error| {
		tg::error!(
			!error,
			path = %version_path.display(),
			"failed to sync the sandbox version file"
		)
	})?;
	sync_file(path)?;
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
		"/opt/tangram/store",
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
