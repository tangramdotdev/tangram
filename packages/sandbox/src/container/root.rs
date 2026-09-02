use {
	crate::libraries,
	std::{
		path::{Path, PathBuf},
		time::SystemTime,
	},
	tangram_client::prelude::*,
};

const BUILD_ATTEMPTS: usize = 3;
const ROOTFS: include_dir::Dir<'static> = include_dir::include_dir!("$OUT_DIR/rootfs");

#[derive(Clone, Debug)]
pub struct Arg {
	pub path: PathBuf,
	pub tangram_path: PathBuf,
	pub vm: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InputTimes {
	libraries: Vec<SystemTime>,
	tangram: SystemTime,
}

pub fn create(arg: &Arg) -> tg::Result<()> {
	// Resolve the build inputs.
	let libraries = libraries::resolve()?;
	let mut input_times = input_times(&arg.tangram_path, &libraries)?;
	if root_is_current(&arg.path, input_times.latest(), arg.vm)? {
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

	for _ in 0..BUILD_ATTEMPTS {
		// Build the root without modifying the installed root.
		remove_path(&temp_path)?;
		let result = build(&temp_path, &arg.tangram_path, &libraries, arg.vm);
		if let Err(error) = result {
			remove_path(&temp_path).ok();
			return Err(error);
		}

		// Retry if an input changed while the root was being built.
		let next_input_times = match input_times(&arg.tangram_path, &libraries) {
			Ok(input_times) => input_times,
			Err(error) => {
				remove_path(&temp_path).ok();
				return Err(error);
			},
		};
		if input_times != next_input_times {
			input_times = next_input_times;
			continue;
		}

		// Install the complete root atomically.
		let result = install(&temp_path, &arg.path, parent_path);
		if result.is_err() {
			remove_path(&temp_path).ok();
		}
		return result;
	}
	remove_path(&temp_path).ok();
	let error = tg::error!(
		attempts = BUILD_ATTEMPTS,
		"the sandbox inputs did not stabilize while building the root"
	);
	Err(error)
}

impl InputTimes {
	#[must_use]
	fn latest(&self) -> SystemTime {
		self.libraries
			.iter()
			.copied()
			.fold(self.tangram, std::cmp::max)
	}
}

fn build(
	path: &Path,
	tangram_path: &Path,
	libraries: &[libraries::Library],
	vm: bool,
) -> tg::Result<()> {
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
	if vm {
		stage_tangram(path, tangram_path)?;
	}

	let lib_path = path.join("opt/tangram/lib");
	libraries::stage(&lib_path, libraries)?;
	tangram_util::fs::sync_recursive_sync(path).map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to sync the sandbox directory"
		)
	})?;
	let modified = SystemTime::now();
	tangram_util::fs::set_modified_sync(path, modified).map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to set the sandbox directory modification time"
		)
	})?;
	Ok(())
}

fn input_times(tangram_path: &Path, libraries: &[libraries::Library]) -> tg::Result<InputTimes> {
	let metadata = std::fs::metadata(tangram_path).map_err(|error| {
		tg::error!(
			!error,
			path = %tangram_path.display(),
			"failed to stat a sandbox input"
		)
	})?;
	let tangram = metadata.modified().map_err(|error| {
		tg::error!(
			!error,
			path = %tangram_path.display(),
			"failed to get a sandbox input modification time"
		)
	})?;
	let mut library_times = Vec::with_capacity(libraries.len());
	for library in libraries {
		let metadata = std::fs::metadata(&library.source).map_err(|error| {
			tg::error!(
				!error,
				path = %library.source.display(),
				"failed to stat a sandbox input"
			)
		})?;
		let modified = metadata.modified().map_err(|error| {
			tg::error!(
				!error,
				path = %library.source.display(),
				"failed to get a sandbox input modification time"
			)
		})?;
		library_times.push(modified);
	}
	let libraries = library_times;
	let input_times = InputTimes { libraries, tangram };

	Ok(input_times)
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
		tangram_util::fs::rename_exchange_sync(temp_path, path).map_err(|error| {
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

fn root_is_current(path: &Path, input_modified: SystemTime, vm: bool) -> tg::Result<bool> {
	let metadata = match std::fs::symlink_metadata(path) {
		Ok(metadata) => metadata,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
		Err(error) => {
			return Err(tg::error!(
				!error,
				path = %path.display(),
				"failed to stat the sandbox directory"
			));
		},
	};
	if !metadata.is_dir() {
		return Ok(false);
	}
	if vm {
		let tangram_path = path.join("opt/tangram/libexec/tangram");
		let metadata = match std::fs::metadata(&tangram_path) {
			Ok(metadata) => metadata,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
			Err(error) => {
				return Err(tg::error!(
					!error,
					path = %tangram_path.display(),
					"failed to stat the staged tangram executable"
				));
			},
		};
		if !metadata.is_file() || metadata.len() == 0 {
			return Ok(false);
		}
	}
	let modified = metadata.modified().map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to get the sandbox directory modification time"
		)
	})?;
	let current = modified > input_modified;
	Ok(current)
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

fn restore_rootfs_symlinks(rootfs_path: &Path) -> tg::Result<()> {
	let lib64_path = rootfs_path.join("lib64");
	std::fs::remove_file(&lib64_path).ok();
	std::fs::remove_dir_all(&lib64_path).ok();
	std::os::unix::fs::symlink("/opt/tangram/lib", &lib64_path)
		.map_err(|error| tg::error!(!error, "failed to restore the lib64 symlink"))?;

	let tg_path = rootfs_path.join("opt/tangram/bin/tg");
	std::fs::remove_file(&tg_path).ok();
	std::os::unix::fs::symlink("tangram", &tg_path)
		.map_err(|error| tg::error!(!error, "failed to restore the tg symlink"))?;

	let usr_path = rootfs_path.join("usr");
	std::fs::create_dir_all(&usr_path).map_err(
		|error| tg::error!(!error, path = %usr_path.display(), "failed to create the usr directory"),
	)?;
	let usr_lib_path = usr_path.join("lib");
	std::fs::remove_file(&usr_lib_path).ok();
	std::fs::remove_dir_all(&usr_lib_path).ok();
	std::os::unix::fs::symlink("/opt/tangram/lib", &usr_lib_path)
		.map_err(|error| tg::error!(!error, "failed to restore the usr lib symlink"))?;

	Ok(())
}

fn stage_tangram(rootfs_path: &Path, tangram_path: &Path) -> tg::Result<()> {
	let path = rootfs_path.join("opt/tangram/libexec/tangram");
	std::fs::copy(tangram_path, &path).map_err(|error| {
		tg::error!(
			!error,
			from = %tangram_path.display(),
			to = %path.display(),
			"failed to stage the tangram executable"
		)
	})?;
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
