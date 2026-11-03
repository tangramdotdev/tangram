use {
	crate::temp,
	std::{
		ffi::OsStr,
		io,
		os::unix::fs::PermissionsExt as _,
		path::{Path, PathBuf},
		process::{Command, Stdio},
	},
};

/// The size of the file system's image in bytes.
const SIZE: u64 = 2 * 1024 * 1024 * 1024;

/// A loop-mounted ext4 file system with project quotas, which is unmounted and deleted when it is dropped.
pub struct Mount {
	_lock: std::fs::File,
	path: PathBuf,
}

/// Determine whether passwordless sudo is available.
#[must_use]
pub fn available() -> bool {
	Command::new("sudo")
		.args(["-n", "true"])
		.stdin(Stdio::null())
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.status()
		.is_ok_and(|status| status.success())
}

/// Get the path of the directory that holds the mounts. It is on disk and traversable by every user, so that a server running as root can execute a binary in it from a sandbox.
#[must_use]
pub fn base_path() -> PathBuf {
	// SAFETY: getuid has no preconditions.
	let uid = unsafe { libc::getuid() };
	PathBuf::from(format!("/var/tmp/tangram_test_quota_{uid}"))
}

/// Remove a directory on a mount, including the files that a server running as root created in it.
pub fn remove(path: &Path) -> io::Result<()> {
	sudo(&[OsStr::new("rm"), OsStr::new("-rf"), path.as_os_str()])?;
	Ok(())
}

impl Mount {
	/// Clean up the mounts of exited runs, then create a mount. Return `None` if the platform is not Linux or passwordless sudo is not available.
	pub fn new() -> io::Result<Option<Self>> {
		if !cfg!(target_os = "linux") || !available() {
			return Ok(None);
		}

		// Lock the base directory, so that a run's cleanup never sees another run's mount before its lock is held.
		let base_path = base_path();
		std::fs::create_dir_all(&base_path)?;
		std::fs::set_permissions(&base_path, std::fs::Permissions::from_mode(0o755))?;
		let base_lock = std::fs::File::create(base_path.join("lock"))?;
		base_lock.lock()?;

		// Clean up the mounts of exited runs.
		cleanup(&base_path)?;

		// Create the run's directory and take its lock.
		let path = temp::Temp::with_parent(&base_path)?.into_path();
		std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))?;
		let lock = std::fs::File::create(path.join("lock"))?;
		lock.lock()?;
		drop(base_lock);
		let mount = Self { _lock: lock, path };

		// Create the image and its file system.
		let image_path = mount.image_path();
		std::fs::File::create(&image_path)?.set_len(SIZE)?;
		let output = Command::new("mkfs.ext4")
			.args([
				"-q",
				"-F",
				"-O",
				"quota,project",
				"-E",
				"quotatype=prjquota",
			])
			.arg(&image_path)
			.stdin(Stdio::null())
			.output()?;
		if !output.status.success() {
			let stderr = String::from_utf8_lossy(&output.stderr);
			return Err(io::Error::other(format!(
				"failed to create the file system: {}",
				stderr.trim()
			)));
		}

		// Mount the file system and give it to the current user.
		let mount_path = mount.mount_path();
		std::fs::create_dir(&mount_path)?;
		sudo(&[
			OsStr::new("mount"),
			OsStr::new("-o"),
			OsStr::new("loop,prjquota"),
			image_path.as_os_str(),
			mount_path.as_os_str(),
		])?;
		// SAFETY: getuid and getgid have no preconditions.
		let owner = unsafe { format!("{}:{}", libc::getuid(), libc::getgid()) };
		sudo(&[
			OsStr::new("chown"),
			OsStr::new(&owner),
			mount_path.as_os_str(),
		])?;
		remove(&mount_path.join("lost+found"))?;

		Ok(Some(mount))
	}

	/// Get the path of the run's directory.
	#[must_use]
	pub fn path(&self) -> &Path {
		&self.path
	}

	/// Get the path at which the file system is mounted.
	#[must_use]
	pub fn mount_path(&self) -> PathBuf {
		self.path.join("mount")
	}

	#[must_use]
	fn image_path(&self) -> PathBuf {
		self.path.join("image")
	}
}

impl Drop for Mount {
	fn drop(&mut self) {
		destroy(&self.path).ok();
	}
}

/// Unmount and delete the mounts whose runs have exited.
fn cleanup(base_path: &Path) -> io::Result<()> {
	for entry in std::fs::read_dir(base_path)? {
		let entry = entry?;
		if !entry.file_type()?.is_dir() {
			continue;
		}
		let path = entry.path();
		let alive = std::fs::File::open(path.join("lock"))
			.is_ok_and(|file| matches!(file.try_lock(), Err(std::fs::TryLockError::WouldBlock)));
		if alive {
			continue;
		}
		destroy(&path)?;
	}
	Ok(())
}

/// Unmount a run's file system and delete its directory.
fn destroy(path: &Path) -> io::Result<()> {
	let mount_path = path.join("mount");
	let mounted = Command::new("mountpoint")
		.arg("-q")
		.arg(&mount_path)
		.stdin(Stdio::null())
		.status()
		.is_ok_and(|status| status.success());
	if mounted && sudo(&[OsStr::new("umount"), mount_path.as_os_str()]).is_err() {
		sudo(&[
			OsStr::new("umount"),
			OsStr::new("-l"),
			mount_path.as_os_str(),
		])?;
	}
	remove(path)?;
	Ok(())
}

/// Run a command with passwordless sudo.
fn sudo(args: &[&OsStr]) -> io::Result<()> {
	let output = Command::new("sudo")
		.arg("-n")
		.args(args)
		.stdin(Stdio::null())
		.output()?;
	if !output.status.success() {
		let command = args
			.iter()
			.map(|arg| arg.to_string_lossy())
			.collect::<Vec<_>>()
			.join(" ");
		let stderr = String::from_utf8_lossy(&output.stderr);
		return Err(io::Error::other(format!(
			"failed to run sudo {command}: {}",
			stderr.trim()
		)));
	}
	Ok(())
}
