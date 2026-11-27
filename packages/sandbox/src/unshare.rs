use {
	rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags, Stat},
	std::{
		ffi::{CStr, CString},
		io::{Read as _, Write as _},
		os::{fd::OwnedFd, unix::fs::MetadataExt as _},
		path::{Path, PathBuf},
		process::ExitCode,
	},
	tangram_client::prelude::*,
	tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
};

/// An operation of the helper, which runs in a user namespace that maps a rootless sandbox's identities, in which the runner is root and holds the capabilities over every file that the sandbox's identities own.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
	/// Give the runner each path, without its entries.
	Own,

	/// Check that the namespace maps a subordinate identity, by giving a new file in the directory at the path to it and removing the file.
	Probe,

	/// Give the runner every file under the path that another identity owns.
	Reclaim,

	/// Remove the path.
	Remove,
}

/// A directory of a walk, with the names of its directories that the walk has yet to enter and its name in its parent.
struct Frame {
	directories: Vec<CString>,
	directory: OwnedFd,
	name: CString,
}

/// Give the runner every file under a path that a sandbox's workload gave to another of the sandbox's identities, so that the runner can read, move, and remove the files.
pub async fn reclaim(ids: &crate::Ids, tangram_path: &Path, path: &Path) -> tg::Result<()> {
	let crate::Ids::Rootless(maps) = ids else {
		return Ok(());
	};

	// Skip the helper when the runner already owns every file.
	let owned = tokio::task::spawn_blocking({
		let path = path.to_owned();
		move || owned(&path)
	})
	.await
	.map_err(|error| tg::error!(!error, "the ownership task panicked"))?;
	if owned {
		return Ok(());
	}

	run(tangram_path, maps, Operation::Reclaim, &[path.to_owned()]).await?;

	Ok(())
}

/// Give the runner the directories that it prepares in a sandbox's directory at a root path, without their entries, which the workload of an earlier boot of the sandbox may have given to other identities of the sandbox, so that the runner can prepare them again.
pub async fn reclaim_directories(
	ids: &crate::Ids,
	tangram_path: &Path,
	root_path: &Path,
) -> tg::Result<()> {
	let crate::Ids::Rootless(maps) = ids else {
		return Ok(());
	};

	// Find the directories that the runner does not own or cannot reach.
	let upper = crate::Sandbox::host_upper_path_from_root(root_path);
	let paths = [
		crate::Sandbox::host_output_path_from_root(root_path),
		crate::Sandbox::host_scratch_path_from_root(root_path),
		crate::Sandbox::host_tmp_path_from_root(root_path),
		upper.join("opt"),
		upper.join("opt/tangram"),
		upper,
		crate::Sandbox::host_work_path_from_root(root_path),
	];
	// SAFETY: These functions have no preconditions.
	let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
	let paths = paths
		.into_iter()
		.filter(|path| match std::fs::symlink_metadata(path) {
			Ok(metadata) => metadata.uid() != uid || metadata.gid() != gid,
			Err(error) => error.kind() != std::io::ErrorKind::NotFound,
		})
		.collect::<Vec<_>>();
	if paths.is_empty() {
		return Ok(());
	}

	run(tangram_path, maps, Operation::Own, &paths).await?;

	Ok(())
}

/// Remove a path that a sandbox's workload may have written under any of the sandbox's identities, and succeed if the path does not exist.
pub async fn remove(ids: &crate::Ids, tangram_path: &Path, path: &Path) -> tg::Result<()> {
	// Remove the path as the runner.
	let error = match tangram_util::fs::remove(path).await {
		Ok(()) => return Ok(()),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
		Err(error) => error,
	};

	// Remove the rest in the namespace of a rootless sandbox, if the runner lacks the permission to remove a file that another of its identities owns.
	match ids {
		crate::Ids::Rootless(maps) if error.kind() == std::io::ErrorKind::PermissionDenied => {
			run(tangram_path, maps, Operation::Remove, &[path.to_owned()]).await?;
			Ok(())
		},
		crate::Ids::Mapped(_) | crate::Ids::Rootless(_) | crate::Ids::Single => {
			Err(tg::error!(!error, path = %path.display(), "failed to remove the sandbox path"))
		},
	}
}

/// Check that the runner can install the rootless maps in the helper's namespace with the helper at the Tangram path, and give a file to a subordinate identity and remove it there, as it does to reclaim and remove a sandbox's files, using a temporary directory in the directory at the path.
pub async fn probe(tangram_path: &Path, path: &Path, maps: &crate::IdMaps) -> tg::Result<()> {
	let temp = tangram_util::fs::Temp::new_in(path)
		.map_err(|error| tg::error!(!error, "failed to create the probe directory"))?;
	tokio::fs::create_dir(temp.path())
		.await
		.map_err(|error| tg::error!(!error, "failed to create the probe directory"))?;
	let result = run(
		tangram_path,
		maps,
		Operation::Probe,
		&[temp.path().to_owned()],
	)
	.await;
	tokio::fs::remove_dir(temp.path())
		.await
		.map_err(|error| tg::error!(!error, "failed to remove the probe directory"))?;
	result?;
	Ok(())
}

/// Run the helper in a new user namespace once the parent installs the maps, which the helper waits for on stdin after it signals on stdout that it created the namespace.
pub fn main(operation: Operation, paths: &[PathBuf]) -> tg::Result<ExitCode> {
	// Create the user namespace, keeping the mapping files writable for the parent.
	crate::linux::unshare(libc::CLONE_NEWUSER, "failed to unshare the user namespace")?;
	crate::util::set_dumpable(true)?;

	// Wait for the parent to install the maps.
	std::io::stdout()
		.write_all(&[0])
		.and_then(|()| std::io::stdout().flush())
		.map_err(|error| tg::error!(!error, "failed to signal the user namespace"))?;
	let mut byte = [0];
	let count = std::io::stdin()
		.read(&mut byte)
		.map_err(|error| tg::error!(!error, "failed to wait for the user namespace maps"))?;
	if count == 0 {
		return Err(tg::error!("the parent did not map the user namespace"));
	}

	// Perform the operation on each path as the namespace's root.
	for path in paths {
		let path = CString::new(path.as_os_str().as_encoded_bytes())
			.map_err(|error| tg::error!(!error, "the path contains a null byte"))?;
		match operation {
			Operation::Own => own_path(&path)?,
			Operation::Probe => probe_directory(&path)?,
			Operation::Reclaim => reclaim_path(&path)?,
			Operation::Remove => remove_path(&path)?,
		}
	}

	Ok(ExitCode::SUCCESS)
}

impl Operation {
	#[must_use]
	pub fn name(self) -> &'static str {
		match self {
			Self::Own => "own",
			Self::Probe => "probe",
			Self::Reclaim => "reclaim",
			Self::Remove => "remove",
		}
	}
}

/// Run the helper for an operation on paths in a namespace with the rootless maps, in which the runner is root.
async fn run(
	tangram_path: &Path,
	maps: &crate::IdMaps,
	operation: Operation,
	paths: &[PathBuf],
) -> tg::Result<()> {
	// Spawn the helper.
	let mut child = tokio::process::Command::new(tangram_path)
		.args(["sandbox", "unshare", operation.name(), "--"])
		.args(paths)
		.kill_on_drop(true)
		.stdin(std::process::Stdio::piped())
		.stdout(std::process::Stdio::piped())
		.stderr(std::process::Stdio::piped())
		.spawn()
		.map_err(|error| tg::error!(!error, "failed to spawn the user namespace helper"))?;
	let pid = child
		.id()
		.and_then(|pid| libc::pid_t::try_from(pid).ok())
		.ok_or_else(|| tg::error!("failed to get the user namespace helper's pid"))?;
	let mut stdin = child.stdin.take().unwrap();
	let mut stdout = child.stdout.take().unwrap();

	// Install the maps once the helper has created its namespace, and then release the helper.
	let result =
		async {
			let mut byte = [0];
			stdout.read_exact(&mut byte).await.map_err(|error| {
				tg::error!(!error, "failed to wait for the user namespace helper")
			})?;
			crate::ids::write_rootless(pid, maps, 0, 0).await?;
			stdin.write_all(&[0]).await.map_err(|error| {
				tg::error!(!error, "failed to release the user namespace helper")
			})?;
			Ok::<_, tg::Error>(())
		}
		.await;
	drop(stdin);

	// Wait for the helper.
	let output = child
		.wait_with_output()
		.await
		.map_err(|error| tg::error!(!error, "failed to wait for the user namespace helper"))?;
	let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
	result
		.map_err(|error| tg::error!(!error, %stderr, "failed to map the user namespace helper"))?;
	if !output.status.success() {
		return Err(tg::error!(
			%stderr,
			operation = %operation.name(),
			paths = ?paths,
			status = %output.status,
			"the user namespace helper failed"
		));
	}

	Ok(())
}

/// Determine whether the runner owns every file under a path and can read every directory, or the path does not exist.
fn owned(path: &Path) -> bool {
	// SAFETY: These functions have no preconditions.
	let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
	let mut pending = vec![path.to_owned()];
	while let Some(path) = pending.pop() {
		let metadata = match std::fs::symlink_metadata(&path) {
			Ok(metadata) => metadata,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
			Err(_) => return false,
		};
		if metadata.uid() != uid || metadata.gid() != gid {
			return false;
		}
		if metadata.is_dir() {
			let Ok(entries) = std::fs::read_dir(&path) else {
				return false;
			};
			for entry in entries {
				let Ok(entry) = entry else {
					return false;
				};
				pending.push(entry.path());
			}
		}
	}
	true
}

fn own_path(path: &CStr) -> tg::Result<()> {
	// SAFETY: These functions have no preconditions.
	let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
	let stat = match rustix::fs::statat(rustix::fs::CWD, path, AtFlags::SYMLINK_NOFOLLOW) {
		Ok(stat) => stat,
		Err(rustix::io::Errno::NOENT) => return Ok(()),
		Err(error) => return Err(tg::error!(!error, "failed to stat the path")),
	};
	chown(rustix::fs::CWD, path, &stat, uid, gid)?;
	Ok(())
}

fn probe_directory(path: &CStr) -> tg::Result<()> {
	// Create a file in the directory.
	let directory = open_directory(rustix::fs::CWD, path)
		.map_err(|error| tg::error!(!error, "failed to open the probe directory"))?;
	let name = c"probe";
	let file = rustix::fs::openat(
		&directory,
		name,
		OFlags::CREATE | OFlags::EXCL | OFlags::WRONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
		Mode::from_raw_mode(0o600),
	)
	.map_err(|error| tg::error!(!error, "failed to create the probe file"))?;

	// Give the file to the first subordinate identity, and then remove it.
	let result = rustix::fs::fchown(
		&file,
		Some(rustix::fs::Uid::from_raw(1)),
		Some(rustix::fs::Gid::from_raw(1)),
	)
	.map_err(|error| {
		tg::error!(
			!error,
			"failed to give the probe file to a subordinate identity"
		)
	});
	drop(file);
	rustix::fs::unlinkat(&directory, name, AtFlags::empty())
		.map_err(|error| tg::error!(!error, "failed to remove the probe file"))?;
	result?;

	Ok(())
}

fn reclaim_path(path: &CStr) -> tg::Result<()> {
	// SAFETY: These functions have no preconditions.
	let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };

	// Reclaim the path itself.
	let stat = match rustix::fs::statat(rustix::fs::CWD, path, AtFlags::SYMLINK_NOFOLLOW) {
		Ok(stat) => stat,
		Err(rustix::io::Errno::NOENT) => return Ok(()),
		Err(error) => return Err(tg::error!(!error, "failed to stat the path")),
	};
	chown(rustix::fs::CWD, path, &stat, uid, gid)?;
	if FileType::from_raw_mode(stat.st_mode) != FileType::Directory {
		return Ok(());
	}

	// Reclaim the entries depth first through directory descriptors, so that no symlink redirects the walk, and without entering another filesystem.
	let device = stat.st_dev;
	let directory = open_directory(rustix::fs::CWD, path)
		.map_err(|error| tg::error!(!error, "failed to open the directory"))?;
	let directories = reclaim_entries(&directory, device, uid, gid)?;
	let mut stack = vec![Frame {
		directories,
		directory,
		name: CString::default(),
	}];
	while let Some(frame) = stack.last_mut() {
		let Some(name) = frame.directories.pop() else {
			stack.pop();
			continue;
		};
		let directory = open_directory(&frame.directory, &name)
			.map_err(|error| tg::error!(!error, "failed to open a directory"))?;
		let directories = reclaim_entries(&directory, device, uid, gid)?;
		stack.push(Frame {
			directories,
			directory,
			name,
		});
	}

	Ok(())
}

fn remove_path(path: &CStr) -> tg::Result<()> {
	// Remove a path that is not a directory.
	let stat = match rustix::fs::statat(rustix::fs::CWD, path, AtFlags::SYMLINK_NOFOLLOW) {
		Ok(stat) => stat,
		Err(rustix::io::Errno::NOENT) => return Ok(()),
		Err(error) => return Err(tg::error!(!error, "failed to stat the path")),
	};
	if FileType::from_raw_mode(stat.st_mode) != FileType::Directory {
		rustix::fs::unlinkat(rustix::fs::CWD, path, AtFlags::empty())
			.map_err(|error| tg::error!(!error, "failed to remove the path"))?;
		return Ok(());
	}

	// Remove the entries depth first through directory descriptors, so that no symlink redirects the walk, and without entering another filesystem, whose mount point then fails to be removed.
	let device = stat.st_dev;
	let directory = open_directory(rustix::fs::CWD, path)
		.map_err(|error| tg::error!(!error, "failed to open the directory"))?;
	let directories = remove_entries(&directory, device)?;
	let mut stack = vec![Frame {
		directories,
		directory,
		name: CString::default(),
	}];
	while let Some(frame) = stack.last_mut() {
		// Enter the next directory.
		if let Some(name) = frame.directories.pop() {
			let directory = open_directory(&frame.directory, &name)
				.map_err(|error| tg::error!(!error, "failed to open a directory"))?;
			let directories = remove_entries(&directory, device)?;
			stack.push(Frame {
				directories,
				directory,
				name,
			});
			continue;
		}

		// Remove the directory once its entries are removed.
		let frame = stack.pop().unwrap();
		drop(frame.directory);
		if let Some(parent) = stack.last() {
			rustix::fs::unlinkat(&parent.directory, frame.name.as_c_str(), AtFlags::REMOVEDIR)
				.map_err(|error| tg::error!(!error, "failed to remove a directory"))?;
		}
	}
	rustix::fs::unlinkat(rustix::fs::CWD, path, AtFlags::REMOVEDIR)
		.map_err(|error| tg::error!(!error, "failed to remove the directory"))?;

	Ok(())
}

/// Give the runner the entries of a directory, and return the names of its directories on the device.
fn reclaim_entries(
	directory: &OwnedFd,
	device: u64,
	uid: u32,
	gid: u32,
) -> tg::Result<Vec<CString>> {
	let mut directories = Vec::new();
	let entries = Dir::read_from(directory)
		.map_err(|error| tg::error!(!error, "failed to read the directory"))?;
	for entry in entries {
		let entry = entry.map_err(|error| tg::error!(!error, "failed to read the directory"))?;
		let name = entry.file_name();
		if name == c"." || name == c".." {
			continue;
		}
		let stat = rustix::fs::statat(directory, name, AtFlags::SYMLINK_NOFOLLOW)
			.map_err(|error| tg::error!(!error, "failed to stat a directory entry"))?;
		chown(directory, name, &stat, uid, gid)?;
		if FileType::from_raw_mode(stat.st_mode) == FileType::Directory && stat.st_dev == device {
			directories.push(name.to_owned());
		}
	}
	Ok(directories)
}

/// Remove the entries of a directory other than its directories on the device, and return the names of those.
fn remove_entries(directory: &OwnedFd, device: u64) -> tg::Result<Vec<CString>> {
	let mut directories = Vec::new();
	let entries = Dir::read_from(directory)
		.map_err(|error| tg::error!(!error, "failed to read the directory"))?;
	for entry in entries {
		let entry = entry.map_err(|error| tg::error!(!error, "failed to read the directory"))?;
		let name = entry.file_name();
		if name == c"." || name == c".." {
			continue;
		}
		let stat = rustix::fs::statat(directory, name, AtFlags::SYMLINK_NOFOLLOW)
			.map_err(|error| tg::error!(!error, "failed to stat a directory entry"))?;
		if FileType::from_raw_mode(stat.st_mode) != FileType::Directory {
			rustix::fs::unlinkat(directory, name, AtFlags::empty())
				.map_err(|error| tg::error!(!error, "failed to remove a file"))?;
		} else if stat.st_dev == device {
			directories.push(name.to_owned());
		} else {
			rustix::fs::unlinkat(directory, name, AtFlags::REMOVEDIR)
				.map_err(|error| tg::error!(!error, "failed to remove a mount point"))?;
		}
	}
	Ok(directories)
}

fn chown(
	directory: impl rustix::fd::AsFd,
	name: &CStr,
	stat: &Stat,
	uid: u32,
	gid: u32,
) -> tg::Result<()> {
	if stat.st_uid == uid && stat.st_gid == gid {
		return Ok(());
	}
	rustix::fs::chownat(
		directory,
		name,
		Some(rustix::fs::Uid::from_raw(uid)),
		Some(rustix::fs::Gid::from_raw(gid)),
		AtFlags::SYMLINK_NOFOLLOW,
	)
	.map_err(|error| tg::error!(!error, "failed to change the owner of a file"))?;
	Ok(())
}

fn open_directory(directory: impl rustix::fd::AsFd, name: &CStr) -> rustix::io::Result<OwnedFd> {
	rustix::fs::openat(
		directory,
		name,
		OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
		Mode::empty(),
	)
}
