use {
	crate::Result,
	std::{
		io::{Read as _, Write as _},
		os::unix::fs::PermissionsExt as _,
		path::{Path, PathBuf},
		process::ExitCode,
	},
};

pub use tangram_tests_util::quota::*;

/// The name of the file in a test's temporary directory that records the directories that the test created on the mount.
const DIRECTORIES: &str = "quota_directories";

/// The name of the file in the run's directory that records the server directories that received project ID ranges, one per line, with each line's index selecting its range.
const PROJECT_IDS: &str = "project_ids";

/// The number of project IDs in each range.
const PROJECT_ID_COUNT: u32 = 1024;

/// Get directories on the run's file system with project quotas.
#[derive(clap::Subcommand)]
pub enum Subcommand {
	/// Create a directory on the file system, record it in the test's temporary directory, and print its path, or print nothing if the file system is not available.
	Directory,

	/// Print the project ID range for a server directory as JSON, which is the same on every call for the directory and does not overlap the range of any other directory in the run, or print nothing if the file system is not available.
	ProjectIds { directory: PathBuf },

	/// Print the path of the Tangram executable that a server running as root runs, or nothing if the file system is not available.
	Tangram,
}

pub fn command(subcommand: &Subcommand) -> Result<ExitCode> {
	let Some(path) = std::env::var_os("TANGRAM_TEST_QUOTA").filter(|path| !path.is_empty()) else {
		return Ok(ExitCode::SUCCESS);
	};
	let path = PathBuf::from(path);
	match subcommand {
		Subcommand::Directory => {
			let directory = tangram_tests_util::Temp::with_parent(&path.join("mount"))?.into_path();
			std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755))?;
			let mut file = std::fs::File::options()
				.append(true)
				.create(true)
				.open(std::env::temp_dir().join(DIRECTORIES))?;
			writeln!(file, "{}", directory.display())?;
			println!("{}", directory.display());
		},
		Subcommand::ProjectIds { directory } => {
			let start = project_id_start(&path, directory)?;
			println!(r#"{{"count":{PROJECT_ID_COUNT},"start":{start}}}"#);
		},
		Subcommand::Tangram => {
			println!("{}", tangram_path(&path).display());
		},
	}
	Ok(ExitCode::SUCCESS)
}

/// Create the run's file system with project quotas, and link or copy Tangram next to it, so that a server running as root can execute it from a sandbox. Return `None` if passwordless sudo is not available.
pub fn create() -> Result<Option<Mount>> {
	let Some(mount) = Mount::new()? else {
		return Ok(None);
	};
	let source = tangram_tests_util::database::which("tangram").ok_or("failed to find tangram")?;
	let path = tangram_path(mount.path());
	let bin_path = path.parent().unwrap();
	std::fs::create_dir(bin_path)?;
	std::fs::set_permissions(bin_path, std::fs::Permissions::from_mode(0o755))?;
	if std::fs::hard_link(&source, &path).is_err() {
		std::fs::copy(&source, &path)?;
	}

	Ok(Some(mount))
}

/// Remove the directories that a test created on the mount, and return the errors.
pub fn cleanup(temp_path: &Path) -> Vec<String> {
	let Ok(directories) = std::fs::read_to_string(temp_path.join(DIRECTORIES)) else {
		return Vec::new();
	};
	directories
		.lines()
		.filter(|line| !line.is_empty())
		.filter_map(|directory| {
			remove(Path::new(directory))
				.err()
				.map(|error| format!("failed to remove the quota directory {directory}: {error}"))
		})
		.collect()
}

/// Get the first project ID of a server directory's range, recording the directory if it is new.
fn project_id_start(path: &Path, directory: &Path) -> Result<u32> {
	// Lock the record, so that concurrent tests receive distinct ranges.
	let mut file = std::fs::File::options()
		.append(true)
		.create(true)
		.read(true)
		.open(path.join(PROJECT_IDS))?;
	file.lock()?;

	// Find the directory's index, and record the directory if it is new.
	let mut directories = String::new();
	file.read_to_string(&mut directories)?;
	let directory = directory
		.to_str()
		.ok_or("the directory is not valid UTF-8")?;
	let index = directories.lines().position(|line| line == directory);
	let index = if let Some(index) = index {
		index
	} else {
		writeln!(file, "{directory}")?;
		directories.lines().count()
	};

	// Compute the start of the range. Project ID 0 is the file system's default project.
	let start = u32::try_from(index)? * PROJECT_ID_COUNT + 1;

	Ok(start)
}

fn tangram_path(path: &Path) -> PathBuf {
	path.join("bin/tangram")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn project_id_ranges_are_stable_and_disjoint() {
		let temp = tangram_tests_util::Temp::new().unwrap();
		let path = temp.path();
		std::fs::create_dir_all(path).unwrap();
		let first = project_id_start(path, Path::new("/first")).unwrap();
		let second = project_id_start(path, Path::new("/second")).unwrap();
		assert_eq!(first, 1);
		assert_eq!(second, first + PROJECT_ID_COUNT);
		assert_eq!(project_id_start(path, Path::new("/first")).unwrap(), first);
		assert_eq!(
			project_id_start(path, Path::new("/second")).unwrap(),
			second
		);
	}
}
