use {
	crate::Result,
	std::{
		io::Write as _,
		os::unix::fs::PermissionsExt as _,
		path::{Path, PathBuf},
		process::ExitCode,
		sync::atomic::{AtomicU32, Ordering},
	},
};

pub use tangram_tests_util::quota::*;

/// The name of the file in a test's temporary directory that records the directories that the test created on the mount.
const DIRECTORIES: &str = "quota_directories";

/// The number of project IDs in each test's range.
const PROJECT_ID_COUNT: u32 = 1024;

/// The number of ranges, which keeps every project ID below the limit of the server's configuration.
const PROJECT_ID_RANGES: u32 = 1 << 20;

/// The index of the next test's range.
static PROJECT_ID_RANGE: AtomicU32 = AtomicU32::new(0);

/// Get directories on the run's file system with project quotas.
#[derive(clap::Subcommand)]
pub enum Subcommand {
	/// Create a directory on the file system, record it in the test's temporary directory, and print its path, or print nothing if the file system is not available.
	Directory,

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

/// Get the project ID range for a test as JSON, which does not overlap the range of any other running test.
pub fn project_ids() -> String {
	let index = PROJECT_ID_RANGE.fetch_add(1, Ordering::Relaxed);
	let start = project_id_start(index);
	format!(r#"{{"count":{PROJECT_ID_COUNT},"start":{start}}}"#)
}

/// Get the first project ID of a range, skipping project ID 0, which is the file system's default project.
fn project_id_start(index: u32) -> u32 {
	index % PROJECT_ID_RANGES * PROJECT_ID_COUNT + 1
}

fn tangram_path(path: &Path) -> PathBuf {
	path.join("bin/tangram")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn project_id_ranges_are_disjoint_and_within_the_config_limit() {
		assert_eq!(project_id_start(0), 1);
		assert_eq!(project_id_start(1), 1 + PROJECT_ID_COUNT);
		assert_eq!(project_id_start(PROJECT_ID_RANGES), 1);
		let last = project_id_start(PROJECT_ID_RANGES - 1) + PROJECT_ID_COUNT - 1;
		assert!(last <= i32::MAX.cast_unsigned());
	}
}
