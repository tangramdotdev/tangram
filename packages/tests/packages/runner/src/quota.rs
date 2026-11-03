use {
	crate::Result,
	std::{
		io::Write as _,
		os::unix::fs::PermissionsExt as _,
		path::{Path, PathBuf},
		process::ExitCode,
	},
};

pub use tangram_tests_util::quota::*;

/// The name of the file in a test's temporary directory that records the directories that the test created on the mount.
const DIRECTORIES: &str = "quota_directories";

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

fn tangram_path(path: &Path) -> PathBuf {
	path.join("bin/tangram")
}
