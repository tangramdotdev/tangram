use futures::TryFutureExt as _;
use rand::{distributions::Alphanumeric, Rng as _};
use std::{
	ops::Deref,
	path::{Path, PathBuf},
};

pub use self::artifact::{Artifact, Directory, File, Symlink};

pub mod artifact;

pub struct Temp {
	path: Option<PathBuf>,
}

impl Temp {
	#[must_use]
	pub fn new() -> Self {
		let temp_path = if cfg!(target_os = "linux") {
			Path::new("/tmp")
		} else if cfg!(target_os = "macos") {
			Path::new("/private/tmp")
		} else {
			unreachable!()
		};
		let name = rand::thread_rng()
			.sample_iter(&Alphanumeric)
			.take(16)
			.map(char::from)
			.collect::<String>();
		let path = temp_path.join(name);
		Self { path: Some(path) }
	}

	#[must_use]
	pub fn path(&self) -> &Path {
		self.path.as_ref().unwrap()
	}

	pub async fn remove(&self) -> std::io::Result<()> {
		tokio::fs::remove_file(self.path.as_ref().unwrap())
			.or_else(|_| tokio::fs::remove_dir_all(self.path.as_ref().unwrap()))
			.await
	}
}

impl Deref for Temp {
	type Target = Path;

	fn deref(&self) -> &Self::Target {
		self.path()
	}
}

impl AsRef<Path> for Temp {
	fn as_ref(&self) -> &Path {
		self.path()
	}
}

impl Default for Temp {
	fn default() -> Self {
		Self::new()
	}
}

impl Drop for Temp {
	fn drop(&mut self) {
		if let Some(path) = self.path.take() {
			std::fs::remove_file(path.clone())
				.or_else(|_| std::fs::remove_dir_all(path))
				.ok();
		}
	}
}
