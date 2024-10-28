use futures::TryFutureExt as _;
use rand::{distributions::Alphanumeric, Rng as _};
use std::path::{Path, PathBuf};

pub use self::artifact::Artifact;

pub mod artifact;

pub struct Temp {
	path: Option<PathBuf>,
}

impl Temp {
	pub fn new() -> Self {
		let temp_path = std::env::temp_dir();
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
		let path = self.path.take().unwrap();
		tokio::spawn(async move {
			tokio::fs::remove_file(path.clone())
				.or_else(|_| tokio::fs::remove_dir_all(path))
				.await
				.ok()
		});
	}
}
