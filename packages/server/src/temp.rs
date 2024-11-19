use crate::{util::fs::remove, Server};
use futures::FutureExt as _;
use std::path::{Path, PathBuf};

pub struct Temp {
	pub path: PathBuf,
	pub preserve: bool,
}

impl Temp {
	pub fn new(server: &Server) -> Self {
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		let id = uuid::Uuid::now_v7();
		let id = ENCODING.encode(&id.into_bytes());
		let path = server.temp_path().join(id);
		let preserve = server.config.advanced.preserve_temp_directories;
		Self { path, preserve }
	}

	pub fn path(&self) -> &Path {
		&self.path
	}
}

impl AsRef<Path> for Temp {
	fn as_ref(&self) -> &Path {
		self.path()
	}
}

impl Drop for Temp {
	fn drop(&mut self) {
		if !self.preserve {
			tokio::spawn(remove(self.path.clone()).map(|_| ()));
		}
	}
}
